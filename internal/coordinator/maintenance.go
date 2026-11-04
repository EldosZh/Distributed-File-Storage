package coordinator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"slices"
	"sync"
	"time"

	"distributed-file-storage/internal/model"
	"distributed-file-storage/internal/placement"
	"distributed-file-storage/internal/storage"
	"distributed-file-storage/internal/transport"
)

func (c *Coordinator) Run(ctx context.Context) {
	ticker := time.NewTicker(c.Config.RepairInterval)
	defer ticker.Stop()
	for {
		if e := c.Maintain(ctx); e != nil && ctx.Err() == nil {
			slog.Error("maintenance", "error", e)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
func (c *Coordinator) Maintain(ctx context.Context) error {
	c.maintenance.Lock()
	defer c.maintenance.Unlock()
	c.lifecycle.RLock()
	defer c.lifecycle.RUnlock()
	if e := c.Monitor(time.Now()); e != nil {
		return e
	}
	if e := c.reconcile(ctx); e != nil {
		return e
	}
	chunks, e := c.Store.LiveChunks()
	if e != nil {
		return e
	}
	ids := []string{}
	for _, n := range c.healthy() {
		ids = append(ids, n.ID)
	}
	ring := placement.New(ids, c.Config.Virtual)
	for _, ch := range chunks {
		for _, target := range ring.Select(ch.ID, c.Config.Replicas) {
			if !slices.Contains(ch.Nodes, target) {
				if e := c.Store.Queue(ch.ID, target); e != nil {
					return e
				}
			}
		}
	}
	tasks, e := c.Store.Tasks()
	if e != nil {
		return e
	}
	jobs := make(chan model.Repair)
	var wg sync.WaitGroup
	for i := 0; i < c.Config.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for t := range jobs {
				if ctx.Err() == nil {
					c.repair(ctx, t)
				}
			}
		}()
	}
	for _, t := range tasks {
		if t.Status == "PENDING" && t.Next <= time.Now().Unix() {
			select {
			case jobs <- t:
			case <-ctx.Done():
				close(jobs)
				wg.Wait()
				return ctx.Err()
			}
		}
	}
	close(jobs)
	wg.Wait()
	// Extra copies are retained; this favors safety over reclaiming rebalance overhead.
	return nil
}
func (c *Coordinator) repair(ctx context.Context, t model.Repair) {
	start := time.Now()
	defer func() { c.Metrics.RepairNanos.Add(time.Since(start).Nanoseconds()) }()
	c.Metrics.Repairs.Add(1)
	if _, e := c.Store.DB.Exec("UPDATE repair_tasks SET status='RUNNING' WHERE id=?", t.ID); e != nil {
		return
	}
	source := ""
	e := func() error {
		referenced, e := c.Store.Referenced(t.Chunk)
		if e != nil {
			return e
		}
		if !referenced {
			return nil
		}
		var size int64
		if e = c.Store.DB.QueryRow("SELECT size FROM chunks WHERE id=?", t.Chunk).Scan(&size); e != nil {
			return e
		}
		if c.Config.RepairRate > 0 {
			timer := time.NewTimer(time.Duration(float64(size) / float64(c.Config.RepairRate) * float64(time.Second)))
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			}
		}
		nodes, e := c.Store.Replicas(t.Chunk)
		if e != nil {
			return e
		}
		target, ok := c.node(t.Target)
		if !ok || target.State != "HEALTHY" {
			return errors.New("replacement not healthy")
		}
		if target.Free < size {
			return errors.New("replacement capacity unavailable")
		}
		b, src, e := c.read(ctx, model.Chunk{ID: t.Chunk, Size: size, Nodes: nodes})
		if e != nil {
			return e
		}
		source = src
		if e = c.put(ctx, target, t.Chunk, b); e != nil {
			return e
		}
		return c.Store.Replica(t.Chunk, t.Target, true)
	}()
	status := "DONE"
	msg := ""
	attempts := t.Attempts + 1
	next := int64(0)
	if e != nil {
		c.Metrics.RepairFailures.Add(1)
		msg = e.Error()
		status = "PENDING"
		if attempts >= c.Config.MaxRetries {
			status = "FAILED"
		}
		next = time.Now().Add(time.Second * time.Duration(1<<min(attempts, 10))).Unix()
	}
	if _, err := c.Store.DB.Exec("UPDATE repair_tasks SET source_node=?,status=?,attempts=?,next_attempt_at=?,last_error=? WHERE id=?", source, status, attempts, next, msg, t.ID); err != nil {
		slog.Error("repair persistence", "error", err)
	}
}

type GCCandidate struct {
	Node  string `json:"node_id"`
	Chunk string `json:"chunk_id"`
	Size  int64  `json:"size"`
}

func (c *Coordinator) GC(ctx context.Context, dry bool) ([]GCCandidate, error) {
	// Upload and repair hold read leases for their full lifecycle. GC is an explicit,
	// nonblocking administrative operation and never races their uncommitted chunks.
	if !c.lifecycle.TryLock() {
		return nil, errors.New("active upload or maintenance; retry GC later")
	}
	defer c.lifecycle.Unlock()
	out := []GCCandidate{}
	for _, n := range c.Nodes() {
		if n.State != "HEALTHY" && n.State != "DRAINING" {
			continue
		}
		resp, e := transport.Request(ctx, c.Client, c.Config.Token, "GET", n.Address+"/internal/inventory", nil)
		if e != nil {
			return out, e
		}
		var items []storage.Inventory
		if resp.StatusCode != 200 {
			resp.Body.Close()
			return out, errors.New("inventory failed")
		}
		e = json.NewDecoder(io.LimitReader(resp.Body, 32<<20)).Decode(&items)
		resp.Body.Close()
		if e != nil {
			return out, e
		}
		for _, item := range items {
			if time.Since(item.Modified) < c.Config.Grace {
				continue
			}
			live, e := c.Store.Referenced(item.ID)
			if e != nil {
				return out, e
			}
			if live {
				continue
			}
			var active int
			if e = c.Store.DB.QueryRow("SELECT COUNT(*) FROM repair_tasks WHERE chunk_id=? AND status IN ('PENDING','RUNNING')", item.ID).Scan(&active); e != nil {
				return out, e
			}
			if active > 0 {
				continue
			}
			out = append(out, GCCandidate{n.ID, item.ID, item.Size})
			if dry {
				continue
			}
			response, e := transport.Request(ctx, c.Client, c.Config.Token, "DELETE", n.Address+"/internal/chunks/"+item.ID, nil)
			if e != nil {
				return out, e
			}
			response.Body.Close()
			if response.StatusCode != 204 {
				return out, errors.New("chunk deletion failed")
			}
			if _, e = c.Store.DB.Exec("DELETE FROM chunk_replicas WHERE chunk_id=? AND node_id=?", item.ID, n.ID); e != nil {
				return out, e
			}
		}
	}
	return out, nil
}
func (c *Coordinator) gc(w http.ResponseWriter, r *http.Request) {
	dry := r.URL.Query().Get("dry_run") != "false"
	items, e := c.GC(r.Context(), dry)
	if e != nil {
		transport.Error(w, 409, e.Error())
		return
	}
	transport.JSON(w, 200, struct {
		Dry        bool          `json:"dry_run"`
		Candidates []GCCandidate `json:"candidates"`
	}{dry, items})
}
func (c *Coordinator) metrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	m := &c.Metrics
	values := map[string]int64{"dfs_uploads_total": m.Uploads.Load(), "dfs_downloads_total": m.Downloads.Load(), "dfs_upload_failures_total": m.UploadFailures.Load(), "dfs_download_failures_total": m.DownloadFailures.Load(), "dfs_corrupt_chunks_total": m.Corrupt.Load(), "dfs_replication_tasks_total": m.Repairs.Load(), "dfs_replication_failures_total": m.RepairFailures.Load()}
	under, e := c.under()
	if e != nil {
		transport.Error(w, 500, e.Error())
		return
	}
	values["dfs_under_replicated_chunks"] = int64(under)
	nodes := c.Nodes()
	for _, n := range nodes {
		values["dfs_storage_used_bytes"] += n.Used
		values["dfs_storage_capacity_bytes"] += n.Capacity
		values["dfs_chunks_stored_total"] += n.Chunks
		if n.State == "HEALTHY" {
			values["dfs_nodes_healthy"]++
		}
		if n.State == "OFFLINE" {
			values["dfs_nodes_offline"]++
		}
	}
	for _, key := range []string{"dfs_nodes_healthy", "dfs_nodes_offline", "dfs_storage_used_bytes", "dfs_storage_capacity_bytes", "dfs_chunks_stored_total"} {
		if _, ok := values[key]; !ok {
			values[key] = 0
		}
	}
	for k, v := range values {
		fmt.Fprintf(w, "%s %d\n", k, v)
	}
	fmt.Fprintf(w, "dfs_chunk_transfer_duration_seconds_sum %f\ndfs_chunk_transfer_duration_seconds_count %d\ndfs_repair_duration_seconds_sum %f\ndfs_repair_duration_seconds_count %d\n", float64(m.TransferNanos.Load())/1e9, m.Transfers.Load(), float64(m.RepairNanos.Load())/1e9, m.Repairs.Load())
}
