package coordinator

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"distributed-file-storage/internal/chunk"
	"distributed-file-storage/internal/config"
	"distributed-file-storage/internal/metadata"
	"distributed-file-storage/internal/model"
	"distributed-file-storage/internal/placement"
	"distributed-file-storage/internal/storage"
	"distributed-file-storage/internal/transport"
)

type Counters struct{ Uploads, Downloads, UploadFailures, DownloadFailures, Corrupt, Repairs, RepairFailures, Transfers, TransferNanos, RepairNanos atomic.Int64 }
type Coordinator struct {
	Config      config.Config
	Store       *metadata.Store
	Client      *http.Client
	mu          sync.RWMutex
	nodes       map[string]model.Node
	lifecycle   sync.RWMutex // GC's exclusive barrier protects uploads and repairs, never metadata-only reads.
	maintenance sync.Mutex
	slots       chan struct{}
	round       atomic.Uint64
	Metrics     Counters
}

func New(c config.Config) (*Coordinator, error) {
	s, e := metadata.Open(c.Database)
	if e != nil {
		return nil, e
	}
	nodes, e := s.Nodes()
	if e != nil {
		s.Close()
		return nil, e
	}
	v := &Coordinator{Config: c, Store: s, Client: transport.Client(), nodes: map[string]model.Node{}, slots: make(chan struct{}, c.MaxConcurrent)}
	for _, n := range nodes {
		n.State = "JOINING"
		v.nodes[n.ID] = n
	}
	return v, nil
}
func (c *Coordinator) Close() error { c.Client.CloseIdleConnections(); return c.Store.Close() }
func (c *Coordinator) Nodes() []model.Node {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := []model.Node{}
	for _, n := range c.nodes {
		out = append(out, n)
	}
	slices.SortFunc(out, func(a, b model.Node) int { return strings.Compare(a.ID, b.ID) })
	return out
}
func (c *Coordinator) healthy() []model.Node {
	out := []model.Node{}
	for _, n := range c.Nodes() {
		if n.State == "HEALTHY" {
			out = append(out, n)
		}
	}
	return out
}
func (c *Coordinator) node(id string) (model.Node, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	n, ok := c.nodes[id]
	return n, ok
}
func ID() string {
	b := make([]byte, 16)
	if _, e := rand.Read(b); e != nil {
		panic(e)
	}
	return hex.EncodeToString(b)
}
func (c *Coordinator) Handler() http.Handler {
	m := http.NewServeMux()
	m.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		transport.JSON(w, 200, map[string]string{"status": "ok"})
	})
	m.HandleFunc("GET /ready", func(w http.ResponseWriter, r *http.Request) {
		if len(c.healthy()) < c.Config.Replicas {
			transport.Error(w, 503, "insufficient healthy nodes for strict writes")
			return
		}
		transport.JSON(w, 200, map[string]string{"status": "ready"})
	})
	m.HandleFunc("POST /api/v1/files", c.upload)
	m.HandleFunc("GET /api/v1/files", func(w http.ResponseWriter, r *http.Request) {
		f, e := c.Store.Files()
		if e != nil {
			transport.Error(w, 500, e.Error())
			return
		}
		transport.JSON(w, 200, f)
	})
	m.HandleFunc("GET /api/v1/files/{id}", c.info)
	m.HandleFunc("GET /api/v1/files/{id}/versions/{version}", c.info)
	m.HandleFunc("GET /api/v1/files/{id}/versions", func(w http.ResponseWriter, r *http.Request) {
		v, e := c.Store.Versions(r.PathValue("id"))
		if e != nil {
			transport.Error(w, 500, e.Error())
			return
		}
		transport.JSON(w, 200, v)
	})
	m.HandleFunc("DELETE /api/v1/files/{id}", func(w http.ResponseWriter, r *http.Request) {
		if e := c.Store.Delete(r.PathValue("id")); e != nil {
			transport.Error(w, 404, "file not found")
			return
		}
		w.WriteHeader(204)
	})
	m.HandleFunc("GET /api/v1/files/{id}/download", c.download)
	m.HandleFunc("GET /api/v1/nodes", func(w http.ResponseWriter, r *http.Request) { transport.JSON(w, 200, c.Nodes()) })
	m.HandleFunc("GET /api/v1/cluster/status", c.status)
	m.HandleFunc("GET /api/v1/cluster/health", c.status)
	m.HandleFunc("GET /api/v1/replication/status", c.replicationStatus)
	m.HandleFunc("GET /metrics", c.metrics)
	internal := http.NewServeMux()
	internal.HandleFunc("POST /internal/nodes/register", c.register)
	internal.HandleFunc("POST /internal/nodes/heartbeat", c.heartbeat)
	internal.HandleFunc("POST /internal/nodes/{id}/drain", c.drain)
	internal.HandleFunc("POST /internal/gc", c.gc)
	m.Handle("/internal/", transport.Auth(c.Config.Token, internal))
	return m
}
func (c *Coordinator) info(w http.ResponseWriter, r *http.Request) {
	version := 0
	if s := r.PathValue("version"); s != "" {
		v, e := strconv.Atoi(s)
		if e != nil || v < 1 {
			transport.Error(w, 400, "invalid version")
			return
		}
		version = v
	}
	f, e := c.Store.Get(r.PathValue("id"), version)
	if e != nil {
		transport.Error(w, 404, "file not found")
		return
	}
	transport.JSON(w, 200, f)
}
func safeName(s string) bool {
	return s != "" && len(s) <= 255 && !strings.ContainsAny(s, "/\\\r\n\x00") && s != "." && s != ".."
}
func (c *Coordinator) upload(w http.ResponseWriter, r *http.Request) {
	select {
	case c.slots <- struct{}{}:
		defer func() { <-c.slots }()
	default:
		transport.Error(w, 429, "operation limit reached")
		return
	}
	c.lifecycle.RLock()
	defer c.lifecycle.RUnlock()
	name := r.URL.Query().Get("name")
	if !safeName(name) {
		transport.Error(w, 400, "safe name query parameter required")
		return
	}
	key := r.Header.Get("Idempotency-Key")
	if len(key) > 200 {
		transport.Error(w, 400, "idempotency key too long")
		return
	}
	id := r.URL.Query().Get("file_id")
	if id == "" {
		id = ID()
	} else {
		f, e := c.Store.Get(id, 0)
		if e != nil || f.Name != name {
			transport.Error(w, 409, "version target must exist and have the same name")
			return
		}
	}
	session := ID()
	if e := c.Store.Begin(session, key, id); e != nil {
		if key != "" {
			state, prior, e := c.Store.Session(key)
			if e == nil && state == "COMMITTED" {
				version, verErr := c.Store.SessionVersion(key)
				if verErr != nil {
					transport.Error(w, 500, verErr.Error())
					return
				}
				f, e := c.Store.Get(prior, version)
				if e == nil {
					transport.JSON(w, 200, f)
					return
				}
			}
			transport.Error(w, 409, "idempotency key already used; pending/failed keys require inspection and a new key")
			return
		}
		transport.Error(w, 500, e.Error())
		return
	}
	committed := false
	defer func() {
		if !committed {
			c.Metrics.UploadFailures.Add(1)
			_ = c.Store.State(session, "FAILED", "upload did not commit")
		}
	}()
	nodes := c.healthy()
	if len(nodes) < c.Config.Replicas {
		transport.Error(w, 503, "insufficient healthy physical nodes")
		return
	}
	ids := []string{}
	for _, n := range nodes {
		ids = append(ids, n.ID)
	}
	ring := placement.New(ids, c.Config.Virtual)
	if e := c.Store.State(session, "UPLOADING", ""); e != nil {
		transport.Error(w, 500, e.Error())
		return
	}
	f := model.File{ID: id, Name: name, ContentType: r.Header.Get("Content-Type"), Created: time.Now().UTC(), Updated: time.Now().UTC(), ChunkSize: c.Config.ChunkSize, Chunks: []model.Chunk{}}
	var offset int64
	r.Body = http.MaxBytesReader(w, r.Body, c.Config.MaxUpload)
	checksum, size, e := chunk.Walk(r.Body, c.Config.ChunkSize, func(seq int, b []byte) error {
		if e := r.Context().Err(); e != nil {
			return e
		}
		hash := chunk.Hash(b)
		if e := c.Store.Track(hash, int64(len(b))); e != nil {
			return e
		}
		selected := ring.Select(hash, len(ids))
		targets := []string{}
		for _, target := range selected {
			n, ok := c.node(target)
			if !ok || n.State != "HEALTHY" {
				continue
			}
			if n.Free < int64(len(b)) {
				continue
			}
			if e := c.put(r.Context(), n, hash, b); e != nil {
				return fmt.Errorf("replica %s: %w", target, e)
			}
			if e := c.Store.Replica(hash, target, true); e != nil {
				return e
			}
			targets = append(targets, target)
			if len(targets) == c.Config.Replicas {
				break
			}
		}
		if len(targets) != c.Config.Replicas {
			return errors.New("insufficient replica capacity")
		}
		f.Chunks = append(f.Chunks, model.Chunk{ID: hash, Checksum: hash, Sequence: seq, Offset: offset, Size: int64(len(b)), Nodes: targets})
		offset += int64(len(b))
		return nil
	})
	if e != nil {
		transport.Error(w, 503, e.Error())
		return
	}
	f.Checksum = checksum
	f.Size = size
	if expected := r.Header.Get("X-Content-SHA256"); expected != "" && expected != checksum {
		transport.Error(w, 422, "complete file checksum mismatch")
		return
	}
	if e = c.Store.State(session, "VERIFYING", ""); e == nil {
		e = c.Store.Commit(&f, session)
	}
	if e != nil {
		transport.Error(w, 500, e.Error())
		return
	}
	committed = true
	c.Metrics.Uploads.Add(1)
	transport.JSON(w, 201, f)
}
func (c *Coordinator) put(ctx context.Context, n model.Node, id string, b []byte) error {
	start := time.Now()
	defer func() { c.Metrics.Transfers.Add(1); c.Metrics.TransferNanos.Add(time.Since(start).Nanoseconds()) }()
	response, e := transport.Request(ctx, c.Client, c.Config.Token, "PUT", n.Address+"/internal/chunks/"+id, bytes.NewReader(b))
	if e != nil {
		return e
	}
	defer response.Body.Close()
	if response.StatusCode != 201 {
		detail, _ := io.ReadAll(io.LimitReader(response.Body, 1024))
		return fmt.Errorf("chunk PUT status %d: %s", response.StatusCode, detail)
	}
	return nil
}
func (c *Coordinator) fetch(ctx context.Context, n model.Node, id string, size int64) ([]byte, error) {
	start := time.Now()
	defer func() { c.Metrics.Transfers.Add(1); c.Metrics.TransferNanos.Add(time.Since(start).Nanoseconds()) }()
	resp, e := transport.Request(ctx, c.Client, c.Config.Token, "GET", n.Address+"/internal/chunks/"+id, nil)
	if e != nil {
		return nil, e
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("chunk GET status %d", resp.StatusCode)
	}
	b, e := io.ReadAll(io.LimitReader(resp.Body, size+1))
	if e != nil {
		return nil, e
	}
	if int64(len(b)) != size || chunk.Hash(b) != id {
		c.Metrics.Corrupt.Add(1)
		_ = c.Store.Replica(id, n.ID, false)
		slog.Warn("corrupted replica", "chunk_id", id, "node_id", n.ID)
		return nil, errors.New("corrupted replica")
	}
	return b, nil
}
func (c *Coordinator) read(ctx context.Context, ch model.Chunk) ([]byte, string, error) {
	ids := slices.Clone(ch.Nodes)
	if len(ids) == 0 {
		return nil, "", errors.New("no verified replicas")
	}
	start := int(c.round.Add(1) % uint64(len(ids)))
	for i := range ids {
		n, ok := c.node(ids[(start+i)%len(ids)])
		if !ok || (n.State != "HEALTHY" && n.State != "DRAINING" && n.State != "SUSPECT") {
			continue
		}
		b, e := c.fetch(ctx, n, ch.ID, ch.Size)
		if e == nil {
			return b, n.ID, nil
		}
		if ctx.Err() != nil {
			return nil, "", ctx.Err()
		}
	}
	return nil, "", errors.New("no readable replica")
}
func (c *Coordinator) download(w http.ResponseWriter, r *http.Request) {
	select {
	case c.slots <- struct{}{}:
		defer func() { <-c.slots }()
	default:
		transport.Error(w, 429, "operation limit reached")
		return
	}
	version := 0
	if s := r.URL.Query().Get("version"); s != "" {
		v, e := strconv.Atoi(s)
		if e != nil || v < 1 {
			transport.Error(w, 400, "invalid version")
			return
		}
		version = v
	}
	f, e := c.Store.Get(r.PathValue("id"), version)
	if e != nil {
		transport.Error(w, 404, "file not found")
		return
	}
	// Content-Length plus checksum headers and a final integrity trailer allow clients
	// to detect a late streaming failure after earlier verified chunks were sent.
	w.Header().Set("Content-Type", f.ContentType)
	w.Header().Set("Content-Disposition", mime.FormatMediaType("attachment", map[string]string{"filename": f.Name}))
	w.Header().Set("X-Content-SHA256", f.Checksum)
	w.Header().Set("Trailer", "X-DFS-Integrity")
	h := sha256.New()
	var sent int64
	for _, ch := range f.Chunks {
		b, _, err := c.read(r.Context(), ch)
		if err != nil {
			c.Metrics.DownloadFailures.Add(1)
			if sent == 0 {
				transport.Error(w, 503, err.Error())
			} else {
				w.Header().Set("X-DFS-Integrity", "failed")
			}
			return
		}
		h.Write(b)
		n, err := w.Write(b)
		sent += int64(n)
		if err != nil {
			c.Metrics.DownloadFailures.Add(1)
			return
		}
	}
	if sent != f.Size || hex.EncodeToString(h.Sum(nil)) != f.Checksum {
		c.Metrics.DownloadFailures.Add(1)
		w.Header().Set("X-DFS-Integrity", "failed")
		return
	}
	w.Header().Set("X-DFS-Integrity", "verified")
	c.Metrics.Downloads.Add(1)
}
func (c *Coordinator) register(w http.ResponseWriter, r *http.Request) {
	var n model.Node
	if e := json.NewDecoder(io.LimitReader(r.Body, 16<<10)).Decode(&n); e != nil {
		transport.Error(w, 400, "invalid registration")
		return
	}
	u, e := url.Parse(n.Address)
	if e != nil || u.Scheme != "http" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Path != "" || !slices.Contains(c.Config.Allowed, n.Address) || !safeName(n.ID) || len(n.ID) > 128 || n.Capacity <= 0 || n.Used < 0 || n.Free < 0 {
		transport.Error(w, 403, "node address must exactly match configured allowlist")
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, other := range c.nodes {
		if other.ID != n.ID && other.Address == n.Address {
			transport.Error(w, 409, "address already belongs to another physical node")
			return
		}
	}
	prior, exists := c.nodes[n.ID]
	if exists && prior.Address != n.Address {
		transport.Error(w, 409, "node identity address conflict")
		return
	}
	n.State = "JOINING"
	if prior.State == "DRAINING" {
		n.State = "DRAINING"
	}
	n.Heartbeat = time.Now().UTC()
	n.Free = max(0, min(n.Free, n.Capacity-n.Used))
	if e = c.Store.SaveNode(n); e != nil {
		transport.Error(w, 500, e.Error())
		return
	}
	c.nodes[n.ID] = n
	transport.JSON(w, 200, n)
}
func (c *Coordinator) heartbeat(w http.ResponseWriter, r *http.Request) {
	var stats model.Node
	if e := json.NewDecoder(io.LimitReader(r.Body, 16<<10)).Decode(&stats); e != nil {
		transport.Error(w, 400, "invalid heartbeat")
		return
	}
	if stats.Capacity <= 0 || stats.Used < 0 || stats.Free < 0 {
		transport.Error(w, 400, "invalid capacity statistics")
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	n, ok := c.nodes[stats.ID]
	if !ok || stats.Address != n.Address {
		transport.Error(w, 409, "register first")
		return
	}
	n.Capacity = stats.Capacity
	n.Used = stats.Used
	n.Free = max(0, min(stats.Free, n.Capacity-n.Used))
	n.Active = stats.Active
	n.Chunks = stats.Chunks
	n.Heartbeat = time.Now().UTC()
	if n.State == "OFFLINE" || n.State == "SUSPECT" {
		n.State = "JOINING"
	}
	if e := c.Store.SaveNode(n); e != nil {
		transport.Error(w, 500, e.Error())
		return
	}
	c.nodes[n.ID] = n
	transport.JSON(w, 200, n)
}
func (c *Coordinator) Monitor(now time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for id, n := range c.nodes {
		age := now.Sub(n.Heartbeat)
		state := n.State
		if state != "DRAINING" {
			if age > 2*c.Config.Timeout {
				n.State = "OFFLINE"
			} else if age > c.Config.Timeout {
				n.State = "SUSPECT"
			}
		}
		if n.State != state {
			if e := c.Store.SaveNode(n); e != nil {
				return e
			}
			c.nodes[id] = n
		}
	}
	return nil
}
func (c *Coordinator) reconcile(ctx context.Context) error {
	for _, n := range c.Nodes() {
		if n.State != "JOINING" {
			continue
		}
		resp, e := transport.Request(ctx, c.Client, c.Config.Token, "GET", n.Address+"/internal/inventory", nil)
		if e != nil {
			continue
		}
		var inv []storage.Inventory
		if resp.StatusCode == 200 {
			e = json.NewDecoder(io.LimitReader(resp.Body, 32<<20)).Decode(&inv)
		} else {
			e = errors.New("inventory failed")
		}
		resp.Body.Close()
		if e != nil {
			continue
		}
		rows, e := c.Store.DB.Query("SELECT c.id,c.size FROM chunks c JOIN chunk_replicas r ON r.chunk_id=c.id WHERE r.node_id=?", n.ID)
		if e != nil {
			return e
		}
		known := []model.Chunk{}
		for rows.Next() {
			var ch model.Chunk
			if e = rows.Scan(&ch.ID, &ch.Size); e != nil {
				break
			}
			known = append(known, ch)
		}
		rowErr := rows.Err()
		rows.Close()
		if e != nil {
			return e
		}
		if rowErr != nil {
			return rowErr
		}
		for _, ch := range known {
			_, e := c.fetch(ctx, n, ch.ID, ch.Size)
			if err := c.Store.Replica(ch.ID, n.ID, e == nil); err != nil {
				return err
			}
		}
		c.mu.Lock()
		current := c.nodes[n.ID]
		if current.State == "JOINING" {
			current.State = "HEALTHY"
			if e = c.Store.SaveNode(current); e == nil {
				c.nodes[n.ID] = current
			}
		}
		c.mu.Unlock()
		if e != nil {
			return e
		}
	}
	return nil
}
func (c *Coordinator) status(w http.ResponseWriter, r *http.Request) {
	files, e := c.Store.Files()
	if e != nil {
		transport.Error(w, 500, e.Error())
		return
	}
	nodes := c.Nodes()
	var capacity, used, logical, free, healthyFree int64
	healthy, offline := 0, 0
	for _, n := range nodes {
		capacity += n.Capacity
		used += n.Used
		free += n.Free
		if n.State == "HEALTHY" {
			healthy++
			healthyFree += n.Free
		}
		if n.State == "OFFLINE" {
			offline++
		}
	}
	for _, f := range files {
		logical += f.Size
	}
	ratio := 0.0
	if logical > 0 {
		ratio = float64(used) / float64(logical)
	}
	transport.JSON(w, 200, struct {
		Nodes       int     `json:"nodes"`
		Healthy     int     `json:"healthy_nodes"`
		Offline     int     `json:"offline_nodes"`
		Capacity    int64   `json:"capacity_bytes"`
		Used        int64   `json:"physical_used_bytes"`
		Free        int64   `json:"free_bytes"`
		Logical     int64   `json:"logical_bytes"`
		Files       int     `json:"files"`
		Replicas    int     `json:"replication_factor"`
		ChunkSize   int     `json:"chunk_size"`
		Coordinator string  `json:"coordinator_mode"`
		HealthyFree int64   `json:"healthy_free_bytes"`
		Ratio       float64 `json:"physical_to_logical_ratio"`
	}{len(nodes), healthy, offline, capacity, used, free, logical, len(files), c.Config.Replicas, c.Config.ChunkSize, "single authoritative coordinator", healthyFree, ratio})
}
func (c *Coordinator) under() (int, error) {
	chunks, e := c.Store.LiveChunks()
	if e != nil {
		return 0, e
	}
	under := 0
	for _, ch := range chunks {
		count := 0
		for _, id := range ch.Nodes {
			n, ok := c.node(id)
			if ok && n.State == "HEALTHY" {
				count++
			}
		}
		if count < c.Config.Replicas {
			under++
		}
	}
	return under, nil
}
func (c *Coordinator) replicationStatus(w http.ResponseWriter, r *http.Request) {
	under, e := c.under()
	if e != nil {
		transport.Error(w, 500, e.Error())
		return
	}
	tasks, e := c.Store.Tasks()
	if e != nil {
		transport.Error(w, 500, e.Error())
		return
	}
	transport.JSON(w, 200, struct {
		Factor int            `json:"replication_factor"`
		Under  int            `json:"under_replicated_chunks"`
		Tasks  []model.Repair `json:"repair_tasks"`
	}{c.Config.Replicas, under, tasks})
}
func (c *Coordinator) drain(w http.ResponseWriter, r *http.Request) {
	c.mu.Lock()
	n, ok := c.nodes[r.PathValue("id")]
	if !ok {
		c.mu.Unlock()
		transport.Error(w, 404, "node not found")
		return
	}
	n.State = "DRAINING"
	e := c.Store.SaveNode(n)
	if e == nil {
		c.nodes[n.ID] = n
	}
	c.mu.Unlock()
	if e != nil {
		transport.Error(w, 500, e.Error())
		return
	}
	chunks, e := c.Store.LiveChunks()
	if e != nil {
		transport.Error(w, 500, e.Error())
		return
	}
	remaining := 0
	for _, ch := range chunks {
		if slices.Contains(ch.Nodes, n.ID) {
			count := 0
			for _, id := range ch.Nodes {
				other, ok := c.node(id)
				if ok && other.State == "HEALTHY" && id != n.ID {
					count++
				}
			}
			if count < c.Config.Replicas {
				remaining++
			}
		}
	}
	transport.JSON(w, 202, struct {
		State     string `json:"status"`
		Remaining int    `json:"chunks_awaiting_replacement"`
		Safe      bool   `json:"safe_to_remove"`
	}{"DRAINING", remaining, remaining == 0})
}
