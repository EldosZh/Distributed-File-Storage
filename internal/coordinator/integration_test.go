package coordinator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"distributed-file-storage/internal/chunk"
	"distributed-file-storage/internal/config"
	"distributed-file-storage/internal/model"
	"distributed-file-storage/internal/storage"
	"distributed-file-storage/pkg/client"
)

type cluster struct {
	c       *Coordinator
	server  *httptest.Server
	nodes   []*storage.Node
	servers []*httptest.Server
	cfg     config.Config
	api     *client.Client
}

func newCluster(t *testing.T, count int) *cluster {
	t.Helper()
	cfg := config.Config{Token: "test-only-internal-secret", Database: filepath.Join(t.TempDir(), "meta.db"), ChunkSize: 1 << 20, Replicas: 3, Virtual: 128, Heartbeat: time.Second, Timeout: 5 * time.Second, RepairInterval: time.Second, Workers: 2, MaxRetries: 3, Grace: 0, MaxUpload: 1 << 30, MaxConcurrent: 8}
	c, e := New(cfg)
	if e != nil {
		t.Fatal(e)
	}
	v := &cluster{c: c, cfg: cfg}
	v.server = httptest.NewServer(c.Handler())
	v.api = client.New(v.server.URL)
	t.Cleanup(func() {
		v.server.Close()
		for _, s := range v.servers {
			s.Close()
		}
		v.c.Close()
	})
	for i := 0; i < count; i++ {
		v.add(t)
	}
	return v
}
func (v *cluster) add(t *testing.T) {
	t.Helper()
	cfg := v.cfg
	cfg.DataDir = t.TempDir()
	cfg.NodeID = fmt.Sprintf("node-%d", len(v.nodes))
	cfg.Capacity = 128 << 20
	n, e := storage.New(cfg)
	if e != nil {
		t.Fatal(e)
	}
	s := httptest.NewServer(n.Handler())
	n.Config.Address = s.URL
	n.Config.Coordinator = v.server.URL
	v.nodes = append(v.nodes, n)
	v.servers = append(v.servers, s)
	v.c.Config.Allowed = append(v.c.Config.Allowed, s.URL)
	v.register(t, len(v.nodes)-1)
	if e = v.c.Maintain(context.Background()); e != nil {
		t.Fatal(e)
	}
}
func (v *cluster) register(t *testing.T, i int) {
	t.Helper()
	stats, e := v.nodes[i].Stats()
	if e != nil {
		t.Fatal(e)
	}
	b, _ := json.Marshal(stats)
	r, e := http.NewRequest("POST", v.server.URL+"/internal/nodes/register", bytes.NewReader(b))
	if e != nil {
		t.Fatal(e)
	}
	r.Header.Set("Authorization", "Bearer "+v.cfg.Token)
	resp, e := http.DefaultClient.Do(r)
	if e != nil {
		t.Fatal(e)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		t.Fatal(resp.StatusCode, string(b))
	}
}
func upload(t *testing.T, v *cluster, data []byte, key, id string) model.File {
	t.Helper()
	f, e := v.api.Upload(context.Background(), "sample.bin", bytes.NewReader(data), key, id)
	if e != nil {
		t.Fatal(e)
	}
	return f
}
func download(t *testing.T, v *cluster, f model.File, data []byte) {
	t.Helper()
	var b bytes.Buffer
	if e := v.api.Download(context.Background(), f.ID, f.Version, &b); e != nil {
		t.Fatal(e)
	}
	if !bytes.Equal(b.Bytes(), data) {
		t.Fatal("download mismatch")
	}
}
func TestLifecycleFailureRepairRestartGC(t *testing.T) {
	v := newCluster(t, 3)
	data := bytes.Repeat([]byte("distributed!"), 200000)
	f := upload(t, v, data, "original", "")
	if len(f.Chunks) < 2 {
		t.Fatal("expected multiple chunks")
	}
	for _, ch := range f.Chunks {
		if len(ch.Nodes) != 3 {
			t.Fatal("strict replication")
		}
		for i, n := range v.nodes {
			p := filepath.Join(n.Config.DataDir, "chunks", ch.ID[:2], ch.ID)
			b, e := os.ReadFile(p)
			if e != nil || chunk.Hash(b) != ch.ID {
				t.Fatal(i, e)
			}
		}
	}
	download(t, v, f, data)
	// Persist immutable versions and idempotency without duplicating committed files.
	retry := upload(t, v, data, "original", "")
	if retry.ID != f.ID {
		t.Fatal("idempotency")
	}
	f2 := upload(t, v, []byte("next version"), "", f.ID)
	if f2.Version != 2 {
		t.Fatal("version", f2.Version)
	}
	if old := upload(t, v, data, "original", ""); old.Version != 1 {
		t.Fatal("idempotency returned newer version")
	}
	download(t, v, f, data)
	download(t, v, f2, []byte("next version"))
	// Corruption is tested on every copy in turn, forcing fallback regardless of round robin.
	first := f.Chunks[0]
	for i := 0; i < 3; i++ {
		n := v.nodes[i]
		p := filepath.Join(n.Config.DataDir, "chunks", first.ID[:2], first.ID)
		original, e := os.ReadFile(p)
		if e != nil {
			t.Fatal(e)
		}
		if e = os.WriteFile(p, []byte("corruption"), 0600); e != nil {
			t.Fatal(e)
		}
		_, e = v.c.fetch(context.Background(), v.c.Nodes()[i], first.ID, first.Size)
		if e == nil {
			t.Fatal("accepted corruption")
		}
		download(t, v, f, data)
		if e = v.c.Maintain(context.Background()); e != nil {
			t.Fatal(e)
		}
		b, e := os.ReadFile(p)
		if e != nil || !bytes.Equal(original, b) {
			t.Fatal("corrupt copy not repaired", e)
		}
	}
	// Stop a physical node, detect failure without removing membership, restore with a fourth node.
	v.servers[0].Close()
	v.c.mu.Lock()
	n := v.c.nodes["node-0"]
	n.Heartbeat = time.Now().Add(-3 * v.cfg.Timeout)
	v.c.nodes[n.ID] = n
	v.c.mu.Unlock()
	if e := v.c.Monitor(time.Now()); e != nil {
		t.Fatal(e)
	}
	under, e := v.c.under()
	if e != nil || under == 0 {
		t.Fatal("failure must be degraded", under, e)
	}
	download(t, v, f, data)
	v.add(t)
	if e = v.c.Maintain(context.Background()); e != nil {
		t.Fatal(e)
	}
	under, e = v.c.under()
	if e != nil || under != 0 {
		t.Fatal("repair did not restore", under, e)
	}
	// Coordinator restart keeps manifests, membership, repair history; revalidation precedes use.
	v.server.Close()
	if e = v.c.Close(); e != nil {
		t.Fatal(e)
	}
	cfg := v.c.Config
	c, e := New(cfg)
	if e != nil {
		t.Fatal(e)
	}
	v.c = c
	v.server = httptest.NewServer(c.Handler())
	v.api = client.New(v.server.URL)
	for i := 1; i < len(v.nodes); i++ {
		v.register(t, i)
	}
	if e = c.Maintain(context.Background()); e != nil {
		t.Fatal(e)
	}
	download(t, v, f, data)
	// Shared chunks survive deletion and sweep. Only tombstoning both owners permits removal.
	shared := upload(t, v, data, "", "")
	r, e := v.api.Do(context.Background(), "DELETE", "/api/v1/files/"+f.ID, nil)
	if e != nil {
		t.Fatal(e)
	}
	r.Body.Close()
	if _, e = c.GC(context.Background(), false); e != nil {
		t.Fatal(e)
	}
	download(t, v, shared, data)
	r, e = v.api.Do(context.Background(), "DELETE", "/api/v1/files/"+shared.ID, nil)
	if e != nil {
		t.Fatal(e)
	}
	r.Body.Close()
	eligible, e := c.GC(context.Background(), true)
	if e != nil || len(eligible) == 0 {
		t.Fatal("dry run expected garbage", e)
	}
	if _, e = c.GC(context.Background(), false); e != nil {
		t.Fatal(e)
	}
}
func TestUploadFailuresSecurityAndConcurrency(t *testing.T) {
	v := newCluster(t, 3)
	for _, name := range []string{"../unsafe", "bad\\name", "..", "bad\nname"} {
		if _, e := v.api.Upload(context.Background(), name, strings.NewReader("x"), "", ""); e == nil {
			t.Fatal("unsafe filename accepted")
		}
	}
	r, e := http.Post(v.server.URL+"/internal/nodes/register", "application/json", strings.NewReader(`{}`))
	if e != nil {
		t.Fatal(e)
	}
	r.Body.Close()
	if r.StatusCode != 401 {
		t.Fatal("unauthorized registration")
	}
	req, _ := http.NewRequest("PUT", v.servers[0].URL+"/internal/chunks/invalid", strings.NewReader("x"))
	req.Header.Set("Authorization", "Bearer "+v.cfg.Token)
	r, e = http.DefaultClient.Do(req)
	if e != nil {
		t.Fatal(e)
	}
	r.Body.Close()
	if r.StatusCode != 400 {
		t.Fatal("invalid chunk ID")
	}
	hash := chunk.Hash([]byte("correct"))
	req, _ = http.NewRequest("PUT", v.servers[0].URL+"/internal/chunks/"+hash, strings.NewReader("wrong"))
	req.Header.Set("Authorization", "Bearer "+v.cfg.Token)
	r, e = http.DefaultClient.Do(req)
	if e != nil {
		t.Fatal(e)
	}
	r.Body.Close()
	if r.StatusCode != 422 {
		t.Fatal("corrupt PUT")
	}
	// An interrupted stream stores an orphan at most; it never publishes a file.
	reader, writer := io.Pipe()
	go func() {
		_, _ = writer.Write(bytes.Repeat([]byte{1}, (1<<20)+10))
		_ = writer.CloseWithError(fmt.Errorf("interrupted"))
	}()
	if _, e = v.api.Upload(context.Background(), "interrupted", reader, "", ""); e == nil {
		t.Fatal("interrupted upload succeeded")
	}
	files, e := v.c.Store.Files()
	if e != nil || len(files) != 0 {
		t.Fatal("incomplete file visible", e)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, e = v.api.Upload(ctx, "cancelled", strings.NewReader("x"), "", ""); e == nil {
		t.Fatal("ignored cancellation")
	}
	data := bytes.Repeat([]byte("same-data"), 10000)
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			f, e := v.api.Upload(context.Background(), "parallel", bytes.NewReader(data), "", "")
			if e != nil {
				t.Error(e)
				return
			}
			if e = v.api.Download(context.Background(), f.ID, 0, io.Discard); e != nil {
				t.Error(e)
			}
		}()
	}
	wg.Wait()
	v.c.lifecycle.RLock()
	if _, e = v.c.GC(context.Background(), false); e == nil {
		t.Fatal("GC did not protect active upload")
	}
	v.c.lifecycle.RUnlock()
	// Real partial replica failure must reject the upload.
	v.servers[2].Close()
	if _, e = v.api.Upload(context.Background(), "failure", strings.NewReader("new-content"), "", ""); e == nil {
		t.Fatal("partial replication accepted")
	}
}
func TestEmptyCapacityRepairRetryAndDrain(t *testing.T) {
	v := newCluster(t, 3)
	f := upload(t, v, nil, "", "")
	download(t, v, f, nil)
	f = upload(t, v, []byte("repair me"), "", "")
	v.c.mu.Lock()
	n := v.c.nodes["node-1"]
	n.State = "OFFLINE"
	v.c.nodes[n.ID] = n
	n = v.c.nodes["node-2"]
	n.State = "OFFLINE"
	v.c.nodes[n.ID] = n
	v.c.mu.Unlock()
	if _, e := v.api.Upload(context.Background(), "few", strings.NewReader("x"), "", ""); e == nil {
		t.Fatal("insufficient physical nodes")
	}
	if e := v.c.Store.Queue(f.Chunks[0].ID, "missing-node"); e != nil {
		t.Fatal(e)
	}
	tasks, e := v.c.Store.Tasks()
	if e != nil {
		t.Fatal(e)
	}
	v.c.repair(context.Background(), tasks[0])
	tasks, _ = v.c.Store.Tasks()
	if tasks[0].Attempts != 1 || tasks[0].Next <= time.Now().Unix() || tasks[0].Status != "PENDING" {
		t.Fatal("backoff", tasks)
	}
	for i := 0; i < 3; i++ {
		v.c.mu.Lock()
		n := v.c.nodes[fmt.Sprintf("node-%d", i)]
		n.Free = 0
		v.c.nodes[n.ID] = n
		v.c.mu.Unlock()
	}
	if _, e := v.api.Upload(context.Background(), "full", strings.NewReader("x"), "", ""); e == nil {
		t.Fatal("capacity not enforced")
	}
}

func TestGCGraceDrainNoSourceAndHeartbeat(t *testing.T) {
	v := newCluster(t, 3)
	f := upload(t, v, []byte("retain while referenced"), "", "")
	ctx := context.Background()
	// Heartbeats update persisted capacity and never resurrect an offline replica blindly.
	stats, e := v.nodes[0].Stats()
	if e != nil {
		t.Fatal(e)
	}
	payload, _ := json.Marshal(stats)
	req, _ := http.NewRequest("POST", v.server.URL+"/internal/nodes/heartbeat", bytes.NewReader(payload))
	req.Header.Set("Authorization", "Bearer "+v.cfg.Token)
	resp, e := http.DefaultClient.Do(req)
	if e != nil {
		t.Fatal(e)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatal("heartbeat")
	}
	req, _ = http.NewRequest("POST", v.server.URL+"/internal/nodes/node-0/drain", nil)
	req.Header.Set("Authorization", "Bearer "+v.cfg.Token)
	resp, e = http.DefaultClient.Do(req)
	if e != nil {
		t.Fatal(e)
	}
	var drain struct {
		Safe bool `json:"safe_to_remove"`
	}
	e = json.NewDecoder(resp.Body).Decode(&drain)
	resp.Body.Close()
	if e != nil || drain.Safe {
		t.Fatal("unsafe drain reported safe")
	}
	download(t, v, f, []byte("retain while referenced"))
	ch := f.Chunks[0]
	for _, id := range ch.Nodes {
		if e = v.c.Store.Replica(ch.ID, id, false); e != nil {
			t.Fatal(e)
		}
	}
	if _, _, e = v.c.read(ctx, ch); e != nil {
		t.Fatal("snapshot still readable", e)
	}
	fresh, e := v.c.Store.Get(f.ID, 0)
	if e != nil {
		t.Fatal(e)
	}
	if _, _, e = v.c.read(ctx, fresh.Chunks[0]); e == nil {
		t.Fatal("accepted absent verified source")
	}
	// Make a known unreferenced orphan without involving a committed manifest.
	orphan := []byte("orphan")
	id := chunk.Hash(orphan)
	if e = v.c.put(ctx, v.c.Nodes()[1], id, orphan); e != nil {
		t.Fatal(e)
	}
	v.c.Config.Grace = time.Hour
	items, e := v.c.GC(ctx, true)
	if e != nil || len(items) != 0 {
		t.Fatal("grace period", items, e)
	}
	v.c.Config.Grace = 0
	items, e = v.c.GC(ctx, true)
	if e != nil || len(items) != 1 || items[0].Chunk != id {
		t.Fatal("mark sweep candidates", items, e)
	}
	if _, e = v.c.GC(ctx, false); e != nil {
		t.Fatal(e)
	}
}
