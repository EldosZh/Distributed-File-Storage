package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"distributed-file-storage/internal/chunk"
	"distributed-file-storage/internal/config"
	"distributed-file-storage/internal/model"
	"distributed-file-storage/internal/transport"
)

type Node struct {
	Config config.Config
	mu     sync.Mutex
	active atomic.Int64
	slots  chan struct{}
}
type Inventory struct {
	ID       string    `json:"chunk_id"`
	Size     int64     `json:"size"`
	Modified time.Time `json:"modified_at"`
}

func New(c config.Config) (*Node, error) {
	root, e := filepath.Abs(c.DataDir)
	if e != nil {
		return nil, e
	}
	c.DataDir = root
	if e = os.MkdirAll(filepath.Join(root, "chunks"), 0700); e != nil {
		return nil, e
	}
	// Refuse symlink/reparse-point roots and components before handling requests.
	for p := filepath.Join(root, "chunks"); ; p = filepath.Dir(p) {
		i, e := os.Lstat(p)
		if e != nil {
			return nil, e
		}
		if i.Mode()&os.ModeSymlink != 0 {
			return nil, errors.New("symlink storage path")
		}
		if filepath.Dir(p) == p {
			break
		}
	}
	identity := filepath.Join(root, "node-id")
	if info, err := os.Lstat(identity); err == nil && !info.Mode().IsRegular() {
		return nil, errors.New("unsafe node identity file")
	}
	b, e := os.ReadFile(identity)
	if e == nil && string(b) != c.NodeID {
		return nil, errors.New("data directory belongs to another node")
	}
	if e != nil && !os.IsNotExist(e) {
		return nil, e
	}
	if e = os.WriteFile(identity, []byte(c.NodeID), 0600); e != nil {
		return nil, e
	}
	// Interrupted temporary writes are never visible as chunks.
	e = filepath.WalkDir(filepath.Join(root, "chunks"), func(p string, d os.DirEntry, e error) error {
		if e != nil {
			return e
		}
		if d.Type()&os.ModeSymlink != 0 {
			return errors.New("symlink in chunk tree")
		}
		if !d.IsDir() && strings.HasPrefix(d.Name(), ".tmp-") {
			return os.Remove(p)
		}
		return nil
	})
	if e != nil {
		return nil, e
	}
	return &Node{Config: c, slots: make(chan struct{}, c.MaxConcurrent)}, nil
}
func (n *Node) path(id string) (string, error) {
	if !chunk.Valid(id) {
		return "", errors.New("invalid SHA-256 chunk ID")
	}
	dir := filepath.Join(n.Config.DataDir, "chunks", id[:2])
	if e := os.MkdirAll(dir, 0700); e != nil {
		return "", e
	}
	i, e := os.Lstat(dir)
	if e != nil || i.Mode()&os.ModeSymlink != 0 {
		return "", errors.New("unsafe shard directory")
	}
	p := filepath.Join(dir, id)
	if i, e = os.Lstat(p); e == nil && (!i.Mode().IsRegular() || i.Mode()&os.ModeSymlink != 0) {
		return "", errors.New("unsafe chunk path")
	}
	return p, nil
}
func (n *Node) inventory() ([]Inventory, error) {
	out := []Inventory{}
	e := filepath.WalkDir(filepath.Join(n.Config.DataDir, "chunks"), func(p string, d os.DirEntry, e error) error {
		if e != nil {
			return e
		}
		if d.Type()&os.ModeSymlink != 0 {
			return errors.New("symlink in chunk tree")
		}
		if !d.IsDir() && chunk.Valid(d.Name()) {
			i, e := d.Info()
			if e != nil {
				return e
			}
			out = append(out, Inventory{d.Name(), i.Size(), i.ModTime()})
		}
		return nil
	})
	return out, e
}
func (n *Node) Stats() (model.Node, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	items, e := n.inventory()
	var used int64
	for _, i := range items {
		used += i.Size
	}
	free, err := diskFree(n.Config.DataDir)
	if e == nil {
		e = err
	}
	return model.Node{ID: n.Config.NodeID, Address: n.Config.Address, Capacity: n.Config.Capacity, Used: used, Free: max(0, min(free, n.Config.Capacity-used)), Chunks: int64(len(items)), Active: n.active.Load()}, e
}
func (n *Node) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		transport.JSON(w, 200, map[string]string{"status": "ok"})
	})
	internal := http.NewServeMux()
	internal.HandleFunc("GET /internal/inventory", func(w http.ResponseWriter, r *http.Request) {
		n.mu.Lock()
		defer n.mu.Unlock()
		v, e := n.inventory()
		if e != nil {
			transport.Error(w, 500, e.Error())
			return
		}
		transport.JSON(w, 200, v)
	})
	internal.HandleFunc("/internal/chunks/{id}", n.handleChunk)
	mux.Handle("/internal/", transport.Auth(n.Config.Token, internal))
	return mux
}
func (n *Node) handleChunk(w http.ResponseWriter, r *http.Request) {
	select {
	case n.slots <- struct{}{}:
		defer func() { <-n.slots }()
	case <-r.Context().Done():
		return
	}
	n.active.Add(1)
	defer n.active.Add(-1)
	id := r.PathValue("id")
	if !chunk.Valid(id) {
		transport.Error(w, 400, "invalid chunk ID")
		return
	}
	n.mu.Lock()
	p, e := n.path(id)
	n.mu.Unlock()
	if e != nil {
		transport.Error(w, 400, e.Error())
		return
	}
	switch r.Method {
	case "PUT":
		// Reserve the maximum request footprint under the capacity mutex; disk I/O is
		// deliberately serialized per node so concurrent writes cannot oversubscribe quota.
		n.mu.Lock()
		defer n.mu.Unlock()
		items, e := n.inventory()
		if e != nil {
			transport.Error(w, 500, e.Error())
			return
		}
		var used int64
		for _, i := range items {
			used += i.Size
		}
		limit := int64(16 << 20)
		available := n.Config.Capacity - used
		if st, e := os.Stat(p); e == nil {
			available += st.Size()
		}
		free, err := diskFree(n.Config.DataDir)
		if err != nil {
			transport.Error(w, 507, err.Error())
			return
		}
		available = min(available, free)
		limit = min(limit, available)
		if limit < 0 {
			transport.Error(w, 507, "node capacity exhausted")
			return
		}
		f, e := os.CreateTemp(filepath.Dir(p), ".tmp-")
		if e != nil {
			transport.Error(w, 507, e.Error())
			return
		}
		tmp := f.Name()
		defer os.Remove(tmp)
		defer f.Close()
		h := sha256.New()
		size, e := io.Copy(io.MultiWriter(f, h), io.LimitReader(r.Body, limit+1))
		if e != nil || size > limit {
			transport.Error(w, 507, "chunk exceeds available capacity or maximum size")
			return
		}
		if hex.EncodeToString(h.Sum(nil)) != id {
			transport.Error(w, 422, "chunk checksum mismatch")
			return
		}
		// A verified identical object is already durable. Avoid replacing an open
		// file on Windows and avoid write amplification for shared chunks.
		if existing, err := os.Open(p); err == nil {
			current := sha256.New()
			existingSize, readErr := io.Copy(current, existing)
			_ = existing.Close()
			if readErr == nil && existingSize == size && hex.EncodeToString(current.Sum(nil)) == id {
				w.WriteHeader(201)
				return
			}
		}
		if e = f.Sync(); e == nil {
			e = f.Close()
		}
		if e == nil {
			e = os.Rename(tmp, p)
		}
		if e == nil {
			e = syncDirectory(filepath.Dir(p))
		}
		if e != nil {
			transport.Error(w, 500, e.Error())
			return
		}
		w.WriteHeader(201)
	case "GET", "HEAD":
		f, e := os.Open(p)
		if e != nil {
			transport.Error(w, 404, "chunk missing")
			return
		}
		defer f.Close()
		i, e := f.Stat()
		if e != nil {
			transport.Error(w, 500, e.Error())
			return
		}
		w.Header().Set("Content-Length", fmt.Sprint(i.Size()))
		w.Header().Set("Content-Type", "application/octet-stream")
		if r.Method == "GET" {
			_, _ = io.Copy(w, f)
		}
	case "DELETE":
		n.mu.Lock()
		e = os.Remove(p)
		n.mu.Unlock()
		if e != nil && !os.IsNotExist(e) {
			transport.Error(w, 500, e.Error())
			return
		}
		w.WriteHeader(204)
	default:
		w.Header().Set("Allow", "PUT, GET, HEAD, DELETE")
		transport.Error(w, 405, "method not allowed")
	}
}
func (n *Node) Run(ctx context.Context) {
	c := transport.Client()
	defer c.CloseIdleConnections()
	ticker := time.NewTicker(n.Config.Heartbeat)
	defer ticker.Stop()
	registered := false
	for {
		stats, e := n.Stats()
		if e == nil {
			payload, _ := json.Marshal(stats)
			endpoint := "/internal/nodes/heartbeat"
			if !registered {
				endpoint = "/internal/nodes/register"
			}
			response, e := transport.Request(ctx, c, n.Config.Token, "POST", n.Config.Coordinator+endpoint, bytes.NewReader(payload))
			if e == nil {
				registered = response.StatusCode == 200
				response.Body.Close()
			} else {
				registered = false
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
