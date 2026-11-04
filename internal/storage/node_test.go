package storage

import (
	"bytes"
	"distributed-file-storage/internal/chunk"
	"distributed-file-storage/internal/config"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestAtomicQuotaAndRestart(t *testing.T) {
	cfg := config.Config{DataDir: t.TempDir(), NodeID: "stable", Token: "test-internal-secret", Capacity: 8, MaxConcurrent: 2}
	n, e := New(cfg)
	if e != nil {
		t.Fatal(e)
	}
	s := httptest.NewServer(n.Handler())
	defer s.Close()
	send := func(method, id string, b []byte) int {
		t.Helper()
		r, _ := http.NewRequest(method, s.URL+"/internal/chunks/"+id, bytes.NewReader(b))
		r.Header.Set("Authorization", "Bearer "+cfg.Token)
		resp, e := http.DefaultClient.Do(r)
		if e != nil {
			t.Fatal(e)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		return resp.StatusCode
	}
	b := []byte("bytes")
	id := chunk.Hash(b)
	if send("PUT", id, b) != 201 || send("PUT", id, b) != 201 || send("HEAD", id, nil) != 200 {
		t.Fatal("idempotent chunk operations")
	}
	large := []byte("more than capacity")
	if send("PUT", chunk.Hash(large), large) != 507 {
		t.Fatal("quota")
	}
	if send("GET", chunk.Hash(large), nil) != 404 {
		t.Fatal("partial file exposed")
	}
	inv, e := n.inventory()
	if e != nil || len(inv) != 1 {
		t.Fatal("quota changed valid chunks", e)
	}
	tmp := filepath.Join(cfg.DataDir, "chunks", id[:2], ".tmp-interrupted")
	if e = os.WriteFile(tmp, []byte("partial"), 0600); e != nil {
		t.Fatal(e)
	}
	n, e = New(cfg)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = os.Stat(tmp); !os.IsNotExist(e) {
		t.Fatal("temporary file not cleaned")
	}
	cfg.NodeID = "another"
	if _, e = New(cfg); e == nil {
		t.Fatal("unstable identity allowed")
	}
	if send("DELETE", id, nil) != 204 || send("GET", id, nil) != 404 {
		t.Fatal("delete")
	}
}
func TestSymlinkRootRejected(t *testing.T) {
	outside := t.TempDir()
	root := filepath.Join(t.TempDir(), "link")
	if e := os.Symlink(outside, root); e != nil {
		t.Skip("symlink creation unavailable:", e)
	}
	if _, e := New(config.Config{DataDir: root, NodeID: "x", MaxConcurrent: 1}); e == nil {
		t.Fatal("symlink root accepted")
	}
}
