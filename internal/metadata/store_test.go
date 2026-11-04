package metadata

import (
	"distributed-file-storage/internal/model"
	"path/filepath"
	"testing"
	"time"
)

func TestCommitRollbackAndPersistence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "metadata.db")
	s, e := Open(path)
	if e != nil {
		t.Fatal(e)
	}
	if e = s.Begin("session", "key", "file"); e != nil {
		t.Fatal(e)
	}
	if e = s.Track("chunk", 5); e != nil {
		t.Fatal(e)
	}
	if _, e = s.DB.Exec(`CREATE TRIGGER reject_manifest BEFORE INSERT ON file_versions BEGIN SELECT RAISE(ABORT,'injected metadata failure'); END`); e != nil {
		t.Fatal(e)
	}
	f := model.File{ID: "file", Name: "name", Created: time.Now(), Chunks: []model.Chunk{{ID: "chunk", Size: 5}}}
	if e = s.Commit(&f, "session"); e == nil {
		t.Fatal("injected failure ignored")
	}
	var count int
	if e = s.DB.QueryRow("SELECT COUNT(*) FROM files").Scan(&count); e != nil || count != 0 {
		t.Fatal("partial metadata committed", e)
	}
	if _, e = s.DB.Exec("DROP TRIGGER reject_manifest"); e != nil {
		t.Fatal(e)
	}
	if e = s.Commit(&f, "session"); e != nil {
		t.Fatal(e)
	}
	if e = s.SaveNode(model.Node{ID: "node", State: "HEALTHY"}); e != nil {
		t.Fatal(e)
	}
	if e = s.Begin("interrupted", "unfinished", "other"); e != nil {
		t.Fatal(e)
	}
	if e = s.Close(); e != nil {
		t.Fatal(e)
	}
	s, e = Open(path)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	if _, e = s.Get("file", 1); e != nil {
		t.Fatal("manifest not persistent", e)
	}
	nodes, e := s.Nodes()
	if e != nil || len(nodes) != 1 {
		t.Fatal("membership not persistent", e)
	}
	state, _, e := s.Session("unfinished")
	if e != nil || state != "FAILED" {
		t.Fatal("interrupted session not recovered", state, e)
	}
}
