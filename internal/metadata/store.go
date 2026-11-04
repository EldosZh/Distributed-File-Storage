package metadata

import (
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"time"

	"distributed-file-storage/internal/model"
	_ "modernc.org/sqlite"
)

type Store struct{ DB *sql.DB }

const schema = `
CREATE TABLE IF NOT EXISTS schema_migrations(version INTEGER PRIMARY KEY);
INSERT OR IGNORE INTO schema_migrations VALUES(1);
CREATE TABLE IF NOT EXISTS files(id TEXT PRIMARY KEY,name TEXT NOT NULL,deleted INTEGER NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS file_versions(file_id TEXT NOT NULL,version INTEGER NOT NULL,manifest TEXT NOT NULL,PRIMARY KEY(file_id,version));
CREATE TABLE IF NOT EXISTS chunks(id TEXT PRIMARY KEY,size INTEGER NOT NULL,created INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS file_chunks(file_id TEXT,version INTEGER,sequence INTEGER,chunk_id TEXT NOT NULL,PRIMARY KEY(file_id,version,sequence));
CREATE INDEX IF NOT EXISTS file_chunks_chunk ON file_chunks(chunk_id);
CREATE TABLE IF NOT EXISTS chunk_replicas(chunk_id TEXT,node_id TEXT,valid INTEGER NOT NULL DEFAULT 1,PRIMARY KEY(chunk_id,node_id));
CREATE TABLE IF NOT EXISTS storage_nodes(id TEXT PRIMARY KEY,record TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS upload_sessions(id TEXT PRIMARY KEY,key TEXT UNIQUE,state TEXT NOT NULL,file_id TEXT,error TEXT NOT NULL DEFAULT '',updated INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS repair_tasks(id INTEGER PRIMARY KEY,chunk_id TEXT NOT NULL,source_node TEXT NOT NULL DEFAULT '',target_node TEXT NOT NULL,status TEXT NOT NULL DEFAULT 'PENDING',attempts INTEGER NOT NULL DEFAULT 0,next_attempt_at INTEGER NOT NULL DEFAULT 0,last_error TEXT NOT NULL DEFAULT '',UNIQUE(chunk_id,target_node));
`

func Open(path string) (*Store, error) {
	if e := os.MkdirAll(filepath.Dir(path), 0700); e != nil {
		return nil, e
	}
	db, e := sql.Open("sqlite", path)
	if e != nil {
		return nil, e
	}
	db.SetMaxOpenConns(1)
	if _, e = db.Exec("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA busy_timeout=5000;" + schema); e != nil {
		db.Close()
		return nil, e
	}
	s := &Store{db}
	// Migration 2 pins idempotent replies to their original immutable version.
	var version int
	if e = db.QueryRow("SELECT MAX(version) FROM schema_migrations").Scan(&version); e != nil {
		db.Close()
		return nil, e
	}
	if version < 2 {
		tx, err := db.Begin()
		if err != nil {
			db.Close()
			return nil, err
		}
		_, err = tx.Exec("ALTER TABLE upload_sessions ADD COLUMN file_version INTEGER NOT NULL DEFAULT 0; INSERT INTO schema_migrations VALUES(2)")
		if err == nil {
			err = tx.Commit()
		} else {
			_ = tx.Rollback()
		}
		if err != nil {
			db.Close()
			return nil, err
		}
	}
	_, e = db.Exec("UPDATE upload_sessions SET state='FAILED',error='coordinator restarted' WHERE state NOT IN ('COMMITTED','FAILED','ABORTED'); UPDATE repair_tasks SET status='PENDING' WHERE status='RUNNING'")
	if e != nil {
		db.Close()
		return nil, e
	}
	return s, e
}
func (s *Store) Close() error { return s.DB.Close() }
func (s *Store) Nodes() ([]model.Node, error) {
	rows, e := s.DB.Query("SELECT record FROM storage_nodes ORDER BY id")
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []model.Node{}
	for rows.Next() {
		var b string
		var n model.Node
		if e = rows.Scan(&b); e != nil {
			return nil, e
		}
		if e = json.Unmarshal([]byte(b), &n); e != nil {
			return nil, e
		}
		out = append(out, n)
	}
	return out, rows.Err()
}
func (s *Store) SaveNode(n model.Node) error {
	b, e := json.Marshal(n)
	if e != nil {
		return e
	}
	_, e = s.DB.Exec("INSERT INTO storage_nodes(id,record) VALUES(?,?) ON CONFLICT(id) DO UPDATE SET record=excluded.record", n.ID, string(b))
	return e
}
func (s *Store) Files() ([]model.File, error) {
	rows, e := s.DB.Query(`SELECT v.manifest FROM file_versions v JOIN files f ON f.id=v.file_id WHERE f.deleted=0 AND v.version=(SELECT MAX(version) FROM file_versions WHERE file_id=f.id) ORDER BY f.id`)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []model.File{}
	for rows.Next() {
		var b string
		var f model.File
		if e = rows.Scan(&b); e != nil {
			return nil, e
		}
		if e = json.Unmarshal([]byte(b), &f); e != nil {
			return nil, e
		}
		out = append(out, f)
	}
	return out, rows.Err()
}
func (s *Store) Get(id string, version int) (model.File, error) {
	var b string
	var f model.File
	query := `SELECT manifest FROM file_versions JOIN files ON files.id=file_id WHERE file_id=? AND files.deleted=0`
	var e error
	if version == 0 {
		e = s.DB.QueryRow(query+" ORDER BY version DESC LIMIT 1", id).Scan(&b)
	} else {
		e = s.DB.QueryRow(query+" AND version=?", id, version).Scan(&b)
	}
	if e != nil {
		return f, e
	}
	e = json.Unmarshal([]byte(b), &f)
	if e != nil {
		return f, e
	}
	for i := range f.Chunks {
		f.Chunks[i].Nodes, e = s.Replicas(f.Chunks[i].ID)
		if e != nil {
			return f, e
		}
	}
	return f, nil
}
func (s *Store) Versions(id string) ([]int, error) {
	rows, e := s.DB.Query("SELECT version FROM file_versions JOIN files ON files.id=file_id WHERE file_id=? AND files.deleted=0 ORDER BY version", id)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []int{}
	for rows.Next() {
		var v int
		if e = rows.Scan(&v); e != nil {
			return nil, e
		}
		out = append(out, v)
	}
	return out, rows.Err()
}
func (s *Store) Begin(session, key, id string) error {
	var k any
	if key != "" {
		k = key
	}
	_, e := s.DB.Exec("INSERT INTO upload_sessions(id,key,state,file_id,updated) VALUES(?,?,'INITIATED',?,?)", session, k, id, time.Now().Unix())
	return e
}
func (s *Store) Session(key string) (string, string, error) {
	var state, id string
	e := s.DB.QueryRow("SELECT state,file_id FROM upload_sessions WHERE key=?", key).Scan(&state, &id)
	return state, id, e
}
func (s *Store) SessionVersion(key string) (int, error) {
	var v int
	e := s.DB.QueryRow("SELECT file_version FROM upload_sessions WHERE key=?", key).Scan(&v)
	return v, e
}
func (s *Store) State(session, state, msg string) error {
	_, e := s.DB.Exec("UPDATE upload_sessions SET state=?,error=?,updated=? WHERE id=?", state, msg, time.Now().Unix(), session)
	return e
}
func (s *Store) Track(id string, size int64) error {
	_, e := s.DB.Exec("INSERT OR IGNORE INTO chunks VALUES(?,?,?)", id, size, time.Now().Unix())
	return e
}
func (s *Store) Replica(id, node string, valid bool) error {
	_, e := s.DB.Exec("INSERT INTO chunk_replicas VALUES(?,?,?) ON CONFLICT(chunk_id,node_id) DO UPDATE SET valid=excluded.valid", id, node, valid)
	return e
}
func (s *Store) Replicas(id string) ([]string, error) {
	rows, e := s.DB.Query("SELECT node_id FROM chunk_replicas WHERE chunk_id=? AND valid=1 ORDER BY node_id", id)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []string{}
	for rows.Next() {
		var n string
		if e = rows.Scan(&n); e != nil {
			return nil, e
		}
		out = append(out, n)
	}
	return out, rows.Err()
}
func (s *Store) Commit(f *model.File, session string) error {
	tx, e := s.DB.Begin()
	if e != nil {
		return e
	}
	defer tx.Rollback()
	var sessionFile, state string
	if e = tx.QueryRow("SELECT file_id,state FROM upload_sessions WHERE id=?", session).Scan(&sessionFile, &state); e != nil {
		return e
	}
	if sessionFile != f.ID || (state != "INITIATED" && state != "UPLOADING" && state != "VERIFYING") {
		return errors.New("upload session is not active for this file")
	}
	var deleted int
	e = tx.QueryRow("SELECT deleted FROM files WHERE id=?", f.ID).Scan(&deleted)
	if e != nil && !errors.Is(e, sql.ErrNoRows) {
		return e
	}
	if e == nil && deleted != 0 {
		return errors.New("file was deleted during upload")
	}
	if _, e = tx.Exec("INSERT OR IGNORE INTO files(id,name) VALUES(?,?)", f.ID, f.Name); e != nil {
		return e
	}
	if e = tx.QueryRow("SELECT COALESCE(MAX(version),0)+1 FROM file_versions WHERE file_id=?", f.ID).Scan(&f.Version); e != nil {
		return e
	}
	f.Status = "COMMITTED"
	b, e := json.Marshal(f)
	if e != nil {
		return e
	}
	if _, e = tx.Exec("INSERT INTO file_versions VALUES(?,?,?)", f.ID, f.Version, string(b)); e != nil {
		return e
	}
	for _, c := range f.Chunks {
		if _, e = tx.Exec("INSERT INTO file_chunks VALUES(?,?,?,?)", f.ID, f.Version, c.Sequence, c.ID); e != nil {
			return e
		}
	}
	if _, e = tx.Exec("UPDATE upload_sessions SET state='COMMITTED',file_id=?,file_version=?,updated=? WHERE id=?", f.ID, f.Version, time.Now().Unix(), session); e != nil {
		return e
	}
	return tx.Commit()
}
func (s *Store) Delete(id string) error {
	res, e := s.DB.Exec("UPDATE files SET deleted=1 WHERE id=? AND deleted=0", id)
	if e != nil {
		return e
	}
	n, e := res.RowsAffected()
	if n == 0 {
		return sql.ErrNoRows
	}
	return e
}
func (s *Store) LiveChunks() ([]model.Chunk, error) {
	rows, e := s.DB.Query(`SELECT DISTINCT c.id,c.size FROM chunks c JOIN file_chunks fc ON fc.chunk_id=c.id JOIN files f ON f.id=fc.file_id WHERE f.deleted=0 ORDER BY c.id`)
	if e != nil {
		return nil, e
	}
	out := []model.Chunk{}
	for rows.Next() {
		var c model.Chunk
		if e = rows.Scan(&c.ID, &c.Size); e != nil {
			rows.Close()
			return nil, e
		}
		out = append(out, c)
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return nil, e
	}
	for i := range out {
		out[i].Nodes, e = s.Replicas(out[i].ID)
		if e != nil {
			return nil, e
		}
	}
	return out, nil
}
func (s *Store) Referenced(id string) (bool, error) {
	var n int
	e := s.DB.QueryRow("SELECT COUNT(*) FROM file_chunks fc JOIN files f ON f.id=fc.file_id WHERE chunk_id=? AND f.deleted=0", id).Scan(&n)
	return n > 0, e
}
func (s *Store) Queue(id, target string) error {
	_, e := s.DB.Exec(`INSERT INTO repair_tasks(chunk_id,target_node) SELECT ?,? WHERE (SELECT COUNT(*) FROM repair_tasks WHERE status IN ('PENDING','RUNNING'))<10000 ON CONFLICT(chunk_id,target_node) DO UPDATE SET status='PENDING',attempts=0,next_attempt_at=0 WHERE repair_tasks.status='DONE'`, id, target)
	return e
}
func (s *Store) Tasks() ([]model.Repair, error) {
	rows, e := s.DB.Query("SELECT id,chunk_id,source_node,target_node,status,attempts,next_attempt_at,last_error FROM repair_tasks ORDER BY id")
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []model.Repair{}
	for rows.Next() {
		var t model.Repair
		if e = rows.Scan(&t.ID, &t.Chunk, &t.Source, &t.Target, &t.Status, &t.Attempts, &t.Next, &t.Error); e != nil {
			return nil, e
		}
		out = append(out, t)
	}
	return out, rows.Err()
}
