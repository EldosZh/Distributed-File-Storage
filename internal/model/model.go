package model

import "time"

type Chunk struct {
	ID       string   `json:"chunk_id"`
	Sequence int      `json:"sequence_number"`
	Offset   int64    `json:"offset"`
	Size     int64    `json:"size"`
	Checksum string   `json:"checksum"`
	Nodes    []string `json:"replica_nodes"`
}
type File struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	Size        int64     `json:"size"`
	ContentType string    `json:"content_type"`
	Created     time.Time `json:"created_at"`
	Updated     time.Time `json:"updated_at"`
	Version     int       `json:"version"`
	Checksum    string    `json:"checksum"`
	ChunkSize   int       `json:"chunk_size"`
	Chunks      []Chunk   `json:"chunks"`
	Status      string    `json:"status"`
}
type Node struct {
	ID        string    `json:"node_id"`
	Address   string    `json:"address"`
	Capacity  int64     `json:"capacity_bytes"`
	Used      int64     `json:"used_bytes"`
	Free      int64     `json:"free_bytes"`
	Active    int64     `json:"active_operations"`
	Chunks    int64     `json:"stored_chunks"`
	Heartbeat time.Time `json:"last_heartbeat"`
	State     string    `json:"status"`
}
type Repair struct {
	ID       int64  `json:"repair_id"`
	Chunk    string `json:"chunk_id"`
	Source   string `json:"source_node"`
	Target   string `json:"target_node"`
	Status   string `json:"status"`
	Attempts int    `json:"attempts"`
	Next     int64  `json:"next_attempt_at"`
	Error    string `json:"last_error"`
}
