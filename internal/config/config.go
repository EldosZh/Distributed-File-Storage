package config

import (
	"errors"
	"gopkg.in/yaml.v3"
	"os"
	"time"
)

type Config struct {
	Listen         string        `yaml:"listen"`
	Token          string        `yaml:"internal_token"`
	Database       string        `yaml:"database"`
	ChunkSize      int           `yaml:"chunk_size"`
	Replicas       int           `yaml:"replication_factor"`
	Virtual        int           `yaml:"virtual_nodes"`
	Heartbeat      time.Duration `yaml:"heartbeat_interval"`
	Timeout        time.Duration `yaml:"failure_timeout"`
	RepairInterval time.Duration `yaml:"repair_interval"`
	Workers        int           `yaml:"repair_workers"`
	RepairRate     int64         `yaml:"repair_bytes_per_second_per_worker"`
	MaxRetries     int           `yaml:"max_retries"`
	Grace          time.Duration `yaml:"gc_grace"`
	MaxUpload      int64         `yaml:"max_upload_bytes"`
	MaxConcurrent  int           `yaml:"max_concurrent_operations"`
	Allowed        []string      `yaml:"allowed_node_addresses"`
	NodeID         string        `yaml:"node_id"`
	Address        string        `yaml:"advertise_address"`
	Coordinator    string        `yaml:"coordinator_address"`
	DataDir        string        `yaml:"data_dir"`
	Capacity       int64         `yaml:"capacity_bytes"`
}

func Load(path string, node bool) (Config, error) {
	c := Config{Listen: "127.0.0.1:8080", Database: "data/metadata.db", ChunkSize: 4 << 20, Replicas: 3, Virtual: 128, Heartbeat: 5 * time.Second, Timeout: 20 * time.Second, RepairInterval: 5 * time.Second, Workers: 4, MaxRetries: 5, Grace: time.Hour, MaxUpload: 1 << 40, MaxConcurrent: 8, Capacity: 10 << 30}
	c.RepairRate = 16 << 20
	f, e := os.Open(path)
	if e != nil {
		return c, e
	}
	defer f.Close()
	d := yaml.NewDecoder(f)
	d.KnownFields(true)
	if e = d.Decode(&c); e != nil {
		return c, e
	}
	if t := os.Getenv("DFS_INTERNAL_TOKEN"); t != "" {
		c.Token = t
	}
	if len(c.Token) < 16 {
		return c, errors.New("internal_token must contain at least 16 characters (or set DFS_INTERNAL_TOKEN)")
	}
	if c.ChunkSize != 1<<20 && c.ChunkSize != 4<<20 && c.ChunkSize != 8<<20 && c.ChunkSize != 16<<20 {
		return c, errors.New("chunk_size must be 1, 4, 8 or 16 MiB")
	}
	if c.Replicas < 1 || c.Virtual < 1 || c.Workers < 1 || c.Workers > 32 || c.MaxRetries < 1 || c.MaxConcurrent < 1 || c.MaxUpload < 1 || c.Grace < 0 || c.Heartbeat <= 0 || c.Timeout < 2*c.Heartbeat || c.RepairInterval <= 0 || c.RepairRate < 1 {
		return c, errors.New("invalid limits or intervals")
	}
	if node && (c.NodeID == "" || c.Address == "" || c.Coordinator == "" || c.DataDir == "" || c.Capacity <= 0) {
		return c, errors.New("node identity, addresses, data directory and positive capacity required")
	}
	return c, nil
}
