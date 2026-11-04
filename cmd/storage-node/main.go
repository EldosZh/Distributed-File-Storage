package main

import (
	"distributed-file-storage/internal/config"
	"distributed-file-storage/internal/storage"
	"distributed-file-storage/internal/transport"
	"flag"
	"log/slog"
	"os"
)

func main() {
	path := flag.String("config", "configs/node-1.yaml", "YAML configuration")
	flag.Parse()
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	cfg, e := config.Load(*path, true)
	if e != nil {
		slog.Error("configuration", "error", e)
		os.Exit(1)
	}
	n, e := storage.New(cfg)
	if e != nil {
		slog.Error("startup", "error", e)
		os.Exit(1)
	}
	if e = transport.Serve(cfg.Listen, n.Handler(), n.Run); e != nil {
		slog.Error("server", "error", e)
		os.Exit(1)
	}
}
