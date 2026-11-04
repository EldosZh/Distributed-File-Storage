package main

import (
	"distributed-file-storage/internal/config"
	"distributed-file-storage/internal/coordinator"
	"distributed-file-storage/internal/transport"
	"flag"
	"log/slog"
	"os"
)

func main() {
	path := flag.String("config", "configs/coordinator.yaml", "YAML configuration")
	flag.Parse()
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	cfg, e := config.Load(*path, false)
	if e != nil {
		slog.Error("configuration", "error", e)
		os.Exit(1)
	}
	c, e := coordinator.New(cfg)
	if e != nil {
		slog.Error("startup", "error", e)
		os.Exit(1)
	}
	defer c.Close()
	if e = transport.Serve(cfg.Listen, c.Handler(), c.Run); e != nil {
		slog.Error("server", "error", e)
		os.Exit(1)
	}
}
