package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"distributed-file-storage/pkg/client"
)

func main() {
	if e := run(); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
func output(v any) error { e := json.NewEncoder(os.Stdout); e.SetIndent("", "  "); return e.Encode(v) }
func run() error {
	addr := os.Getenv("DFS_ADDRESS")
	if addr == "" {
		addr = "http://127.0.0.1:8080"
	}
	c := client.New(addr)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	args := os.Args[1:]
	if len(args) == 0 {
		return errors.New("usage: dfsctl upload|download|list|info|delete|nodes|cluster status|replication status|verify|benchmark|node drain|gc|version")
	}
	cmd := args[0]
	args = args[1:]
	switch cmd {
	case "version":
		fmt.Println("dfsctl 1.0.0")
		return nil
	case "upload":
		if len(args) < 1 {
			return errors.New("upload requires a path")
		}
		path := args[0]
		fs := flag.NewFlagSet(cmd, flag.ContinueOnError)
		name := fs.String("name", filepath.Base(path), "stored name")
		key := fs.String("key", "", "idempotency key")
		existing := fs.String("file-id", "", "create immutable version of existing file")
		if e := fs.Parse(args[1:]); e != nil {
			return e
		}
		f, e := os.Open(path)
		if e != nil {
			return e
		}
		defer f.Close()
		meta, e := c.Upload(ctx, *name, f, *key, *existing)
		if e != nil {
			return e
		}
		return output(meta)
	case "download", "verify":
		if len(args) < 1 {
			return errors.New("file ID required")
		}
		fs := flag.NewFlagSet(cmd, flag.ContinueOnError)
		path := fs.String("output", args[0]+".download", "output path")
		version := fs.Int("version", 0, "immutable version; default latest")
		if e := fs.Parse(args[1:]); e != nil {
			return e
		}
		if cmd == "verify" {
			if e := c.Download(ctx, args[0], *version, io.Discard); e != nil {
				return e
			}
			fmt.Println("SHA-256 verified")
			return nil
		}
		f, e := os.CreateTemp(filepath.Dir(*path), ".dfs-download-")
		if e != nil {
			return e
		}
		tmp := f.Name()
		defer os.Remove(tmp)
		e = c.Download(ctx, args[0], *version, f)
		if e == nil {
			e = f.Sync()
		}
		ce := f.Close()
		if e != nil {
			return e
		}
		if ce != nil {
			return ce
		}
		return os.Rename(tmp, *path)
	case "benchmark":
		return benchmark(ctx, c, args)
	case "gc", "node":
		token := os.Getenv("DFS_INTERNAL_TOKEN")
		if token == "" {
			return errors.New("DFS_INTERNAL_TOKEN required")
		}
		path := "/internal/gc"
		if cmd == "gc" {
			if len(args) > 0 && args[0] == "--apply" {
				path += "?dry_run=false"
			}
		} else {
			if len(args) != 2 || args[0] != "drain" {
				return errors.New("usage: dfsctl node drain <node-id>")
			}
			path = "/internal/nodes/" + args[1] + "/drain"
		}
		req, e := newAdmin(ctx, c.Address+path, token)
		if e != nil {
			return e
		}
		resp, e := c.HTTP.Do(req)
		if e != nil {
			return e
		}
		defer resp.Body.Close()
		_, e = io.Copy(os.Stdout, resp.Body)
		if resp.StatusCode >= 300 {
			return fmt.Errorf("HTTP %d", resp.StatusCode)
		}
		return e
	default:
		path := ""
		method := "GET"
		switch cmd {
		case "list":
			path = "/api/v1/files"
		case "nodes":
			path = "/api/v1/nodes"
		case "cluster", "replication":
			if len(args) != 1 || args[0] != "status" {
				return errors.New("expected status")
			}
			path = "/api/v1/" + cmd + "/status"
		case "info", "delete":
			if len(args) != 1 {
				return errors.New("file ID required")
			}
			path = "/api/v1/files/" + args[0]
			if cmd == "delete" {
				method = "DELETE"
			}
		default:
			return fmt.Errorf("unknown command %s", cmd)
		}
		resp, e := c.Do(ctx, method, path, nil)
		if e != nil {
			return e
		}
		defer resp.Body.Close()
		if method == "DELETE" {
			return nil
		}
		var v any
		if e = json.NewDecoder(resp.Body).Decode(&v); e != nil {
			return e
		}
		return output(v)
	}
}
