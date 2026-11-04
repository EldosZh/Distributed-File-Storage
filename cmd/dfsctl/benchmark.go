package main

import (
	"context"
	"crypto/sha256"
	"flag"
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"
	"sync"
	"time"

	"distributed-file-storage/internal/model"
	"distributed-file-storage/pkg/client"
)

func newAdmin(ctx context.Context, url, token string) (*http.Request, error) {
	r, e := http.NewRequestWithContext(ctx, "POST", url, nil)
	if e == nil {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	return r, e
}

type zeros struct{}

func (zeros) Read(p []byte) (int, error) { clear(p); return len(p), nil }

type measurement struct {
	Name     string  `json:"name"`
	MiBs     float64 `json:"mib_per_second"`
	Requests float64 `json:"requests_per_second"`
	P50      float64 `json:"p50_ms"`
	P95      float64 `json:"p95_ms"`
	P99      float64 `json:"p99_ms"`
	Errors   int     `json:"errors"`
}

func measure(name string, n, parallel int, size int64, fn func() error) measurement {
	var mu sync.Mutex
	lat := []float64{}
	bad := 0
	jobs := make(chan int)
	var wg sync.WaitGroup
	start := time.Now()
	for i := 0; i < parallel; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range jobs {
				s := time.Now()
				e := fn()
				d := float64(time.Since(s)) / float64(time.Millisecond)
				mu.Lock()
				lat = append(lat, d)
				if e != nil {
					bad++
				}
				mu.Unlock()
			}
		}()
	}
	for i := 0; i < n; i++ {
		jobs <- i
	}
	close(jobs)
	wg.Wait()
	elapsed := time.Since(start).Seconds()
	sort.Float64s(lat)
	percent := func(p float64) float64 { return lat[min(len(lat)-1, max(0, int(math.Ceil(float64(len(lat))*p))-1))] }
	return measurement{name, float64(int64(n-bad)*size) / (1 << 20) / elapsed, float64(n-bad) / elapsed, percent(.50), percent(.95), percent(.99), bad}
}
func benchmark(ctx context.Context, c *client.Client, args []string) error {
	fs := flag.NewFlagSet("benchmark", flag.ContinueOnError)
	size := fs.Int64("size", 8<<20, "streamed bytes per request")
	n := fs.Int("requests", 5, "requests per phase")
	parallel := fs.Int("concurrency", 2, "parallel operations")
	if e := fs.Parse(args); e != nil {
		return e
	}
	if *size < 0 || *size > 1<<30 || *n < 1 || *parallel < 1 || *parallel > 32 {
		return fmt.Errorf("invalid benchmark bounds")
	}
	var mu sync.Mutex
	ids := []string{}
	upload := func() error {
		f, e := c.Upload(ctx, "benchmark.bin", io.LimitReader(zeros{}, *size), "", "")
		if e == nil {
			mu.Lock()
			ids = append(ids, f.ID)
			mu.Unlock()
		}
		return e
	}
	defer func() {
		for _, id := range ids {
			r, e := c.Do(ctx, "DELETE", "/api/v1/files/"+id, nil)
			if e == nil {
				r.Body.Close()
			}
		}
	}()
	rows := []measurement{measure("upload", *n, 1, *size, upload), measure("concurrent_upload", *n, *parallel, *size, upload)}
	if len(ids) == 0 {
		return fmt.Errorf("all benchmark uploads failed")
	}
	download := func() error { return c.Download(ctx, ids[0], 0, io.Discard) }
	rows = append(rows, measure("download", *n, 1, *size, download), measure("concurrent_download", *n, *parallel, *size, download), measure("metadata_lookup", *n, 1, 0, func() error { var f model.File; return c.JSON(ctx, "/api/v1/files/"+ids[0], &f) }), measure("sha256", *n, 1, *size, func() error { _, e := io.Copy(sha256.New(), io.LimitReader(zeros{}, *size)); return e }))
	var cluster any
	if e := c.JSON(ctx, "/api/v1/cluster/status", &cluster); e != nil {
		return e
	}
	return output(struct {
		Environment string        `json:"environment"`
		Bytes       int64         `json:"file_bytes"`
		Concurrent  int           `json:"concurrency"`
		Cluster     any           `json:"cluster"`
		Results     []measurement `json:"results"`
		Replication string        `json:"replication_measurement"`
	}{"client-observed; deployment topology supplied by operator", *size, *parallel, cluster, rows, "upload includes strict replica writes; use transfer/repair metrics for internal timings; no isolated replication throughput measurement"})
}
