// Package client provides a streaming client for the public coordinator API.
package client

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"distributed-file-storage/internal/model"
)

type Client struct {
	Address string
	HTTP    *http.Client
}

// Public aliases let applications name manifests without importing an internal package.
type File = model.File
type Chunk = model.Chunk

func New(address string) *Client {
	return &Client{strings.TrimRight(address, "/"), &http.Client{Timeout: 30 * time.Minute}}
}
func (c *Client) Do(ctx context.Context, method, path string, body io.Reader) (*http.Response, error) {
	r, e := http.NewRequestWithContext(ctx, method, c.Address+path, body)
	if e != nil {
		return nil, e
	}
	resp, e := c.HTTP.Do(r)
	if e != nil {
		return nil, e
	}
	if resp.StatusCode >= 300 {
		defer resp.Body.Close()
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 8192))
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, b)
	}
	return resp, nil
}
func (c *Client) JSON(ctx context.Context, path string, v any) error {
	r, e := c.Do(ctx, "GET", path, nil)
	if e != nil {
		return e
	}
	defer r.Body.Close()
	return json.NewDecoder(r.Body).Decode(v)
}
func (c *Client) Upload(ctx context.Context, name string, body io.Reader, key, existing string) (model.File, error) {
	var f model.File
	r, e := http.NewRequestWithContext(ctx, "POST", c.Address+"/api/v1/files?name="+url.QueryEscape(name)+"&file_id="+url.QueryEscape(existing), body)
	if e != nil {
		return f, e
	}
	r.Header.Set("Content-Type", "application/octet-stream")
	r.Header.Set("Idempotency-Key", key)
	resp, e := c.HTTP.Do(r)
	if e != nil {
		return f, e
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 8192))
		return f, fmt.Errorf("HTTP %d: %s", resp.StatusCode, b)
	}
	e = json.NewDecoder(resp.Body).Decode(&f)
	return f, e
}

// Download verifies the whole file and final trailer. Callers should write to a
// temporary file and rename only after this method succeeds.
func (c *Client) Download(ctx context.Context, id string, version int, w io.Writer) error {
	var f model.File
	path := "/api/v1/files/" + url.PathEscape(id)
	if version > 0 {
		path += fmt.Sprintf("/versions/%d", version)
	}
	if e := c.JSON(ctx, path, &f); e != nil {
		return e
	}
	resp, e := c.Do(ctx, "GET", "/api/v1/files/"+url.PathEscape(id)+fmt.Sprintf("/download?version=%d", f.Version), nil)
	if e != nil {
		return e
	}
	defer resp.Body.Close()
	h := sha256.New()
	size, e := io.Copy(io.MultiWriter(w, h), resp.Body)
	if e != nil {
		return e
	}
	if size != f.Size || hex.EncodeToString(h.Sum(nil)) != f.Checksum || resp.Trailer.Get("X-DFS-Integrity") != "verified" {
		return fmt.Errorf("download integrity verification failed")
	}
	return nil
}
