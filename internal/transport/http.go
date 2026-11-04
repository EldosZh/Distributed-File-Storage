package transport

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func Client() *http.Client {
	return &http.Client{Timeout: 30 * time.Second, Transport: &http.Transport{Proxy: nil, DialContext: (&net.Dialer{Timeout: 3 * time.Second}).DialContext, ResponseHeaderTimeout: 10 * time.Second, MaxIdleConnsPerHost: 8}, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
}
func JSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
func Error(w http.ResponseWriter, status int, e string) {
	JSON(w, status, struct {
		Error string `json:"error"`
	}{e})
}
func Auth(token string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+token)) != 1 {
			Error(w, 401, "internal authentication required")
			return
		}
		next.ServeHTTP(w, r)
	})
}
func Request(ctx context.Context, c *http.Client, token, method, url string, body interface{ Read([]byte) (int, error) }) (*http.Response, error) {
	r, e := http.NewRequestWithContext(ctx, method, url, body)
	if e != nil {
		return nil, e
	}
	r.Header.Set("Authorization", "Bearer "+token)
	return c.Do(r)
}
func Serve(listen string, h http.Handler, background func(context.Context)) error {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	s := &http.Server{Addr: listen, Handler: Log(h), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 30 * time.Minute, WriteTimeout: 30 * time.Minute, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 32 << 10}
	s.BaseContext = func(net.Listener) context.Context { return ctx }
	done := make(chan struct{})
	go func() {
		defer close(done)
		if background != nil {
			background(ctx)
		}
	}()
	shutdownDone := make(chan struct{})
	go func() {
		defer close(shutdownDone)
		<-ctx.Done()
		stop, c := context.WithTimeout(context.Background(), 10*time.Second)
		defer c()
		if err := s.Shutdown(stop); err != nil {
			_ = s.Close()
		}
	}()
	slog.Info("server starting", "address", listen)
	e := s.ListenAndServe()
	cancel()
	<-done
	<-shutdownDone
	if e == http.ErrServerClosed {
		return nil
	}
	return e
}
func Log(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		id := time.Now().UnixNano()
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Request-ID", fmt.Sprint(id))
		recorded := &responseWriter{ResponseWriter: w, status: 200}
		next.ServeHTTP(recorded, r)
		slog.Info("request", "request_id", id, "operation", r.Method, "path", r.URL.Path, "status", recorded.status, "duration", time.Since(start))
	})
}

type responseWriter struct {
	http.ResponseWriter
	status int
	wrote  bool
}

func (w *responseWriter) WriteHeader(status int) {
	if !w.wrote {
		w.status = status
		w.wrote = true
		w.ResponseWriter.WriteHeader(status)
	}
}
func (w *responseWriter) Write(p []byte) (int, error) {
	if !w.wrote {
		w.WriteHeader(200)
	}
	return w.ResponseWriter.Write(p)
}
func (w *responseWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
