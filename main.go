package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"time"
)

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
func bad(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, 4096)
	defer r.Body.Close()
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()
	if err := d.Decode(v); err != nil {
		bad(w, 400, "invalid JSON body (maximum 4 KB)")
		return false
	}
	if err := d.Decode(new(any)); err != io.EOF {
		bad(w, 400, "expected exactly one JSON object")
		return false
	}
	return true
}
func idFrom(w http.ResponseWriter, r *http.Request) (int64, bool) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id < 1 {
		bad(w, 400, "invalid task id")
		return 0, false
	}
	return id, true
}
func storeError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, errNotFound):
		bad(w, 404, err.Error())
	case errors.Is(err, errTitle):
		bad(w, 400, err.Error())
	default:
		slog.Error("storage error", "error", err)
		bad(w, 500, "storage error")
	}
}
func handler(s *Store) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) { writeJSON(w, 200, map[string]string{"status": "ok"}) })
	mux.HandleFunc("GET /tasks", func(w http.ResponseWriter, r *http.Request) { writeJSON(w, 200, s.List()) })
	mux.HandleFunc("POST /tasks", func(w http.ResponseWriter, r *http.Request) {
		var in struct {
			Title string `json:"title"`
		}
		if !decode(w, r, &in) {
			return
		}
		t, err := s.Create(in.Title)
		if err != nil {
			storeError(w, err)
			return
		}
		w.Header().Set("Location", "/tasks/"+strconv.FormatInt(t.ID, 10))
		writeJSON(w, 201, t)
	})
	mux.HandleFunc("PATCH /tasks/{id}", func(w http.ResponseWriter, r *http.Request) {
		id, ok := idFrom(w, r)
		if !ok {
			return
		}
		var in struct {
			Done *bool `json:"done"`
		}
		if !decode(w, r, &in) {
			return
		}
		if in.Done == nil {
			bad(w, 400, "done is required")
			return
		}
		t, err := s.Complete(id, *in.Done)
		if err != nil {
			storeError(w, err)
			return
		}
		writeJSON(w, 200, t)
	})
	mux.HandleFunc("DELETE /tasks/{id}", func(w http.ResponseWriter, r *http.Request) {
		id, ok := idFrom(w, r)
		if !ok {
			return
		}
		if err := s.Delete(id); err != nil {
			storeError(w, err)
			return
		}
		w.WriteHeader(204)
	})
	return mux
}
func run() error {
	addr := flag.String("addr", "127.0.0.1:8080", "listen address (local only by default)")
	data := flag.String("data", "tasks.jsonl", "event log file; run only one process per file")
	flag.Parse()
	s, err := OpenStore(*data)
	if err != nil {
		return err
	}
	defer s.Close()
	server := &http.Server{Addr: *addr, Handler: handler(s), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 60 * time.Second}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	errs := make(chan error, 1)
	go func() {
		slog.Info("Taskbox ready", "url", "http://"+*addr, "data", *data)
		errs <- server.ListenAndServe()
	}()
	select {
	case err := <-errs:
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	case <-ctx.Done():
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return server.Shutdown(shutdown)
}
func main() {
	if err := run(); err != nil {
		slog.Error("Taskbox stopped", "error", err)
		os.Exit(1)
	}
}
