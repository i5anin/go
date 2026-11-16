package main

import (
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func testStore(t *testing.T) *Store {
	t.Helper()
	s, e := OpenStore(filepath.Join(t.TempDir(), "tasks.jsonl"))
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { s.Close() })
	return s
}
func TestPersistence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "log")
	s, e := OpenStore(path)
	if e != nil {
		t.Fatal(e)
	}
	a, e := s.Create("  Learn Go  ")
	if e != nil {
		t.Fatal(e)
	}
	b, e := s.Create("Delete me")
	if e != nil {
		t.Fatal(e)
	}
	if _, e = s.Complete(a.ID, true); e != nil {
		t.Fatal(e)
	}
	if e = s.Delete(b.ID); e != nil {
		t.Fatal(e)
	}
	s.Close()
	s, e = OpenStore(path)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	tasks := s.List()
	if len(tasks) != 1 || tasks[0].Title != "Learn Go" || !tasks[0].Done {
		t.Fatalf("unexpected tasks: %+v", tasks)
	}
	c, e := s.Create("Next")
	if e != nil || c.ID <= b.ID {
		t.Fatalf("ID reuse: %+v %v", c, e)
	}
}
func TestConcurrentCreates(t *testing.T) {
	s := testStore(t)
	var wg sync.WaitGroup
	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, e := s.Create("Go"); e != nil {
				t.Error(e)
			}
		}()
	}
	wg.Wait()
	tasks := s.List()
	if len(tasks) != 30 {
		t.Fatal(len(tasks))
	}
	for i, task := range tasks {
		if task.ID != int64(i+1) {
			t.Fatal(task)
		}
	}
}
func TestCorruptLogFailsClosed(t *testing.T) {
	for _, body := range []string{"broken\n", `{"op":"put","task":{"id":1}}`, "{\"op\":\"bad\",\"task\":{\"id\":1}}\n"} {
		t.Run(body, func(t *testing.T) {
			p := filepath.Join(t.TempDir(), "log")
			os.WriteFile(p, []byte(body), 0600)
			s, e := OpenStore(p)
			if e == nil {
				s.Close()
				t.Fatal("accepted corrupt log")
			}
			data, _ := os.ReadFile(p)
			if string(data) != body {
				t.Fatal("modified corrupt input")
			}
		})
	}
}
func TestHTTP(t *testing.T) {
	s := testStore(t)
	h := handler(s)
	request := func(method, path, body string, want int) *httptest.ResponseRecorder {
		t.Helper()
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(method, path, strings.NewReader(body)))
		if w.Code != want {
			t.Fatalf("%s %s: %d, want %d: %s", method, path, w.Code, want, w.Body.String())
		}
		return w
	}
	request("GET", "/health", "", 200)
	if got := request("GET", "/tasks", "", 200).Body.String(); strings.TrimSpace(got) != "[]" {
		t.Fatal(got)
	}
	w := request("POST", "/tasks", `{"title":"Learn Go"}`, 201)
	var task Task
	if e := json.Unmarshal(w.Body.Bytes(), &task); e != nil || task.ID != 1 {
		t.Fatal(w.Body.String())
	}
	request("PATCH", "/tasks/1", `{"done":true}`, 200)
	if !s.List()[0].Done {
		t.Fatal("not completed")
	}
	request("PATCH", "/tasks/1", `{"done":false}`, 200)
	request("DELETE", "/tasks/1", "", 204)
	request("DELETE", "/tasks/1", "", 404)
	for _, tc := range []struct {
		method, path, body string
		code               int
	}{
		{"POST", "/tasks", `{"title":" "}`, 400}, {"POST", "/tasks", `{"title":"x","extra":1}`, 400},
		{"POST", "/tasks", `{"title":"x"} {}`, 400}, {"POST", "/tasks", "null", 400},
		{"POST", "/tasks", `{"title":"` + strings.Repeat("x", 5000) + `"}`, 400},
		{"PATCH", "/tasks/1", `{}`, 400}, {"PATCH", "/tasks/no", `{"done":true}`, 400},
		{"PATCH", "/tasks/999", `{"done":true}`, 404}, {"PUT", "/tasks", `{}`, 405},
	} {
		request(tc.method, tc.path, tc.body, tc.code)
	}
}
func TestWriteFailureDoesNotMutateMemory(t *testing.T) {
	s := testStore(t)
	s.file.Close()
	if _, e := s.Create("no"); e == nil {
		t.Fatal("expected disk error")
	}
	if len(s.List()) != 0 {
		t.Fatal("mutation escaped")
	}
}
