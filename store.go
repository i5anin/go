package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

var errNotFound = errors.New("task not found")
var errTitle = errors.New("title must contain 1 to 200 characters")

type Task struct {
	ID        int64     `json:"id"`
	Title     string    `json:"title"`
	Done      bool      `json:"done"`
	CreatedAt time.Time `json:"created_at"`
}
type event struct {
	Op   string `json:"op"`
	Task Task   `json:"task"`
}

// Store is a single-process, append-only event log. Mutations become visible
// only after the event has been written and synced to disk.
type Store struct {
	mu     sync.Mutex
	file   *os.File
	tasks  map[int64]Task
	next   int64
	failed bool
}

func OpenStore(path string) (*Store, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	s := &Store{file: f, tasks: map[int64]Task{}, next: 1}
	scan := bufio.NewScanner(f)
	scan.Buffer(make([]byte, 4096), 1<<20)
	line := 0
	for scan.Scan() {
		line++
		var e event
		if err := json.Unmarshal(scan.Bytes(), &e); err != nil {
			f.Close()
			return nil, fmt.Errorf("invalid log line %d: %w", line, err)
		}
		if e.Task.ID <= 0 || (e.Op != "put" && e.Op != "delete") {
			f.Close()
			return nil, fmt.Errorf("invalid event at line %d", line)
		}
		if e.Op == "put" {
			s.tasks[e.Task.ID] = e.Task
		} else {
			delete(s.tasks, e.Task.ID)
		}
		if e.Task.ID >= s.next {
			s.next = e.Task.ID + 1
		}
	}
	if err := scan.Err(); err != nil {
		f.Close()
		return nil, err
	}
	// A non-newline-terminated tail may be a torn write; never append to it.
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	if st.Size() > 0 {
		var last [1]byte
		if _, err := f.ReadAt(last[:], st.Size()-1); err != nil || last[0] != '\n' {
			f.Close()
			return nil, errors.New("incomplete log tail; restore a verified backup before restarting")
		}
	}
	if _, err := f.Seek(0, 2); err != nil {
		f.Close()
		return nil, err
	}
	return s, nil
}
func (s *Store) Close() error { s.mu.Lock(); defer s.mu.Unlock(); return s.file.Close() }
func (s *Store) append(e event) error {
	if s.failed {
		return errors.New("storage is unavailable; restart after checking disk")
	}
	data, err := json.Marshal(e)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	offset, err := s.file.Seek(0, 2)
	if err != nil {
		return err
	}
	n, err := s.file.Write(data)
	if err == nil && n != len(data) {
		err = errors.New("short write")
	}
	if err == nil {
		err = s.file.Sync()
	}
	if err != nil {
		// Fail closed even when rollback succeeds: the disk needs attention.
		s.failed = true
		if rollback := s.file.Truncate(offset); rollback != nil {
			return fmt.Errorf("write: %v; rollback: %w", err, rollback)
		}
		_ = s.file.Sync()
		return err
	}
	return nil
}
func (s *Store) List() []Task {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Task, 0, len(s.tasks))
	for _, t := range s.tasks {
		out = append(out, t)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}
func (s *Store) Create(title string) (Task, error) {
	title = strings.TrimSpace(title)
	if len([]rune(title)) < 1 || len([]rune(title)) > 200 {
		return Task{}, errTitle
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	t := Task{ID: s.next, Title: title, CreatedAt: time.Now().UTC()}
	if err := s.append(event{Op: "put", Task: t}); err != nil {
		return Task{}, err
	}
	s.tasks[t.ID] = t
	s.next++
	return t, nil
}
func (s *Store) Complete(id int64, done bool) (Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.tasks[id]
	if !ok {
		return Task{}, errNotFound
	}
	t.Done = done
	if err := s.append(event{Op: "put", Task: t}); err != nil {
		return Task{}, err
	}
	s.tasks[id] = t
	return t, nil
}
func (s *Store) Delete(id int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.tasks[id]
	if !ok {
		return errNotFound
	}
	if err := s.append(event{Op: "delete", Task: t}); err != nil {
		return err
	}
	delete(s.tasks, id)
	return nil
}
