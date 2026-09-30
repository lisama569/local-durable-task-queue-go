package taskqueue

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// fileStore is an append-only, fsynced, local durability layer.
//
// On disk each task mutation is appended as one self-describing record:
//
//	["LDTQ" magic][version byte][type byte][u32 payload length][JSON payload][u32 crc32]
//
// Every record carries the full Task snapshot, so recovery is a single
// ordered replay (last snapshot wins). A torn tail from a crash mid-write is
// detected by CRC and stops replay; all earlier fsynced records remain valid.
type fileStore struct {
	dir         string
	f           *os.File
	mu          sync.Mutex
	tasks       map[string]*Task
	releaseLock func() error
}

const (
	logName    = "queue.log"
	lockName   = "queue.lock"
	logMagic   = "LDTQ"
	logVersion = byte(1)
	recTask    = byte(1)
)

func openFileStore(dir string) (_ *fileStore, err error) {
	if dir == "" {
		return nil, fmt.Errorf("%w: empty storage directory", ErrInvalidArgument)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("taskqueue: create storage dir: %w", err)
	}

	release, err := acquireDirLock(filepath.Join(dir, lockName))
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			_ = release()
		}
	}()

	s := &fileStore{dir: dir, tasks: make(map[string]*Task)}
	if err := s.replay(); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(dir, logName),
		os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("taskqueue: open log: %w", err)
	}
	s.f = f
	s.releaseLock = release
	return s, nil
}

func (s *fileStore) PutTask(ctx context.Context, t *Task) (*Task, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if t.IdempotencyID != "" {
		for _, cur := range s.tasks {
			if cur.IdempotencyID == t.IdempotencyID {
				return cloneTask(cur), true, nil
			}
		}
	}
	s.tasks[t.ID] = cloneTask(t)
	if err := s.appendLocked(s.tasks[t.ID]); err != nil {
		return nil, false, err
	}
	return cloneTask(t), false, nil
}

func (s *fileStore) GetTask(ctx context.Context, id string) (*Task, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.tasks[id]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	return cloneTask(t), nil
}

func (s *fileStore) ClaimNext(ctx context.Context, now time.Time, workerID string, leaseTTL time.Duration) (*Task, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	// Sweep expired leases first so dead workers never block progress.
	if ids, err := s.recoverExpiredLocked(now); err != nil {
		return nil, err
	} else if len(ids) > 0 {
		// continue below to claim from the recovered set
	}

	var best *Task
	for _, t := range s.tasks {
		if t.State != StatePending || now.Before(t.RunAt) {
			continue
		}
		if best == nil || t.RunAt.Before(best.RunAt) ||
			(t.RunAt.Equal(best.RunAt) && t.ID < best.ID) {
			best = t
		}
	}
	if best == nil {
		return nil, ErrNotFound
	}

	best.State = StateRunning
	best.Attempt++
	best.LeasedBy = workerID
	best.LeaseExpires = now.Add(leaseTTL)
	best.FencingToken++ // invalidate every previous lease/report
	best.UpdatedAt = now
	if err := s.appendLocked(best); err != nil {
		return nil, err
	}
	return cloneTask(best), nil
}

func (s *fileStore) SaveTask(ctx context.Context, t *Task) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.tasks[t.ID]
	if !ok {
		return fmt.Errorf("%w: %s", ErrNotFound, t.ID)
	}
	// Compare-and-swap: a stale report (expired lease, recovered task) must
	// never overwrite the current generation.
	if cur.FencingToken != t.FencingToken {
		return fmt.Errorf("%w: task %s token %d != current %d",
			ErrFencing, t.ID, t.FencingToken, cur.FencingToken)
	}
	s.tasks[t.ID] = cloneTask(t)
	return s.appendLocked(s.tasks[t.ID])
}

func (s *fileStore) CompleteTask(ctx context.Context, id string, token int64, now time.Time) (*Task, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	t, err := s.requireRunning(id, token)
	if err != nil {
		return nil, err
	}
	t.State = StateSucceeded
	t.LeasedBy = ""
	t.LeaseExpires = time.Time{}
	t.UpdatedAt = now
	if err := s.appendLocked(t); err != nil {
		return nil, err
	}
	return cloneTask(t), nil
}

func (s *fileStore) ReportFailure(ctx context.Context, id string, token int64, now time.Time, mut func(*Task)) (*Task, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	t, err := s.requireRunning(id, token)
	if err != nil {
		return nil, err
	}
	mut(t)
	t.UpdatedAt = now
	if err := s.appendLocked(t); err != nil {
		return nil, err
	}
	return cloneTask(t), nil
}

// requireRunning enforces the lease boundary under the store lock.
func (s *fileStore) requireRunning(id string, token int64) (*Task, error) {
	t, ok := s.tasks[id]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	switch t.State {
	case StateSucceeded, StateDead:
		return nil, fmt.Errorf("%w: %s is %s", ErrTerminal, id, t.State)
	case StateRunning:
		if t.FencingToken != token {
			return nil, fmt.Errorf("%w: task %s token %d != current %d",
				ErrFencing, id, token, t.FencingToken)
		}
		return t, nil
	default:
		return nil, fmt.Errorf("%w: %s is %s", ErrNotRunning, id, t.State)
	}
}

func (s *fileStore) ListTasks(ctx context.Context) ([]*Task, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*Task, 0, len(s.tasks))
	for _, t := range s.tasks {
		out = append(out, cloneTask(t))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func (s *fileStore) RecoverExpired(ctx context.Context, now time.Time) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.recoverExpiredLocked(now)
}

// recoverExpiredLocked moves expired running leases back to pending.
// The fencing token is bumped, so reports from the dead lease fail CAS.
func (s *fileStore) recoverExpiredLocked(now time.Time) ([]string, error) {
	var ids []string
	for _, t := range s.tasks {
		if t.State == StateRunning && !t.LeaseExpires.After(now) {
			t.State = StatePending
			t.LeasedBy = ""
			t.LeaseExpires = time.Time{}
			t.FencingToken++
			t.UpdatedAt = now
			ids = append(ids, t.ID)
			if err := s.appendLocked(t); err != nil {
				return nil, err
			}
		}
	}
	sort.Strings(ids)
	return ids, nil
}

func (s *fileStore) RecoverOnOpen(ctx context.Context, now time.Time) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var ids []string
	for _, t := range s.tasks {
		// A lease survived only in memory of a process that is now gone.
		// Expire it immediately regardless of its old deadline.
		if t.State == StateRunning {
			t.State = StatePending
			t.LeasedBy = ""
			t.LeaseExpires = time.Time{}
			t.FencingToken++
			t.UpdatedAt = now
			ids = append(ids, t.ID)
			if err := s.appendLocked(t); err != nil {
				return nil, err
			}
		}
	}
	sort.Strings(ids)
	return ids, nil
}

func (s *fileStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var firstErr error
	if s.f != nil {
		if err := s.f.Sync(); err != nil {
			firstErr = err
		}
		if err := s.f.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		s.f = nil
	}
	if s.releaseLock != nil {
		if err := s.releaseLock(); err != nil && firstErr == nil {
			firstErr = err
		}
		s.releaseLock = nil
	}
	return firstErr
}

// ---------- append-only log ----------

func (s *fileStore) appendLocked(t *Task) error {
	payload, err := json.Marshal(t)
	if err != nil {
		return fmt.Errorf("taskqueue: encode task: %w", err)
	}
	var buf bytes.Buffer
	buf.WriteString(logMagic)
	buf.WriteByte(logVersion)
	buf.WriteByte(recTask)
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(payload)))
	buf.Write(lenBuf[:])
	buf.Write(payload)
	var crcBuf [4]byte
	binary.BigEndian.PutUint32(crcBuf[:], crc32.ChecksumIEEE(buf.Bytes()))
	buf.Write(crcBuf[:])

	if _, err := s.f.Write(buf.Bytes()); err != nil {
		return fmt.Errorf("taskqueue: write log: %w", err)
	}
	if err := s.f.Sync(); err != nil {
		return fmt.Errorf("taskqueue: fsync log: %w", err)
	}
	return nil
}

func (s *fileStore) replay() error {
	data, err := os.ReadFile(filepath.Join(s.dir, logName))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("taskqueue: read log: %w", err)
	}
	r := bytes.NewReader(data)
	hdr := make([]byte, 4+1+1+4)
	valid := int64(0)
	for {
		if _, err := io.ReadFull(r, hdr); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				break // clean or torn tail
			}
			return err
		}
		if string(hdr[0:4]) != logMagic {
			return s.truncateTail(valid, "bad magic")
		}
		if hdr[4] != logVersion || hdr[5] != recTask {
			return s.truncateTail(valid, "unsupported record")
		}
		n := int(binary.BigEndian.Uint32(hdr[6:10]))
		rest := make([]byte, n+4)
		if _, err := io.ReadFull(r, rest); err != nil {
			break // torn payload/crc tail
		}
		payload, crc := rest[:n], rest[n:]
		storedCRC := binary.BigEndian.Uint32(crc)
		if crc32.ChecksumIEEE(append(hdr, payload...)) != storedCRC {
			return s.truncateTail(valid, "crc mismatch")
		}
		var t Task
		if err := json.Unmarshal(payload, &t); err != nil {
			return s.truncateTail(valid, "bad payload")
		}
		s.tasks[t.ID] = &t
		valid = r.Size() - int64(r.Len())
	}
	return nil
}

// truncateTail drops every byte from offset onward, removing a torn record
// before the file is reopened for appending.
func (s *fileStore) truncateTail(offset int64, reason string) error {
	if err := os.Truncate(filepath.Join(s.dir, logName), offset); err != nil {
		return fmt.Errorf("taskqueue: truncate corrupt log (%s): %w", reason, err)
	}
	return nil
}

// cloneTask returns a deep-enough copy so callers can never mutate stored
// state outside the store lock.
func cloneTask(t *Task) *Task {
	cp := *t
	if t.Payload != nil {
		cp.Payload = append([]byte(nil), t.Payload...)
	}
	if len(t.Failures) > 0 {
		cp.Failures = append([]FailureRecord(nil), t.Failures...)
	}
	return &cp
}
