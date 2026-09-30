package taskqueue

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

type testClock struct{ t time.Time }

func newTestClock() *testClock      { return &testClock{t: time.Date(2026, 9, 30, 10, 0, 0, 0, time.UTC)} }
func (c *testClock) now() time.Time { return c.t }
func (c *testClock) advance(d time.Duration) time.Time {
	c.t = c.t.Add(d)
	return c.t
}

func newTestQueue(t *testing.T, opts ...func(*Config)) (*Queue, context.Context) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "queue")
	clock := newTestClock()
	cfg := Config{
		Dir:               dir,
		WorkerID:          "test-worker",
		LeaseTTL:          time.Second,
		MaxAttempts:       3,
		BaseBackoff:       10 * time.Millisecond,
		MaxBackoff:        100 * time.Millisecond,
		ClaimPollInterval: time.Millisecond,
		Now:               clock.now,
	}
	for _, o := range opts {
		o(&cfg)
	}
	q, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("open queue: %v", err)
	}
	t.Cleanup(func() { _ = q.Close() })
	return q, context.Background()
}

// newClockQueue returns a queue plus the fake clock driving it, so tests can
// simulate lease expiry and delayed retries deterministically.
func newClockQueue(t *testing.T, cfg Config) (*Queue, *testClock) {
	t.Helper()
	clock := newTestClock()
	if cfg.LeaseTTL <= 0 {
		cfg.LeaseTTL = time.Second
	}
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 3
	}
	if cfg.BaseBackoff <= 0 {
		cfg.BaseBackoff = 10 * time.Millisecond
	}
	if cfg.MaxBackoff <= 0 {
		cfg.MaxBackoff = 100 * time.Millisecond
	}
	if cfg.ClaimPollInterval <= 0 {
		cfg.ClaimPollInterval = time.Millisecond
	}
	cfg.Now = clock.now
	cfg.Dir = filepath.Join(t.TempDir(), "queue")
	q, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("open clock queue: %v", err)
	}
	t.Cleanup(func() { _ = q.Close() })
	return q, clock
}

func mustEnqueue(t *testing.T, q *Queue, ctx context.Context, idem string, payload []byte) *Task {
	t.Helper()
	task, existing, err := q.Enqueue(ctx, EnqueueRequest{IdempotencyID: idem, Payload: payload})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if existing {
		t.Fatalf("enqueue unexpectedly returned existing task for fresh idempotency id")
	}
	t.Logf("enqueued task=%s idempotency=%q maxAttempts=%d", task.ID, idem, task.MaxAttempts)
	return task
}

// claimWithin claims with a timeout, failing the test if nothing appears.
func claimWithin(t *testing.T, q *Queue, d time.Duration) *Lease {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	l, err := q.Claim(ctx)
	if err != nil {
		t.Fatalf("claim within %v: %v", d, err)
	}
	return l
}

func assertState(t *testing.T, q *Queue, ctx context.Context, id string, want State) *Task {
	t.Helper()
	task, err := q.Get(ctx, id)
	if err != nil {
		t.Fatalf("get %s: %v", id, err)
	}
	if task.State != want {
		t.Fatalf("task=%s state=%s want=%s attempt=%d (judgement: final state must match)",
			id, task.State, want, task.Attempt)
	}
	return task
}
