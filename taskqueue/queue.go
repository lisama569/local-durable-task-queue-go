package taskqueue

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// Config configures a Queue.
type Config struct {
	// Dir is the local directory holding queue data (created if absent).
	Dir string
	// WorkerID identifies this process in leases and failure records.
	WorkerID string
	// LeaseTTL is how long a claim is valid. Expired leases are reclaimed.
	LeaseTTL time.Duration
	// MaxAttempts is the default per-task attempt budget.
	MaxAttempts int
	// BaseBackoff is the delay before the first retry.
	BaseBackoff time.Duration
	// MaxBackoff caps the exponential backoff.
	MaxBackoff time.Duration
	// ClaimPollInterval controls how often Claim looks for work after an
	// empty sweep. Default 50ms.
	ClaimPollInterval time.Duration
	// Now overrides the clock (tests). Defaults to time.Now.
	Now func() time.Time
}

// Queue is an embeddable durable local task queue.
type Queue struct {
	cfg Config
	st  store
	now func() time.Time
}

const (
	defaultLeaseTTL     = 30 * time.Second
	defaultMaxAttempts  = 3
	defaultBaseBackoff  = 200 * time.Millisecond
	defaultMaxBackoff   = 10 * time.Second
	defaultPollInterval = 50 * time.Millisecond
)

// Open opens (or creates) the queue in cfg.Dir and immediately re-enqueues
// any running leases left behind by a previous (crashed) process.
func Open(ctx context.Context, cfg Config) (*Queue, error) {
	if cfg.Dir == "" {
		return nil, fmt.Errorf("%w: Dir is required", ErrInvalidArgument)
	}
	if cfg.WorkerID == "" {
		cfg.WorkerID = "worker-local"
	}
	if cfg.LeaseTTL <= 0 {
		cfg.LeaseTTL = defaultLeaseTTL
	}
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = defaultMaxAttempts
	}
	if cfg.BaseBackoff <= 0 {
		cfg.BaseBackoff = defaultBaseBackoff
	}
	if cfg.MaxBackoff <= 0 {
		cfg.MaxBackoff = defaultMaxBackoff
	}
	if cfg.ClaimPollInterval <= 0 {
		cfg.ClaimPollInterval = defaultPollInterval
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	st, err := openFileStore(cfg.Dir)
	if err != nil {
		return nil, err
	}
	q := &Queue{cfg: cfg, st: st, now: cfg.Now}
	recovered, err := st.RecoverOnOpen(ctx, q.now())
	if err != nil {
		_ = st.Close()
		return nil, err
	}
	if len(recovered) > 0 {
		// Logged at best-effort via fmt? Keep surface clean: callers can
		// query; recovery is observable in task UpdatedAt / fencing token.
		_ = recovered
	}
	return q, nil
}

// Enqueue delivers a task. Reusing an existing IdempotencyID returns the
// original task (existing == true); no duplicate is ever stored.
func (q *Queue) Enqueue(ctx context.Context, req EnqueueRequest) (t *Task, existing bool, err error) {
	now := q.now()
	maxAttempts := req.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = q.cfg.MaxAttempts
	}
	id, err := newID(now)
	if err != nil {
		return nil, false, err
	}
	t = &Task{
		ID:            id,
		IdempotencyID: req.IdempotencyID,
		Payload:       append([]byte(nil), req.Payload...),
		State:         StatePending,
		Attempt:       0,
		MaxAttempts:   maxAttempts,
		RunAt:         now.Add(req.Delay),
		FencingToken:  1,
		CreatedAt:     now,
		UpdatedAt:     now,
	}
	stored, existing, err := q.st.PutTask(ctx, t)
	if err != nil {
		return nil, false, err
	}
	return stored, existing, nil
}

// Claim blocks until a runnable task can be exclusively leased or ctx ends.
// Expired leases are swept into pending before selection, so a crashed
// consumer can never block a task forever.
func (q *Queue) Claim(ctx context.Context) (*Lease, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		t, err := q.st.ClaimNext(ctx, q.now(), q.cfg.WorkerID, q.cfg.LeaseTTL)
		if err == nil {
			return &Lease{
				TaskID:       t.ID,
				Attempt:      t.Attempt,
				FencingToken: t.FencingToken,
				Payload:      t.Payload,
				LeaseExpires: t.LeaseExpires,
			}, nil
		}
		if !errors.Is(err, ErrNotFound) {
			return nil, err
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(q.cfg.ClaimPollInterval):
		}
	}
}

// Complete confirms successful processing. Only the current lease holder may
// confirm; duplicate or stale confirms return ErrFencing/ErrTerminal and never
// re-execute or re-activate anything. A repeated confirm carrying the same
// token as the already-confirmed lease is an idempotent no-op.
func (q *Queue) Complete(ctx context.Context, taskID string, fencingToken int64) error {
	t, err := q.st.GetTask(ctx, taskID)
	if err != nil {
		return err
	}
	if t.State == StateSucceeded {
		if t.FencingToken == fencingToken {
			return nil // harmless duplicate ack of the same lease
		}
		return fmt.Errorf("%w: %s already succeeded", ErrTerminal, taskID)
	}
	_, err = q.st.CompleteTask(ctx, taskID, fencingToken, q.now())
	return err
}

// Fail reports a processing outcome and applies retry / dead-letter rules:
//
//   - *RetryableError (or errors.As match) while Attempt < MaxAttempts:
//     back to pending with exponential backoff (delayed retry).
//   - *RetryableError at/over the attempt budget: dead.
//   - any other non-nil error: permanent failure, dead immediately.
//
// Stale reports (lost lease) are rejected and cannot mutate the task.
func (q *Queue) Fail(ctx context.Context, res Result) error {
	if res.TaskID == "" {
		return fmt.Errorf("%w: empty task id", ErrInvalidArgument)
	}
	kind := res.Kind
	var retErr *RetryableError
	if kind == FailureKindNone {
		// Default classification: RetryableError means transient; every
		// other non-nil error is a permanent failure.
		if res.Cause != nil && errors.As(res.Cause, &retErr) {
			kind = FailureKindRetryable
		} else {
			kind = FailureKindPermanent
		}
	}

	now := q.now()
	rec := FailureRecord{
		Kind:    kind,
		Message: res.Message,
		At:      now,
	}
	if res.Cause != nil {
		rec.Cause = res.Cause.Error()
	}

	_, err := q.st.ReportFailure(ctx, res.TaskID, res.FencingToken, now, func(t *Task) {
		rec.Attempt = t.Attempt
		switch {
		case kind == FailureKindRetryable && t.Attempt < t.MaxAttempts:
			rec.Backoff = q.backoff(t.Attempt)
			rec.NextRun = now.Add(rec.Backoff)
			t.Failures = append(t.Failures, rec)
			t.State = StatePending
			t.RunAt = rec.NextRun
			t.LeasedBy = ""
			t.LeaseExpires = time.Time{}
			t.FencingToken++ // stale loser lease may no longer report
		default:
			rec.Dead = true
			t.Failures = append(t.Failures, rec)
			t.State = StateDead
			t.RunAt = time.Time{}
			t.LeasedBy = ""
			t.LeaseExpires = time.Time{}
		}
	})
	return err
}

// backoff returns the exponential delay before retry number `attempt`
// (attempts are 1-based): base * 2^(attempt-1), capped at MaxBackoff, with
// deterministic jitter-free growth so tests can reason about ordering.
func (q *Queue) backoff(attempt int) time.Duration {
	d := q.cfg.BaseBackoff
	for i := 1; i < attempt; i++ {
		d *= 2
		if d >= q.cfg.MaxBackoff {
			return q.cfg.MaxBackoff
		}
	}
	if d > q.cfg.MaxBackoff {
		return q.cfg.MaxBackoff
	}
	return d
}

// Get returns the current state of one task.
func (q *Queue) Get(ctx context.Context, id string) (*Task, error) {
	return q.st.GetTask(ctx, id)
}

// DeadLetters returns all dead-lettered tasks with their full failure chains,
// newest failure last, in task-ID order.
func (q *Queue) DeadLetters(ctx context.Context) ([]*Task, error) {
	all, err := q.st.ListTasks(ctx)
	if err != nil {
		return nil, err
	}
	var out []*Task
	for _, t := range all {
		if t.State == StateDead {
			out = append(out, t)
		}
	}
	return out, nil
}

// Stats returns queue counters.
func (q *Queue) Stats(ctx context.Context) (Stats, error) {
	all, err := q.st.ListTasks(ctx)
	if err != nil {
		return Stats{}, err
	}
	var s Stats
	for _, t := range all {
		s.Total++
		switch t.State {
		case StatePending:
			s.Pending++
		case StateRunning:
			s.Running++
		case StateSucceeded:
			s.Succeeded++
		case StateDead:
			s.Dead++
		}
	}
	return s, nil
}

// Close flushes and releases the queue. After Close, methods return errors.
func (q *Queue) Close() error { return q.st.Close() }
