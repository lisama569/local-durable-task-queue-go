// Package worker provides a concurrent consumer pool for a taskqueue.
package worker

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"

	"github.com/lisama569/local-durable-task-queue-go/taskqueue"
)

// Executor executes one task payload.
//
// Classification of the returned error:
//
//   - nil: success, the task is confirmed.
//   - *taskqueue.RetryableError (wrapped or direct): transient; scheduled for
//     delayed retry until MaxAttempts, then dead-lettered.
//   - any other error: permanent; dead-lettered immediately.
type Executor interface {
	Execute(ctx context.Context, taskID string, attempt int, payload []byte) error
}

// Queue is the subset of *taskqueue.Queue consumed by the pool.
type Queue interface {
	Claim(ctx context.Context) (*taskqueue.Lease, error)
	Complete(ctx context.Context, taskID string, fencingToken int64) error
	Fail(ctx context.Context, res taskqueue.Result) error
}

// Logger is the minimal logging surface.
type Logger interface {
	Printf(format string, args ...any)
}

// Config configures a Pool.
type Config struct {
	// Concurrency is the number of parallel consumers (>=1).
	Concurrency int
	// Name tags log lines (defaults to "worker").
	Name string
	// Log receives structured progress lines (defaults to stdlib log).
	Log Logger
}

// Pool runs N concurrent consumers over a queue.
type Pool struct {
	q   Queue
	ex  Executor
	cfg Config
}

// New creates a pool bound to q using ex as the downstream executor.
func New(q Queue, ex Executor, cfg Config) *Pool {
	if cfg.Concurrency < 1 {
		cfg.Concurrency = 1
	}
	if cfg.Name == "" {
		cfg.Name = "worker"
	}
	if cfg.Log == nil {
		cfg.Log = log.Default()
	}
	return &Pool{q: q, ex: ex, cfg: cfg}
}

// Run starts the consumers and blocks until ctx is canceled. Claims already
// in flight finish (confirm or fail) when ctx is canceled during execution;
// claims interrupted between lease and execution expire and are reclaimed
// after restart (at-least-once, never lost).
func (p *Pool) Run(ctx context.Context) error {
	var wg sync.WaitGroup
	for i := 0; i < p.cfg.Concurrency; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			p.consume(ctx, id)
		}(i + 1)
	}
	wg.Wait()
	return ctx.Err()
}

func (p *Pool) consume(ctx context.Context, id int) {
	for {
		if ctx.Err() != nil {
			return
		}
		lease, err := p.q.Claim(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return
			}
			p.cfg.Log.Printf("%s#%d claim error: %v", p.cfg.Name, id, err)
			select {
			case <-ctx.Done():
				return
			default:
			}
			continue
		}

		done := make(chan struct{})
		var execErr error
		go func() {
			defer close(done)
			execErr = p.ex.Execute(ctx, lease.TaskID, lease.Attempt, lease.Payload)
		}()
		select {
		case <-ctx.Done():
			// Process is shutting down. Do NOT report: the lease is durable
			// and will be recovered on restart (or when its TTL expires).
			p.cfg.Log.Printf("%s#%d task=%s attempt=%d decision=abort-on-shutdown (lease will recover)",
				p.cfg.Name, id, lease.TaskID, lease.Attempt)
			<-done // don't orphan the executor goroutine if it is quick
			return
		case <-done:
		}

		p.report(ctx, id, lease, execErr)
	}
}

func (p *Pool) report(ctx context.Context, id int, lease *taskqueue.Lease, execErr error) {
	if execErr == nil {
		if err := p.q.Complete(ctx, lease.TaskID, lease.FencingToken); err != nil {
			// Stale lease: another attempt/worker owns the task now. This
			// report is ignored; nothing is re-executed here.
			p.cfg.Log.Printf("%s#%d task=%s attempt=%d decision=complete-rejected reason=%v",
				p.cfg.Name, id, lease.TaskID, lease.Attempt, err)
			return
		}
		p.cfg.Log.Printf("%s#%d task=%s attempt=%d decision=complete",
			p.cfg.Name, id, lease.TaskID, lease.Attempt)
		return
	}

	var ret *taskqueue.RetryableError
	kind := taskqueue.FailureKindPermanent
	decision := "dead"
	if errors.As(execErr, &ret) {
		kind = taskqueue.FailureKindRetryable
		decision = "retry"
	}
	err := p.q.Fail(ctx, taskqueue.Result{
		TaskID:       lease.TaskID,
		FencingToken: lease.FencingToken,
		Kind:         kind,
		Message:      fmt.Sprintf("%s failure", decision),
		Cause:        execErr,
	})
	if err != nil {
		p.cfg.Log.Printf("%s#%d task=%s attempt=%d decision=%s-rejected reason=%v",
			p.cfg.Name, id, lease.TaskID, lease.Attempt, decision, err)
		return
	}
	p.cfg.Log.Printf("%s#%d task=%s attempt=%d decision=%s reason=%q",
		p.cfg.Name, id, lease.TaskID, lease.Attempt, decision, execErr.Error())
}
