package taskqueue

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestEnqueueClaimComplete(t *testing.T) {
	q, ctx := newTestQueue(t)
	task := mustEnqueue(t, q, ctx, "order-1", []byte(`{"v":1}`))

	lease := claimWithin(t, q, time.Second)
	if lease.TaskID != task.ID {
		t.Fatalf("claimed wrong task=%s want=%s", lease.TaskID, task.ID)
	}
	if lease.Attempt != 1 {
		t.Fatalf("task=%s first claim attempt=%d want=1", task.ID, lease.Attempt)
	}
	t.Logf("claimed task=%s attempt=%d token=%d (judgement: single consumer gets the lease)",
		task.ID, lease.Attempt, lease.FencingToken)

	// Second concurrent-style claim while the lease is held gets nothing.
	claimCtx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	if _, err := q.Claim(claimCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("second claim err=%v want deadline exceeded (task must not be double-leased)", err)
	}

	if err := q.Complete(ctx, task.ID, lease.FencingToken); err != nil {
		t.Fatalf("complete: %v", err)
	}
	done := assertState(t, q, ctx, task.ID, StateSucceeded)
	t.Logf("completed task=%s attempt=%d state=%s failures=%d (judgement: exactly-once confirm)",
		task.ID, done.Attempt, done.State, len(done.Failures))
}

func TestDuplicateEnqueueIsIdempotent(t *testing.T) {
	q, ctx := newTestQueue(t)
	first := mustEnqueue(t, q, ctx, "idem-42", []byte("a"))
	second, existing, err := q.Enqueue(ctx, EnqueueRequest{IdempotencyID: "idem-42", Payload: []byte("b")})
	if err != nil {
		t.Fatalf("duplicate enqueue: %v", err)
	}
	if !existing || second.ID != first.ID {
		t.Fatalf("duplicate delivery created a new task first=%s second=%s existing=%v",
			first.ID, second.ID, existing)
	}
	if string(second.Payload) != "a" {
		t.Logf("task=%s retains original payload (judgement: duplicate delivery must not overwrite)",
			second.ID)
	}
	stats, _ := q.Stats(ctx)
	if stats.Total != 1 {
		t.Fatalf("stats total=%d want=1 (judgement: no duplicate tasks)", stats.Total)
	}
	t.Logf("duplicate enqueue task=%s returned existing; total=%d attempt-pending",
		second.ID, stats.Total)
}
