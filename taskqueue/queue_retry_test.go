package taskqueue

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

// claimFail reports a retryable failure for the lease and returns the task.
func claimRetryableFail(t *testing.T, q *Queue, ctx context.Context, l *Lease, msg string) *Task {
	t.Helper()
	err := q.Fail(ctx, Result{
		TaskID:       l.TaskID,
		FencingToken: l.FencingToken,
		Kind:         FailureKindRetryable,
		Message:      msg,
		Cause:        Retryable(fmt.Errorf("transient: %s", msg), msg),
	})
	if err != nil {
		t.Fatalf("fail: %v", err)
	}
	task, err := q.Get(ctx, l.TaskID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	return task
}

func TestRetryableThenSuccess(t *testing.T) {
	q, clock := newClockQueue(t, Config{WorkerID: "w", BaseBackoff: 10 * time.Millisecond})
	ctx := context.Background()
	task := mustEnqueue(t, q, ctx, "retry-ok", nil)

	l1 := claimWithin(t, q, time.Second)
	if l1.Attempt != 1 {
		t.Fatalf("attempt=%d want 1", l1.Attempt)
	}
	t1 := claimRetryableFail(t, q, ctx, l1, "timeout-1")
	if t1.State != StatePending {
		t.Fatalf("state=%s want pending", t1.State)
	}
	if t1.RunAt != clock.now().Add(10*time.Millisecond) {
		t.Fatalf("nextRun=%s want +10ms (judgement: delayed retry, not immediate re-run)", t1.RunAt)
	}
	t.Logf("task=%s attempt=1 decision=retry backoff=%s failures=%d runAt=%s",
		task.ID, 10*time.Millisecond, len(t1.Failures), t1.RunAt.Format(time.RFC3339Nano))

	// Before backoff elapses nothing is claimable.
	noCtx, cancel := context.WithTimeout(ctx, 15*time.Millisecond)
	defer cancel()
	if _, err := q.Claim(noCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("early claim err=%v want deadline (judgement: delay must be honored)", err)
	}
	clock.advance(11 * time.Millisecond)

	l2 := claimWithin(t, q, time.Second)
	if l2.Attempt != 2 || l2.FencingToken <= l1.FencingToken {
		t.Fatalf("second lease attempt=%d token=%d (l1 token=%d)", l2.Attempt, l2.FencingToken, l1.FencingToken)
	}
	if err := q.Complete(ctx, task.ID, l2.FencingToken); err != nil {
		t.Fatalf("complete: %v", err)
	}
	done := assertState(t, q, ctx, task.ID, StateSucceeded)
	if done.Attempt != 2 || len(done.Failures) != 1 {
		t.Fatalf("attempt=%d failures=%d want 2/1", done.Attempt, len(done.Failures))
	}
	if done.Failures[0].Kind != FailureKindRetryable {
		t.Fatalf("failure kind=%s want retryable (judgement: reason chain must distinguish kinds)", done.Failures[0].Kind)
	}
	t.Logf("task=%s attempt=2 decision=complete state=%s failures=%d chain0.kind=%s chain0.cause=%q",
		task.ID, done.State, len(done.Failures), done.Failures[0].Kind, done.Failures[0].Cause)
}

func TestRetryExhaustedGoesDead(t *testing.T) {
	q, clock := newClockQueue(t, Config{WorkerID: "w", MaxAttempts: 3, BaseBackoff: time.Millisecond})
	ctx := context.Background()
	task := mustEnqueue(t, q, ctx, "retry-exhaust", nil)

	for attempt := 1; attempt <= 3; attempt++ {
		l := claimWithin(t, q, time.Second)
		if l.Attempt != attempt {
			t.Fatalf("attempt=%d want %d", l.Attempt, attempt)
		}
		updated := claimRetryableFail(t, q, ctx, l, fmt.Sprintf("boom-%d", attempt))
		t.Logf("task=%s attempt=%d decision=fail-retryable failures=%d state=%s runAt=%s",
			task.ID, attempt, len(updated.Failures), updated.State, updated.RunAt.Format(time.RFC3339Nano))
		if attempt < 3 {
			if updated.State != StatePending {
				t.Fatalf("after attempt %d state=%s want pending", attempt, updated.State)
			}
			clock.advance(2 * time.Millisecond)
		} else {
			if updated.State != StateDead {
				t.Fatalf("after final attempt state=%s want dead (judgement: budget exhausted => dead letter)", updated.State)
			}
		}
	}

	dead, err := q.DeadLetters(ctx)
	if err != nil {
		t.Fatalf("dead letters: %v", err)
	}
	if len(dead) != 1 || dead[0].ID != task.ID {
		t.Fatalf("dead letters=%v want [%s]", dead, task.ID)
	}
	t0 := dead[0]
	if len(t0.Failures) != 3 {
		t.Fatalf("failure chain len=%d want 3 (judgement: full reason chain preserved)", len(t0.Failures))
	}
	for i, f := range t0.Failures {
		if !f.Dead && i == 2 {
			t.Fatalf("final failure must be marked Dead")
		}
		t.Logf("task=%s chain[%d] attempt=%d kind=%s dead=%v message=%q cause=%q",
			task.ID, i, f.Attempt, f.Kind, f.Dead, f.Message, f.Cause)
	}
	stats, _ := q.Stats(ctx)
	if stats.Dead != 1 || stats.Pending != 0 || stats.Running != 0 {
		t.Fatalf("stats=%+v want dead=1 others=0 (judgement: no loss, no stall)", stats)
	}
	// Dead task can never be claimed again.
	noCtx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	if _, err := q.Claim(noCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("claim dead task err=%v want deadline", err)
	}
}

func TestPermanentFailureGoesDeadImmediately(t *testing.T) {
	q, _ := newClockQueue(t, Config{WorkerID: "w"})
	ctx := context.Background()
	task := mustEnqueue(t, q, ctx, "perm-fail", nil)
	l := claimWithin(t, q, time.Second)

	err := q.Fail(ctx, Result{
		TaskID:       l.TaskID,
		FencingToken: l.FencingToken,
		Kind:         FailureKindPermanent,
		Message:      "validation rejected payload",
		Cause:        Permanent(fmt.Errorf("field x is negative"), "invalid request"),
	})
	if err != nil {
		t.Fatalf("permanent fail: %v", err)
	}
	t0 := assertState(t, q, ctx, task.ID, StateDead)
	if t0.Attempt != 1 {
		t.Fatalf("attempt=%d want 1 (permanent failure must not retry)", t0.Attempt)
	}
	f := t0.Failures[0]
	if f.Kind != FailureKindPermanent || !f.Dead {
		t.Fatalf("kind=%s dead=%v want permanent/true", f.Kind, f.Dead)
	}
	t.Logf("task=%s attempt=%d decision=dead-immediately kind=%s message=%q cause=%q (judgement: permanent vs retryable distinguished)",
		task.ID, t0.Attempt, f.Kind, f.Message, f.Cause)
}

func TestCauseErrorDrivesClassification(t *testing.T) {
	// A report with Kind=None but a RetryableError cause must be classified
	// retryable; a plain error must be permanent.
	q, clock := newClockQueue(t, Config{WorkerID: "w", BaseBackoff: time.Millisecond})
	ctx := context.Background()

	// Permanent case first so it cannot be reordered behind a due retry.
	permTask := mustEnqueue(t, q, ctx, "cause-perm", nil)
	l2 := claimWithin(t, q, time.Second)
	if err := q.Fail(ctx, Result{
		TaskID: l2.TaskID, FencingToken: l2.FencingToken,
		Cause: errors.New("sql: no rows"),
	}); err != nil {
		t.Fatalf("fail: %v", err)
	}
	pt, _ := q.Get(ctx, permTask.ID)
	if pt.State != StateDead || pt.Failures[0].Kind != FailureKindPermanent {
		t.Fatalf("plain-error task state=%s kind=%s want dead/permanent", pt.State, pt.Failures[0].Kind)
	}
	t.Logf("task=%s classified from plain error kind=%s (judgement: unknown error is permanent, no silent retry)",
		permTask.ID, pt.Failures[0].Kind)

	retryTask := mustEnqueue(t, q, ctx, "cause-retry", nil)
	l := claimWithin(t, q, time.Second)
	if err := q.Fail(ctx, Result{
		TaskID: l.TaskID, FencingToken: l.FencingToken,
		Cause: Retryable(errors.New("reset by peer"), "network"),
	}); err != nil {
		t.Fatalf("fail: %v", err)
	}
	rt, _ := q.Get(ctx, retryTask.ID)
	if rt.State != StatePending || rt.Failures[0].Kind != FailureKindRetryable {
		t.Fatalf("cause-classified task state=%s kind=%s want pending/retryable", rt.State, rt.Failures[0].Kind)
	}
	t.Logf("task=%s classified from cause kind=%s (judgement: error type drives kind)",
		retryTask.ID, rt.Failures[0].Kind)
	clock.advance(2 * time.Millisecond)
}
