package taskqueue

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

// drive 用单个“消费者”反复领取-执行-回报，直到任务进入终态或达到 step 上限。
func drive(t *testing.T, q *Queue, exec *ScriptedExecutor, clk *fakeClock, maxSteps int) *Task {
	t.Helper()
	ctx := context.Background()
	var last *Task
	for step := 0; step < maxSteps; step++ {
		lease, err := q.Fetch(ctx, "driver")
		if err != nil {
			t.Fatalf("step %d fetch: %v", step, err)
		}
		if lease == nil {
			// 可能处于延迟重试窗口：推进时钟后再试。
			clk.advance(q.cfg.RetryDelay + time.Second)
			continue
		}
		result, execErr := exec.Execute(ctx, lease.Task)
		if execErr == nil {
			last, err = lease.Ack(ctx, result)
		} else {
			last, err = lease.Nack(ctx, execErr.(*ExecError))
		}
		if err != nil {
			t.Fatalf("step %d ack/nack: %v", step, err)
		}
		logTaskState(t, fmt.Sprintf("step %d", step), last)
		if last.Status == StatusSucceeded || last.Status == StatusDead {
			return last
		}
		clk.advance(q.cfg.RetryDelay + time.Millisecond)
	}
	t.Fatalf("task did not reach terminal state within %d steps", maxSteps)
	return nil
}

func TestRetryableThenSuccess(t *testing.T) {
	ctx := context.Background()
	clk := newFakeClock()
	q := newTestQueue(clk, Config{RetryDelay: 200 * time.Millisecond, MaxAttempts: 5})
	exec := NewScriptedExecutor()

	created, err := q.Enqueue(ctx, EnqueueOptions{IdempotencyKey: "retry-ok", Payload: "p"})
	if err != nil {
		t.Fatal(err)
	}
	exec.OnRetryableThenSuccess(created.ID, 2, "temporary blip", "final")

	// 前两次失败后任务进入延迟窗口：此刻不可领取。
	l1, _ := q.Fetch(ctx, "driver")
	_, e1 := exec.Execute(ctx, l1.Task)
	r1, err := l1.Nack(ctx, e1.(*ExecError))
	if err != nil {
		t.Fatal(err)
	}
	if r1.Status != StatusPending {
		t.Fatalf("expected pending after retryable failure, got %s", r1.Status)
	}
	if !r1.AvailableAt.After(clk.now()) {
		t.Fatal("expected delayed availability, task is immediately re-runnable")
	}
	if blocked, _ := q.Fetch(ctx, "driver"); blocked != nil {
		t.Fatal("task must not be fetched during delay window")
	}
	t.Logf("task=%s attempt=%d retryable -> pending, delayed until %s (basis: AvailableAt in future)",
		r1.ID, r1.Attempt, r1.AvailableAt.Format(time.RFC3339Nano))

	// 推进时钟越过延迟窗口后继续驱动，应在第 3 次尝试成功。
	clk.advance(201 * time.Millisecond)
	rest := drive(t, q, exec, clk, 10)
	if rest.Status != StatusSucceeded || rest.Attempt != 3 || rest.Result != "final" {
		t.Fatalf("expected success on attempt 3, got %+v", rest)
	}
	if exec.Calls(rest.ID) != 3 {
		t.Fatalf("expected 3 actual executions, got %d", exec.Calls(rest.ID))
	}
	t.Logf("task=%s SUCCEEDED on attempt=%d executions=%d (basis: 2 retryable then success)",
		rest.ID, rest.Attempt, exec.Calls(rest.ID))
}

func TestFatalFailureGoesStraightToDead(t *testing.T) {
	ctx := context.Background()
	clk := newFakeClock()
	q := newTestQueue(clk, Config{RetryDelay: time.Second, MaxAttempts: 5})
	exec := NewScriptedExecutor()

	created, _ := q.Enqueue(ctx, EnqueueOptions{IdempotencyKey: "fatal", Payload: "p"})
	exec.OnFatal(created.ID, "validation rejected: bad payload schema")

	lease, _ := q.Fetch(ctx, "driver")
	_, execErr := exec.Execute(ctx, lease.Task)
	done, err := lease.Nack(ctx, execErr.(*ExecError))
	if err != nil {
		t.Fatal(err)
	}
	if done.Status != StatusDead || done.Attempt != 1 {
		t.Fatalf("fatal error must dead-letter on first attempt, got status=%s attempt=%d", done.Status, done.Attempt)
	}

	dead, err := q.ListDead(ctx)
	if err != nil || len(dead) != 1 || dead[0].ID != created.ID {
		t.Fatalf("expected exactly 1 dead task, got %d err=%v", len(dead), err)
	}
	rec := dead[0].Attempts[0]
	if rec.Class != ClassFatal {
		t.Fatalf("expected fatal class recorded, got %q", rec.Class)
	}
	t.Logf("task=%s DEAD on attempt=%d class=%s reason=%q (basis: non-retryable error bypasses retries)",
		dead[0].ID, dead[0].Attempt, rec.Class, rec.Reason)

	// 死信任务不会再被领取，也不会被重复投递激活。
	if l, _ := q.Fetch(ctx, "driver"); l != nil {
		t.Fatal("dead task must never be re-delivered")
	}
	dup, err := q.Enqueue(ctx, EnqueueOptions{IdempotencyKey: "fatal", Payload: "p"})
	if !errors.Is(err, ErrAlreadyExists) || dup.Status != StatusDead {
		t.Fatalf("dead task must stay dead on re-enqueue, got %+v err=%v", dup, err)
	}
}

func TestRetryExhaustionDeadLettersWithReasonChain(t *testing.T) {
	ctx := context.Background()
	clk := newFakeClock()
	q := newTestQueue(clk, Config{RetryDelay: 10 * time.Millisecond, MaxAttempts: 3})
	exec := NewScriptedExecutor()

	created, _ := q.Enqueue(ctx, EnqueueOptions{IdempotencyKey: "exhaust", Payload: "p"})
	exec.OnRetryableForever(created.ID, "downstream unavailable")

	final := drive(t, q, exec, clk, 10)
	if final.Status != StatusDead {
		t.Fatalf("expected dead after max attempts, got %s", final.Status)
	}
	if final.Attempt != 3 {
		t.Fatalf("expected exactly maxAttempts=3 attempts, got %d", final.Attempt)
	}
	if exec.Calls(final.ID) != 3 {
		t.Fatalf("expected 3 executions, got %d", exec.Calls(final.ID))
	}
	if len(final.Attempts) != 3 {
		t.Fatalf("expected 3 attempt records in reason chain, got %d", len(final.Attempts))
	}
	for i, rec := range final.Attempts {
		if rec.Class != ClassRetryable || rec.Attempt != i+1 {
			t.Fatalf("attempt record %d mismatch: %+v", i, rec)
		}
		if rec.Reason == "" {
			t.Fatalf("attempt %d missing failure reason", i+1)
		}
		t.Logf("task=%s chain[%d] attempt=%d class=%s reason=%q",
			final.ID, i, rec.Attempt, rec.Class, rec.Reason)
	}
	t.Logf("task=%s DEAD after attempt=%d/%d (basis: retryable failures exhausted budget)",
		final.ID, final.Attempt, final.MaxAttempts)

	// 终态任务再回报失败必须被拒绝。
	stale := &Lease{Task: final, q: q, workerID: "driver"}
	if _, err := stale.Nack(ctx, &ExecError{Class: ClassRetryable, Reason: "late"}); !errors.Is(err, ErrTerminal) {
		t.Fatalf("nack on dead task expected ErrTerminal, got %v", err)
	}
}

func TestDelayNotExpiredBlocksFetch(t *testing.T) {
	ctx := context.Background()
	clk := newFakeClock()
	q := newTestQueue(clk, Config{RetryDelay: time.Hour, MaxAttempts: 3})
	exec := NewScriptedExecutor()
	created, _ := q.Enqueue(ctx, EnqueueOptions{IdempotencyKey: "delayed", Payload: "p", Delay: time.Hour})
	exec.OnSuccess(created.ID, "ok")

	// 初始延迟投递未到期。
	if l, _ := q.Fetch(ctx, "driver"); l != nil {
		t.Fatal("delayed enqueue must not be fetchable before AvailableAt")
	}
	clk.advance(59 * time.Minute)
	if l, _ := q.Fetch(ctx, "driver"); l != nil {
		t.Fatal("still within delay window")
	}
	clk.advance(2 * time.Minute)
	final := drive(t, q, exec, clk, 5)
	if final.Status != StatusSucceeded {
		t.Fatalf("expected success once delay elapsed, got %s", final.Status)
	}
	t.Logf("task=%s fetched only after delay elapsed -> %s attempt=%d", final.ID, final.Status, final.Attempt)
}
