package taskqueue

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRecoverLeasedTaskAfterCrashBeforeAck(t *testing.T) {
	ctx := context.Background()
	clk := newFakeClock()
	q := newTestQueue(clk, Config{LeaseDuration: 30 * time.Second, MaxAttempts: 3})
	exec := NewScriptedExecutor()

	created, _ := q.Enqueue(ctx, EnqueueOptions{IdempotencyKey: "crash-1", Payload: "p"})
	exec.OnRetryableThenSuccess(created.ID, 1, "flaky before crash", "done")

	// 消费者领取任务并开始执行（attempt=1），但在 Ack 前“进程被杀”。
	lease, _ := q.Fetch(ctx, "worker-A")
	if lease == nil || lease.Task.Status != StatusLeased || lease.Task.Attempt != 1 {
		t.Fatalf("expected leased attempt 1, got %+v", lease)
	}
	exec.Execute(ctx, lease.Task) // 下游确实执行了，但确认随进程一起丢失
	t.Logf("task=%s attempt=%d leased by worker-A; execution happened but process crashed before ack (no ack/nack issued)",
		created.ID, lease.Task.Attempt)

	// 租约尚未过期：恢复扫描不应回收，也不能被他人领取。
	if n, _ := q.RecoverNow(ctx); n != 0 {
		t.Fatalf("no recovery expected before lease expiry, recovered=%d", n)
	}
	if l, _ := q.Fetch(ctx, "worker-B"); l != nil {
		t.Fatal("valid unexpired lease must block other consumers")
	}

	// 时间越过租约期限；重启后第一时间执行恢复扫描。
	clk.advance(31 * time.Second)
	n, err := q.RecoverNow(ctx)
	if err != nil || n != 1 {
		t.Fatalf("expected 1 recovered task, got n=%d err=%v", n, err)
	}
	recovered, _ := q.Get(ctx, created.ID)
	if recovered.Status != StatusPending {
		t.Fatalf("expected pending after recovery, got %s", recovered.Status)
	}
	if recovered.LeaseOwner != "" {
		t.Fatal("recovered task must clear stale lease owner")
	}
	t.Logf("after restart recovery: task=%s status=%s attempts_so_far=%d (basis: lease expired)",
		recovered.ID, recovered.Status, recovered.Attempt)

	// 被中断尝试的原因链必须保留，可解释“执行中崩溃/超时”。
	last := recovered.Attempts[len(recovered.Attempts)-1]
	if last.Class != ClassRetryable || last.Reason == "" {
		t.Fatalf("expected retryable crash reason recorded, got %+v", last)
	}
	t.Logf("task=%s interrupted attempt=%d recorded class=%s reason=%q",
		recovered.ID, last.Attempt, last.Class, last.Reason)

	// 任务继续推进：新的消费者可领取，按脚本再失败一次后成功，结果稳定。
	final := drive(t, q, exec, clk, 10)
	if final.Status != StatusSucceeded || final.Result != "done" {
		t.Fatalf("expected eventual success after recovery, got %+v", final)
	}
	if final.Attempt != 2 {
		t.Fatalf("expected success on attempt 2 after one crash, got attempt=%d", final.Attempt)
	}
	if exec.Calls(final.ID) != 2 {
		t.Fatalf("expected 2 real executions (1 crashed + 1 ok), got %d", exec.Calls(final.ID))
	}
	t.Logf("task=%s SUCCEEDED on attempt=%d executions=%d; no loss, no permanent stall",
		final.ID, final.Attempt, exec.Calls(final.ID))

	// 再执行恢复也不能影响终态任务。
	clk.advance(time.Hour)
	if n, _ := q.RecoverNow(ctx); n != 0 {
		t.Fatalf("terminal task must never be reactivated, recovered=%d", n)
	}
	again, _ := q.Get(ctx, created.ID)
	if again.Status != StatusSucceeded {
		t.Fatalf("confirmed result must stay stable, got %s", again.Status)
	}
}

func TestRecoveryDoesNotTouchTerminalTasks(t *testing.T) {
	ctx := context.Background()
	clk := newFakeClock()
	q := newTestQueue(clk, Config{LeaseDuration: time.Second, MaxAttempts: 1})

	ok, _ := q.Enqueue(ctx, EnqueueOptions{IdempotencyKey: "kept-ok", Payload: "p"})
	l, _ := q.Fetch(ctx, "w")
	if _, err := l.Ack(ctx, "stable-result"); err != nil {
		t.Fatal(err)
	}

	dead, _ := q.Enqueue(ctx, EnqueueOptions{IdempotencyKey: "kept-dead", Payload: "p"})
	l2, _ := q.Fetch(ctx, "w")
	if _, err := l2.Nack(ctx, &ExecError{Class: ClassFatal, Reason: "permanent"}); err != nil {
		t.Fatal(err)
	}

	clk.advance(time.Hour)
	n, err := q.RecoverNow(ctx)
	if err != nil || n != 0 {
		t.Fatalf("recovery must not touch terminals, n=%d err=%v", n, err)
	}
	g1, _ := q.Get(ctx, ok.ID)
	g2, _ := q.Get(ctx, dead.ID)
	if g1.Status != StatusSucceeded || g1.Result != "stable-result" {
		t.Fatalf("succeeded task mutated: %+v", g1)
	}
	if g2.Status != StatusDead {
		t.Fatalf("dead task mutated: %+v", g2)
	}
	t.Logf("basis: after far-future recovery, %s=%s %s=%s; terminal states are immutable",
		g1.ID, g1.Status, g2.ID, g2.Status)

	// 终态任务持假租约做 Ack/Nack 一律被拒绝。
	fakeLease := &Lease{Task: g1, q: q, workerID: "anyone"}
	if _, err := fakeLease.Ack(ctx, "x"); !errors.Is(err, ErrTerminal) {
		t.Fatalf("ack terminal: %v", err)
	}
	fakeLease2 := &Lease{Task: g2, q: q, workerID: "anyone"}
	if _, err := fakeLease2.Nack(ctx, &ExecError{Class: ClassFatal, Reason: "x"}); !errors.Is(err, ErrTerminal) {
		t.Fatalf("nack terminal: %v", err)
	}
}
