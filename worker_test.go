package taskqueue

import (
	"context"
	"sync"
	"testing"
	"time"
)

func fastTestConfig(concurrency int) Config {
	return Config{
		Concurrency:      concurrency,
		LeaseDuration:    200 * time.Millisecond,
		MaxAttempts:      3,
		RetryDelay:       5 * time.Millisecond,
		PollInterval:     2 * time.Millisecond,
		RecoveryInterval: 20 * time.Millisecond,
	}
}

// waitFor 轮询直到 cond 成立或超时，用于不依赖固定 sleep 的终态等待。
func waitFor(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(2 * time.Millisecond)
	}
	return cond()
}

func TestWorkerGroupEndToEnd(t *testing.T) {
	ctx := context.Background()
	q := New(fastTestConfig(4))
	defer q.Close()
	exec := NewScriptedExecutor()

	specs := []struct {
		key   string
		setup func(id TaskID)
		want  Status
	}{
		{"e2e-ok", func(id TaskID) { exec.OnSuccess(id, "v1") }, StatusSucceeded},
		{"e2e-retry", func(id TaskID) { exec.OnRetryableThenSuccess(id, 2, "blip", "v2") }, StatusSucceeded},
		{"e2e-fatal", func(id TaskID) { exec.OnFatal(id, "poison message") }, StatusDead},
		{"e2e-exhaust", func(id TaskID) { exec.OnRetryableForever(id, "always down") }, StatusDead},
	}
	idByKey := map[string]TaskID{}
	for _, s := range specs {
		task, err := q.Enqueue(ctx, EnqueueOptions{IdempotencyKey: s.key, Payload: "p"})
		if err != nil {
			t.Fatal(err)
		}
		s.setup(task.ID)
		idByKey[s.key] = task.ID
	}

	g := NewWorkerGroup(q, exec, 4).WithLogger(t.Logf).Start(ctx)
	defer g.Stop()

	ok := waitFor(3*time.Second, func() bool {
		for _, s := range specs {
			got, err := q.Get(ctx, idByKey[s.key])
			if err != nil || (got.Status != StatusSucceeded && got.Status != StatusDead) {
				return false
			}
		}
		return true
	})
	if !ok {
		t.Fatal("tasks did not all reach terminal states in time (would indicate loss or permanent stall)")
	}

	for _, s := range specs {
		got, _ := q.Get(ctx, idByKey[s.key])
		if got.Status != s.want {
			t.Errorf("task key=%s id=%s want status %s got %s", s.key, got.ID, s.want, got.Status)
		}
		t.Logf("task=%s key=%s final=%s attempts=%d executions=%d",
			got.ID, s.key, got.Status, got.Attempt, exec.Calls(got.ID))
		if s.key == "e2e-retry" && got.Attempt != 3 {
			t.Errorf("retry task should take 3 attempts, got %d", got.Attempt)
		}
		if s.key == "e2e-exhaust" && got.Attempt != 3 {
			t.Errorf("exhaust task should take exactly maxAttempts=3, got %d", got.Attempt)
		}
	}
	dead, _ := q.ListDead(ctx)
	if len(dead) != 2 {
		t.Fatalf("expected 2 dead letters (fatal + exhausted), got %d", len(dead))
	}
	t.Logf("basis: 4 tasks -> 2 succeeded, 2 dead; no loss, no stall; processed=%d", g.Processed())
}

func TestWorkerGroupCrashAndRestartContinuesTask(t *testing.T) {
	ctx := context.Background()
	q := New(fastTestConfig(2))
	defer q.Close()

	// 闸门执行器：第一批 worker 拿到任务后会阻塞在执行中，此时直接整组停止，
	// 模拟“执行中进程被 kill”——没有 Ack/Nack，任务遗留在 leased。
	released := make(chan struct{})
	var releaseOnce sync.Once
	exec := &gateExecutor{
		gate: released,
		inner: func() *ScriptedExecutor {
			e := NewScriptedExecutor()
			return e
		}(),
	}

	task, err := q.Enqueue(ctx, EnqueueOptions{IdempotencyKey: "restart-1", Payload: "p"})
	if err != nil {
		t.Fatal(err)
	}
	exec.inner.OnSuccess(task.ID, "after-restart")

	runCtx, cancel := context.WithCancel(ctx)
	g1 := NewWorkerGroup(q, exec, 2).WithLogger(t.Logf).Start(runCtx)

	// 等待任务确实被领取（进入 leased），然后“杀死”整组进程。
	if !waitFor(2*time.Second, func() bool {
		got, _ := q.Get(ctx, task.ID)
		return got.Status == StatusLeased
	}) {
		t.Fatal("task was never leased before simulated crash")
	}
	t.Logf("task=%s leased by old group; killing process group before ack (cancelling context)", task.ID)
	cancel()
	g1.Stop()
	releaseOnce.Do(func() { close(released) })

	got, _ := q.Get(ctx, task.ID)
	t.Logf("after crash: task=%s status=%s attempt=%d owner=%q (not lost, not confirmed)",
		got.ID, got.Status, got.Attempt, got.LeaseOwner)
	if got.Status != StatusLeased {
		t.Fatalf("expected leased remnant right after crash, got %s", got.Status)
	}

	// 模拟“新进程启动”：立即恢复扫描（租约未到期则无效果），稍后租约过期被回收，
	// 新的 worker 组继续推进直至成功。
	if n, _ := q.RecoverNow(ctx); n != 0 {
		t.Fatalf("recovery before lease expiry must be no-op, got %d", n)
	}
	g2 := NewWorkerGroup(q, exec, 2).WithLogger(t.Logf).Start(ctx)
	defer g2.Stop()

	if !waitFor(3*time.Second, func() bool {
		f, _ := q.Get(ctx, task.ID)
		return f.Status == StatusSucceeded
	}) {
		f, _ := q.Get(ctx, task.ID)
		t.Fatalf("task did not succeed after restart; status=%s attempt=%d (stalled)", f.Status, f.Attempt)
	}
	final, _ := q.Get(ctx, task.ID)
	if final.Result != "after-restart" {
		t.Fatalf("confirmed result mismatch: %q", final.Result)
	}
	t.Logf("task=%s SUCCEEDED after restart, result=%q attempts=%d (basis: expired lease requeued, result stable)",
		final.ID, final.Result, final.Attempt)
}

// gateExecutor 在闸门关闭时阻塞执行，用于精确模拟“执行进行中进程被杀”。
type gateExecutor struct {
	gate  <-chan struct{}
	inner *ScriptedExecutor
}

func (e *gateExecutor) Execute(ctx context.Context, t *Task) (string, error) {
	if ctx.Err() != nil {
		// 已被取消的旧进程：随 ctx 退出，永不 Ack。
		<-ctx.Done()
		return "", ctx.Err()
	}
	select {
	case <-e.gate:
	case <-ctx.Done():
		return "", ctx.Err()
	}
	return e.inner.Execute(ctx, t)
}

func TestConcurrentStressNoLossNoDuplicate(t *testing.T) {
	ctx := context.Background()
	q := New(fastTestConfig(8))
	defer q.Close()
	exec := NewScriptedExecutor()

	const total = 100
	want := map[TaskID]Status{}
	var mu sync.Mutex
	for i := 0; i < total; i++ {
		key := "stress-" + string(rune('a'+i/26)) + string(rune('a'+i%26)) + "-" + itoa(i)
		task, err := q.Enqueue(ctx, EnqueueOptions{IdempotencyKey: key, Payload: "p"})
		if err != nil {
			t.Fatal(err)
		}
		mu.Lock()
		switch i % 4 {
		case 0:
			exec.OnSuccess(task.ID, "ok")
			want[task.ID] = StatusSucceeded
		case 1:
			exec.OnRetryableThenSuccess(task.ID, 1, "blip", "ok")
			want[task.ID] = StatusSucceeded
		case 2:
			exec.OnFatal(task.ID, "poison")
			want[task.ID] = StatusDead
		default:
			exec.OnRetryableForever(task.ID, "down")
			want[task.ID] = StatusDead
		}
		mu.Unlock()
	}

	g := NewWorkerGroup(q, exec, 8).WithLogger(t.Logf).Start(ctx)
	defer g.Stop()

	ok := waitFor(5*time.Second, func() bool {
		for id := range want {
			got, err := q.Get(ctx, id)
			if err != nil || (got.Status != StatusSucceeded && got.Status != StatusDead) {
				return false
			}
		}
		return true
	})
	if !ok {
		pending, leased := 0, 0
		for id := range want {
			got, _ := q.Get(ctx, id)
			switch got.Status {
			case StatusPending:
				pending++
			case StatusLeased:
				leased++
			}
		}
		t.Fatalf("not all tasks settled: pending=%d leased=%d (loss/stall suspected)", pending, leased)
	}

	succeeded, dead, overExecuted := 0, 0, 0
	for id, expected := range want {
		got, _ := q.Get(ctx, id)
		if got.Status != expected {
			t.Errorf("task=%s want %s got %s", id, expected, got.Status)
		}
		// 关键断言：任何任务的真实执行次数都不能超过“最大尝试次数”——
		// 即同一有效执行绝不可能被两个消费者并发进行。
		if calls := exec.Calls(id); calls > got.MaxAttempts {
			overExecuted++
			t.Errorf("task=%s executed %d times > maxAttempts %d (duplicate effective execution)",
				id, calls, got.MaxAttempts)
		}
		if got.Status == StatusSucceeded {
			succeeded++
		} else {
			dead++
		}
	}
	if succeeded != total/2 {
		t.Errorf("expected %d succeeded, got %d", total/2, succeeded)
	}
	t.Logf("basis: %d tasks under 8 workers -> succeeded=%d dead=%d over-executed=%d; processed=%d",
		total, succeeded, dead, overExecuted, g.Processed())
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b [20]byte
	pos := len(b)
	for i > 0 {
		pos--
		b[pos] = byte('0' + i%10)
		i /= 10
	}
	return string(b[pos:])
}
