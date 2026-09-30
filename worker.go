package taskqueue

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// WorkerGroup 管理若干并发消费者，驱动 Fetch -> Execute -> Ack/Nack 循环，
// 并周期性回收过期租约，以模拟崩溃恢复语义。
type WorkerGroup struct {
	q    *Queue
	exec Executor
	n    int
	logf func(format string, args ...any)

	wg       sync.WaitGroup
	cancel   context.CancelFunc
	stopOnce sync.Once

	processed atomic.Int64
}

// NewWorkerGroup 创建尚未启动的消费者组，可在 Start 前链式配置（如 WithLogger），
// 避免在 goroutine 已运行后再改写字段造成数据竞争。n 小于 1 时取队列配置的 Concurrency。
func NewWorkerGroup(q *Queue, exec Executor, n int) *WorkerGroup {
	if n < 1 {
		n = q.cfg.Concurrency
	}
	return &WorkerGroup{
		q:    q,
		exec: exec,
		n:    n,
		logf: log.Printf,
	}
}

// StartWorkers 在队列上启动 n 个并发消费者和一个过期租约回收循环。
func StartWorkers(parent context.Context, q *Queue, exec Executor, n int) *WorkerGroup {
	return NewWorkerGroup(q, exec, n).Start(parent)
}

// Start 启动消费者与回收循环。
func (g *WorkerGroup) Start(parent context.Context) *WorkerGroup {
	ctx, cancel := context.WithCancel(parent)
	g.cancel = cancel

	for i := 0; i < g.n; i++ {
		workerID := fmt.Sprintf("worker-%d", i+1)
		g.wg.Add(1)
		go g.consume(ctx, workerID)
	}

	g.wg.Add(1)
	go g.recoverLoop(ctx)

	return g
}

// WithLogger 注入日志函数（如测试中的 t.Logf），必须在 Start 之前调用。
func (g *WorkerGroup) WithLogger(f func(format string, args ...any)) *WorkerGroup {
	if f != nil {
		g.logf = f
	}
	return g
}

// Processed 返回已完成 Ack/Nack 终局处理的次数。
func (g *WorkerGroup) Processed() int64 {
	return g.processed.Load()
}

func (g *WorkerGroup) consume(ctx context.Context, workerID string) {
	defer g.wg.Done()
	for {
		if ctx.Err() != nil {
			return
		}
		lease, err := g.q.Fetch(ctx, workerID)
		if err != nil {
			g.logf("[%s] fetch error: %v", workerID, err)
			if !sleep(ctx, g.q.cfg.PollInterval) {
				return
			}
			continue
		}
		if lease == nil {
			if !sleep(ctx, g.q.cfg.PollInterval) {
				return
			}
			continue
		}

		t := lease.Task
		result, execErr := g.exec.Execute(ctx, t)
		// 上下文已取消：不做确认，直接退出。任务保留在 leased，
		// 待租约过期后由 recoverLoop（或重启后的 RecoverNow）回收，精确模拟“执行中进程被杀”。
		if ctx.Err() != nil {
			g.logf("[%s] task=%s attempt=%d interrupted (context cancelled); lease left to expire",
				workerID, t.ID, t.Attempt)
			return
		}

		if execErr == nil {
			if _, ackErr := lease.Ack(ctx, result); ackErr != nil {
				g.logf("[%s] task=%s attempt=%d ack rejected: %v", workerID, t.ID, t.Attempt, ackErr)
				continue
			}
			g.processed.Add(1)
			g.logf("[%s] task=%s attempt=%d ACK result=%q", workerID, t.ID, t.Attempt, result)
			continue
		}

		ee, ok := execErr.(*ExecError)
		if !ok {
			ee = &ExecError{Class: ClassRetryable, Reason: execErr.Error()}
		}
		after, nackErr := lease.Nack(ctx, ee)
		if nackErr != nil {
			g.logf("[%s] task=%s attempt=%d nack rejected: %v", workerID, t.ID, t.Attempt, nackErr)
			continue
		}
		g.processed.Add(1)
		switch after.Status {
		case StatusDead:
			g.logf("[%s] task=%s attempt=%d NACK class=%s -> DEAD reason=%q",
				workerID, t.ID, t.Attempt, ee.Class, ee.Reason)
		default:
			g.logf("[%s] task=%s attempt=%d NACK class=%s -> retry (available in %s) reason=%q",
				workerID, t.ID, t.Attempt, ee.Class, g.q.cfg.RetryDelay, ee.Reason)
		}
	}
}

func (g *WorkerGroup) recoverLoop(ctx context.Context) {
	defer g.wg.Done()
	ticker := time.NewTicker(g.q.cfg.RecoveryInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n, err := g.q.RecoverNow(ctx)
			if err != nil && ctx.Err() == nil {
				g.logf("recovery error: %v", err)
			}
			if n > 0 {
				g.logf("recovery: requeued %d expired lease(s) after crash/timeout", n)
			}
		}
	}
}

// Wait 阻塞直到所有消费者退出。
func (g *WorkerGroup) Wait() {
	g.wg.Wait()
}

// Stop 通知消费者停止并等待退出。
func (g *WorkerGroup) Stop() {
	g.stopOnce.Do(func() { g.cancel() })
	g.wg.Wait()
}

func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
