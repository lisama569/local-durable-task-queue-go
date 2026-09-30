package taskqueue

import (
	"context"
	"fmt"
	"sync"
)

// Executor 是本地可控的下游执行器抽象。
type Executor interface {
	// Execute 执行任务负载；返回 nil error 表示成功，
	// 返回 *ExecError 时其 Class 决定重试或直接进入死信。
	Execute(ctx context.Context, t *Task) (string, error)
}

// Script 依据“当前是第几次尝试”给出本次执行结果。
// 返回 err 为 nil 表示成功；否则应返回 *ExecError 以区分错误类别。
type Script func(attempt int) (result string, err error)

// ScriptedExecutor 用脚本表模拟下游：按任务 ID 预设每次尝试的结果，
// 用于在测试中确定性地制造成功、可重试失败、不可重试失败与“执行中崩溃”。
// “执行中崩溃”不需要执行器配合：测试中直接不 Ack/Nack 并推进租约过期即可。
type ScriptedExecutor struct {
	mu      sync.Mutex
	scripts map[TaskID]Script
	calls   map[TaskID]int
}

// NewScriptedExecutor 创建脚本执行器；未注册脚本的任务默认成功。
func NewScriptedExecutor() *ScriptedExecutor {
	return &ScriptedExecutor{
		scripts: make(map[TaskID]Script),
		calls:   make(map[TaskID]int),
	}
}

// OnScript 为任务注册自定义脚本。
func (e *ScriptedExecutor) OnScript(id TaskID, s Script) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.scripts[id] = s
}

// OnSuccess 设定任务每次执行都成功并返回给定结果。
func (e *ScriptedExecutor) OnSuccess(id TaskID, result string) {
	e.OnScript(id, func(attempt int) (string, error) {
		return result, nil
	})
}

// OnFatal 设定任务第一次执行即不可重试失败。
func (e *ScriptedExecutor) OnFatal(id TaskID, reason string) {
	e.OnScript(id, func(attempt int) (string, error) {
		return "", &ExecError{Class: ClassFatal, Reason: reason}
	})
}

// OnRetryableThenSuccess 设定前 n 次可重试失败、之后成功。
func (e *ScriptedExecutor) OnRetryableThenSuccess(id TaskID, n int, reason string, successResult string) {
	e.OnScript(id, func(attempt int) (string, error) {
		if attempt <= n {
			return "", &ExecError{
				Class:  ClassRetryable,
				Reason: fmt.Sprintf("%s (transient failure %d/%d)", reason, attempt, n),
			}
		}
		return successResult, nil
	})
}

// OnRetryableForever 设定任务始终可重试失败，直到尝试次数耗尽进入死信。
func (e *ScriptedExecutor) OnRetryableForever(id TaskID, reason string) {
	e.OnScript(id, func(attempt int) (string, error) {
		return "", &ExecError{
			Class:  ClassRetryable,
			Reason: fmt.Sprintf("%s (transient failure %d)", reason, attempt),
		}
	})
}

// Calls 返回某任务被实际执行的次数（测试判定依据之一）。
func (e *ScriptedExecutor) Calls(id TaskID) int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.calls[id]
}

// Execute 按脚本与当前尝试次数返回预设结果，并记录实际执行次数。
func (e *ScriptedExecutor) Execute(ctx context.Context, t *Task) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	e.mu.Lock()
	script := e.scripts[t.ID]
	e.calls[t.ID]++
	e.mu.Unlock()

	if script == nil {
		return "ok:no-script", nil
	}
	return script(t.Attempt)
}
