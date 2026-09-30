package taskqueue

import "errors"

// ErrorClass 区分执行失败的类别，决定后续是重试还是进入死信。
type ErrorClass string

const (
	// ClassRetryable 可重试错误：未超过最大尝试次数时会被重新投递。
	ClassRetryable ErrorClass = "retryable"
	// ClassFatal 不可重试错误：立即进入死信。
	ClassFatal ErrorClass = "fatal"
)

// ExecError 是下游执行器回报失败时携带的结构化错误。
type ExecError struct {
	Class  ErrorClass
	Reason string
}

func (e *ExecError) Error() string {
	return string(e.Class) + ": " + e.Reason
}

// 以下哨兵错误让调用方可以用 errors.Is 精确判定队列操作结果。
var (
	// ErrNotFound 任务不存在。
	ErrNotFound = errors.New("taskqueue: task not found")
	// ErrAlreadyExists 幂等键重复投递且任务仍在生命周期内。
	ErrAlreadyExists = errors.New("taskqueue: task already exists")
	// ErrLeaseNotHeld 确认/失败回报时租约不属于该消费者或已过期。
	ErrLeaseNotHeld = errors.New("taskqueue: lease not held or expired")
	// ErrTerminal 终态任务不允许再次确认、失败回报或激活。
	ErrTerminal = errors.New("taskqueue: task already in terminal state")
	// ErrInvalidArgument 参数非法。
	ErrInvalidArgument = errors.New("taskqueue: invalid argument")
)
