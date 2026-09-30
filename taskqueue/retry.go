package taskqueue

import "fmt"

// RetryableError marks an execution failure as transient. Failures wrapped in
// it are retried up to the task's MaxAttempts; any other non-nil error is a
// permanent failure and dead-letters the task immediately.
type RetryableError struct {
	Message string
	Cause   error
}

func (e *RetryableError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("retryable: %s: %v", e.Message, e.Cause)
	}
	return fmt.Sprintf("retryable: %s", e.Message)
}

func (e *RetryableError) Unwrap() error { return e.Cause }

// Retryable wraps err so that a failure becomes retryable.
func Retryable(err error, message string) *RetryableError {
	return &RetryableError{Message: message, Cause: err}
}

// PermanentError marks an explicitly non-retryable failure. It is the
// default for plain errors, but constructing one directly documents intent
// and attaches a cause chain.
type PermanentError struct {
	Message string
	Cause   error
}

func (e *PermanentError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("permanent: %s: %v", e.Message, e.Cause)
	}
	return fmt.Sprintf("permanent: %s", e.Message)
}

func (e *PermanentError) Unwrap() error { return e.Cause }

// Permanent wraps err as an explicitly non-retryable failure.
func Permanent(err error, message string) *PermanentError {
	return &PermanentError{Message: message, Cause: err}
}
