//go:build !windows

package taskqueue

import (
	"fmt"
	"os"
	"syscall"
)

// acquireDirLock takes an exclusive advisory lock on the queue directory.
// It guards against two processes opening the same queue directory at once.
func acquireDirLock(path string) (func() error, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("taskqueue: open lock file: %w", err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("taskqueue: queue directory %q is locked by another process: %w", path, err)
	}
	return func() error {
		unlockErr := syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		closeErr := f.Close()
		if unlockErr != nil {
			return unlockErr
		}
		return closeErr
	}, nil
}
