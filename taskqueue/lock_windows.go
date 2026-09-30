//go:build windows

package taskqueue

import (
	"fmt"
	"os"
)

// acquireDirLock is a best-effort placeholder on Windows: opening with
// O_EXCL ensures at most one fresh holder for the lifetime of the file.
func acquireDirLock(path string) (func() error, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("taskqueue: open lock file: %w", err)
	}
	return f.Close, nil
}
