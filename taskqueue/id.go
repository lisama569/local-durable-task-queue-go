package taskqueue

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"
)

// newID returns a time-prefixed, random-suffixed task identifier:
// sortable by creation time and effectively collision-free locally.
func newID(now time.Time) (string, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("taskqueue: generate id: %w", err)
	}
	return fmt.Sprintf("t_%d_%s", now.UnixNano(), hex.EncodeToString(b[:])), nil
}
