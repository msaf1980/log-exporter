package flock

import "errors"

// ErrTimeout indicates that the lock attempt timed out.
var ErrTimeout error = errors.New("lock timeout exceeded")

// ErrLocked indicates TryLock failed because the lock was already locked.
var ErrLocked error = errors.New("flock is already locked")
