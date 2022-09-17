//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd
// +build darwin dragonfly freebsd linux netbsd openbsd

package flock

import (
	"os"
	"syscall"
	"time"
)

// Lock get file lock.  This call will block until the lock is available.
func Lock(f *os.File) error {
	return syscall.Flock(int(f.Fd()), syscall.LOCK_EX)
}

// TryLock attempts to lock.  This method will return ErrLocked
// immediately if the lock cannot be acquired.
func TryLock(f *os.File) error {
	err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if err == syscall.EWOULDBLOCK {
		return ErrLocked
	}
	return err
}

// Unlock relese the lock. Also lock is released, when file is closed
func Unlock(f *os.File) error {
	return syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
}

// LockWithTimeout tries to lock until the timeout expires.  If the
// timeout expires, this method will return ErrTimeout.
func LockWithTimeout(f *os.File, timeout time.Duration) error {
	result := make(chan error)
	cancel := make(chan struct{})
	go func() {
		err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX)
		select {
		case <-cancel:
			// Timed out, cleanup if necessary.
			Unlock(f)
		case result <- err:
		}
	}()
	select {
	case err := <-result:
		return err
	case <-time.After(timeout):
		close(cancel)
		return ErrTimeout
	}
}
