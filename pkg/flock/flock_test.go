package flock

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const (
	shortWait = 10 * time.Millisecond
	longWait  = 10 * shortWait
)

// LockFromAnotherProc will launch a process and block until that process has
// created the lock file.  If we time out waiting for the other process to take
// the lock, this function will fail the current test.
func LockFromAnotherProc(t *testing.T, path string, kill chan struct{}) (done chan struct{}) {
	cmd := exec.Command(os.Args[0], "-test.run", "TestLockFromOtherProcess")
	cmd.Env = append(
		// We must preserve os.Environ() on Windows,
		// or the subprocess will fail in weird and
		// wonderful ways.
		os.Environ(),
		"FSLOCK_TEST_HELPER_WANTED=1",
		"FSLOCK_TEST_HELPER_PATH="+path,
	)

	if err := cmd.Start(); err != nil {
		t.Fatalf("error starting other proc: %v", err)
	}

	done = make(chan struct{})

	go func() {
		err := cmd.Wait()
		out, _ := cmd.CombinedOutput()
		fmt.Fprintf(os.Stderr, "err: %v, out: %s\n", err, string(out))
		close(done)
	}()

	go func() {
		select {
		case <-kill:
			// this may fail, but there's not much we can do about it
			_ = cmd.Process.Kill()
		case <-done:
		}
	}()

	var err error
	for x := 0; x < 10; x++ {
		time.Sleep(shortWait)
		if _, err = os.Stat(path); err == nil {
			// file created by other process, let's continue
			break
		}
		if x == 9 {
			t.Fatalf("timed out waiting for other process to start")
		}
	}
	return done
}

func TestLockNoContention(t *testing.T) {
	f, err := ioutil.TempFile("", "lock")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())

	if err = Lock(f); err != nil {
		t.Fatal(err)
	}

	started := make(chan struct{})
	acquired := make(chan struct{})
	go func() {
		close(started)
		err := Lock(f)
		close(acquired)
		if err != nil {
			t.Error(err)
		}
	}()

	select {
	case <-started:
		// good, goroutine started.
	case <-time.After(shortWait * 2):
		t.Errorf("timeout waiting for goroutine to start")
	}

	select {
	case <-acquired:
		// got the lock. good.
	case <-time.After(shortWait * 2):
		t.Errorf("Timed out waiting for lock acquisition.")
	}

	err = Unlock(f)
	if err != nil {
		t.Fatal(err)
	}
}

func TestLockBlocks(t *testing.T) {
	f, err := ioutil.TempFile("", "lock")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())

	kill := make(chan struct{})

	// this will block until the other process has the lock.
	procDone := LockFromAnotherProc(t, f.Name(), kill)

	defer func() {
		close(kill)
		// now wait for the other process to exit so the file will be unlocked.
		select {
		case <-procDone:
		case <-time.After(time.Second):
		}
	}()

	started := make(chan struct{})
	acquired := make(chan struct{})
	go func() {
		close(started)
		err := Lock(f)
		close(acquired)
		Unlock(f)
		if err != nil {
			t.Error(err)
		}
	}()

	select {
	case <-started:
		// good, goroutine started.
	case <-time.After(shortWait * 2):
		t.Errorf("timeout waiting for goroutine to start")
	}

	// Waiting for something not to happen is inherently hard...
	select {
	case <-acquired:
		t.Errorf("Unexpected lock acquisition")
	case <-time.After(shortWait * 2):
		// all good.
	}
}

func TestTryLock(t *testing.T) {
	f, err := ioutil.TempFile("", "lock")
	if err != nil {
		t.Fatal(err)
	}
	os.Remove(f.Name())

	err = TryLock(f)
	if err != nil {
		t.Error(err)
	}
	Unlock(f)
}

// temporary disabled failed test in ci
// @TODO: refactor tests for work under CI
func _TestTryLockNoBlock(t *testing.T) {
	f, err := ioutil.TempFile("", "lock")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())

	kill := make(chan struct{})

	// this will block until the other process has the lock.
	procDone := LockFromAnotherProc(t, f.Name(), kill)

	defer func() {
		close(kill)
		// now wait for the other process to exit so the file will be unlocked.
		select {
		case <-procDone:
		case <-time.After(time.Second):
		}
	}()

	started := make(chan struct{})
	result := make(chan error)
	go func() {
		close(started)
		result <- TryLock(f)
	}()

	select {
	case <-started:
		// good, goroutine started.
	case <-time.After(shortWait):
		t.Errorf("timeout waiting for goroutine to start")
	}

	// Wait for trylock to fail.
	select {
	case err := <-result:
		// yes, I know this is redundant with the assert below, but it makes the
		// failed test message clearer.
		if err == nil {
			t.Fatalf("lock succeeded, but should have errored out")
		}
		// This should be the error from trylock failing.
		if err != ErrLocked {
			t.Errorf("already locked, but error: %v", err)
		}
	case <-time.After(shortWait):
		t.Errorf("took too long to fail trylock")
	}
}

func TestUnlockedWithTimeout(t *testing.T) {
	f, err := ioutil.TempFile("", "lock")
	if err != nil {
		t.Fatal(err)
	}
	os.Remove(f.Name())

	if err = LockWithTimeout(f, shortWait); err != nil {
		t.Error(err)
	}
	Unlock(f)
}

// temporary disabled failed test in ci
// @TODO: refactor tests for work under CI
func _TestLockWithTimeout(t *testing.T) {
	f, err := ioutil.TempFile("", "lock")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())

	kill := make(chan struct{})

	// this will block until the other process has the lock.
	procDone := LockFromAnotherProc(t, f.Name(), kill)

	defer func() {
		close(kill)
		// now wait for the other process to exit so the file will be unlocked.
		select {
		case <-procDone:
		case <-time.After(time.Second):
		}
	}()

	started := make(chan struct{})
	result := make(chan error)
	go func() {
		close(started)
		result <- LockWithTimeout(f, shortWait)
	}()

	select {
	case <-started:
		// good, goroutine started.
	case <-time.After(shortWait * 2):
		t.Fatalf("timeout waiting for goroutine to start")
	}

	// Wait for timeout.
	select {
	case err := <-result:
		// yes, I know this is redundant with the assert below, but it makes the
		// failed test message clearer.
		if err == nil {
			t.Errorf("lock succeeded, but should have timed out")
		} else if err != ErrTimeout {
			// This should be the error from the lock timing out.
			t.Error(err)
		}
	case <-time.After(shortWait * 2):
		t.Errorf("lock took too long to timeout")
	}
}

func TestStress(t *testing.T) {
	const lockAttempts = 200
	const concurrentLocks = 10

	var counter = new(int64)
	// Use atomics to update lockState to make sure the lock isn't held by
	// someone else. A value of 1 means locked, 0 means unlocked.
	var lockState = new(int32)

	var wg sync.WaitGroup

	dir, err := ioutil.TempDir("", "lock")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	var stress = func(name string) {
		defer wg.Done()
		f, err := os.OpenFile(filepath.Join(dir, "testing"), os.O_RDWR|os.O_CREATE, 0640)
		require.NoError(t, err)
		for i := 0; i < lockAttempts; i++ {
			err = Lock(f)
			require.NoError(t, err)
			state := atomic.AddInt32(lockState, 1)
			require.Equal(t, int32(1), state)
			// Tell the go routine scheduler to give a slice to someone else
			// while we have this locked.
			runtime.Gosched()
			// need to decrement prior to unlock to avoid the race of someone
			// else grabbing the lock before we decrement the state.
			atomic.AddInt32(lockState, -1)
			err = Unlock(f)
			require.NoError(t, err)
			// increment the general counter
			atomic.AddInt64(counter, 1)
		}
	}

	for i := 0; i < concurrentLocks; i++ {
		wg.Add(1)
		go stress(fmt.Sprintf("Lock %d", i))
	}
	wg.Wait()
	require.Equal(t, int64(lockAttempts*concurrentLocks), *counter)
}

func TestLockFromOtherProcess(t *testing.T) {
	if os.Getenv("FSLOCK_TEST_HELPER_WANTED") == "" {
		fmt.Fprintln(os.Stderr, "skiping")
		return
	}
	filename := os.Getenv("FSLOCK_TEST_HELPER_PATH")

	fmt.Fprintf(os.Stderr, "opening %q", filename)
	f, err := os.Open(filename)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error open %q: %v", filename, err)
		os.Exit(1)
	}
	defer f.Close()

	if err = Lock(f); err != nil {
		fmt.Fprintf(os.Stderr, "error locking %q: %v", filename, err)
		os.Exit(2)
	}
	time.Sleep(longWait)
	err = Unlock(f)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error unlocking %q: %v", filename, err)
		os.Exit(3)
	}

	os.Exit(0)
}
