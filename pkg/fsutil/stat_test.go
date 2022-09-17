package fsutil

import (
	"errors"
	"os"
	"syscall"
	"testing"
)

func TestStat(t *testing.T) {
	var (
		fnode, fnode2 Fsnode
		err           error
	)
	if err = LStat("/NONexistent/NaOn", &fnode); !errors.Is(err, syscall.ENOENT) {
		t.Errorf("LStat() error = %v, want ENOENT", err)
	}

	fp, err := os.CreateTemp("", "log-exporter")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		fp.Close()
		os.Remove(fp.Name())
	}()

	if err = LStat(fp.Name(), &fnode); err != nil {
		t.Errorf("LStat() error = %v", err)
	}
	if err = FStat(fp, &fnode2); err != nil {
		t.Errorf("FStat() error = %v", err)
	}
	if fnode != fnode2 {
		t.Errorf("results mismatch, LStat() = %#v, FStat() = %#v", fnode, fnode2)
	}

	fstat, err := os.Stat(fp.Name())
	if err != nil {
		t.Errorf("os.Stat() error = %v", err)
	}
	Stat(fstat, &fnode2)
	if fnode != fnode2 {
		t.Errorf("results mismatch, LStat() = %#v, Stat() = %#v", fnode, fnode2)
	}

	if !Same(&fnode, &fnode2) {
		t.Errorf("Same(%#v, %#v) = false", fnode, fnode2)
	}
	if Other(&fnode, &fnode2) {
		t.Errorf("Other(%#v, %#v) = true", fnode, fnode2)
	}
}
