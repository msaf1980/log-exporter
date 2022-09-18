package file

import (
	"context"
	"os"
	"path/filepath"
	"time"

	"github.com/msaf1980/log-exporter/pkg/fsutil"
	"github.com/msaf1980/log-exporter/pkg/lreader"
)

func IsNotExist(path string) bool {
	_, err := os.Stat(path)
	return os.IsNotExist(err)
}

func evalSymlinks(ctx context.Context, path string) (string, error) {
	// loop for workaround start during rotate or create
	for retry := 5; retry > 0; retry-- {
		if !IsNotExist(path) {
			break
		}

		select {
		case <-ctx.Done():
			return path, nil
		case <-time.After(500 * time.Millisecond):
		}
	}

	if IsNotExist(path) {
		return path, os.ErrNotExist
	}

	return filepath.EvalSymlinks(path)
}

// openFile open (or reopen file, if truncated or recreated). Return *os.File, truncated, recreated, error
func (in *File) openFile(fp *os.File, reader *lreader.Reader, fpath string, fnode *fsutil.Fsnode) (*os.File, bool, bool, error) {
	var (
		truncated, recreated bool
		err                  error
		fn                   fsutil.Fsnode
		needSeek             bool
	)
	if fp == nil {
		if fp, err = os.Open(fpath); err != nil {
			return nil, truncated, recreated, err
		}
		needSeek = true
	}
	if err = fsutil.FStat(fp, &fn); err != nil {
		fp.Close()
		return nil, truncated, recreated, err
	}
	if fsutil.Other(&fn, fnode) {
		if fnode.Inode == 0 {
			needSeek = true
		} else {
			recreated = true
		}
	} else if fn.Size < fnode.Size {
		truncated = true
	}

	if recreated {
		fp.Close()
		if fp, err = os.Open(fpath); err != nil {
			return nil, truncated, recreated, err
		}
		if err = fsutil.FStat(fp, &fn); err != nil {
			fp.Close()
			return nil, truncated, recreated, err
		}
		*fnode = fn
		fnode.Size = 0 // used as seek offset, reset to beginning
	} else if truncated {
		// TODO: naive detect if file is truncated, may be a better way ?
		*fnode = fn
		fnode.Size = 0 // used as seek offset, reset to beginning
	} else if fn.Nlink != fnode.Nlink {
		fnode.Nlink = fn.Nlink
	}

	// log.Trace().Str("input", in.cfg.Type).Str("file", fpath).Int64("offset", fnode.Size).Int64("size", fn.Size).Msg("check")

	if needSeek || recreated || truncated {
		_, err = fp.Seek(fnode.Size, 0)
		reader.Reset(fp)
	}
	return fp, truncated, recreated, err
}
