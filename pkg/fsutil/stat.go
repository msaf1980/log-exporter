package fsutil

import (
	"os"
	"syscall"
)

type Fsnode struct {
	Dev   uint64
	Inode uint64
	Size  int64
	Nlink uint64
}

func Same(a, b *Fsnode) bool {
	return a.Dev == b.Dev && a.Inode == b.Inode
}

func Other(a, b *Fsnode) bool {
	return a.Dev != b.Dev || a.Inode != b.Inode
}

func copyStat(stat *syscall.Stat_t, fnode *Fsnode) {
	fnode.Dev = stat.Dev
	fnode.Inode = stat.Ino
	fnode.Size = stat.Size
	fnode.Nlink = stat.Nlink
}

func LStat(path string, fnode *Fsnode) error {
	var stat syscall.Stat_t
	if err := syscall.Lstat(path, &stat); err == nil {
		copyStat(&stat, fnode)
		return nil
	} else {
		return err
	}
}

func FStat(f *os.File, fnode *Fsnode) error {
	var stat syscall.Stat_t
	if err := syscall.Fstat(int(f.Fd()), &stat); err == nil {
		copyStat(&stat, fnode)
		return nil
	} else {
		return err
	}
}

func Stat(fi os.FileInfo, fnode *Fsnode) {
	stat := fi.Sys().(*syscall.Stat_t)
	copyStat(stat, fnode)
}

func FSize(f *os.File) (int64, error) {
	var stat syscall.Stat_t
	if err := syscall.Fstat(int(f.Fd()), &stat); err == nil {
		return stat.Size, nil
	} else {
		return -1, err
	}
}

func FSizeN(f *os.File) int64 {
	var stat syscall.Stat_t
	if err := syscall.Fstat(int(f.Fd()), &stat); err == nil {
		return stat.Size
	} else {
		return -1
	}
}

func LSize(path string) (int64, error) {
	var stat syscall.Stat_t
	if err := syscall.Lstat(path, &stat); err == nil {
		return stat.Size, nil
	} else {
		return -1, err
	}
}

func LSizeN(path string) int64 {
	var stat syscall.Stat_t
	if err := syscall.Lstat(path, &stat); err == nil {
		return stat.Size
	} else {
		return -1
	}
}
