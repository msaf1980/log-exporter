package fstatdb

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"os"

	"github.com/msaf1980/log-exporter/pkg/flock"
	"github.com/msaf1980/log-exporter/pkg/fsutil"
)

const (
	SIZE_INT64 = 8
	MAX_SIZE   = 1024
)

var (
	ErrUnexpectedEnd  = errors.New("unexpected end")
	ErrInvalidPathLen = errors.New("empty or long path")
)

// Db store files state in bimary file
// filelen_u64 filname inode_u64 offset_i64
// filelen_u64 filname inode_u64 offset_i64
type Db struct {
	f *os.File
	b bytes.Buffer

	v map[string]fsutil.Fsnode
}

func New() *Db {
	return &Db{
		v: make(map[string]fsutil.Fsnode),
	}
}

func (db *Db) Open(path string) (err error) {
	if db.f, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666); err != nil {
		return err
	}
	if err = flock.TryLock(db.f); err != nil {
		return err
	}

	if err = db.load(); err != nil {
		return err
	}

	return nil
}

func (db *Db) Close() (err error) {
	if len(db.v) > 0 {
		db.v = make(map[string]fsutil.Fsnode)
	}
	return db.f.Close()
}

func read(r io.Reader, b []byte) error {
	if n, err := r.Read(b); err != nil {
		return err
	} else if n != len(b) {
		return ErrUnexpectedEnd
	}
	return nil
}

func (db *Db) load() error {
	var err error
	if len(db.v) > 0 {
		db.v = make(map[string]fsutil.Fsnode)
	}
	r := bufio.NewReader(db.f)
	var buf [MAX_SIZE]byte
	for {
		// path len
		err = read(r, buf[:SIZE_INT64])
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		n := binary.LittleEndian.Uint64(buf[:SIZE_INT64])
		if n < 1 || n > MAX_SIZE {
			return ErrInvalidPathLen
		}
		// path
		err = read(r, buf[:n])
		if err != nil {
			return err
		}
		path := string(buf[:n])

		var fsnode fsutil.Fsnode
		// inode
		err = read(r, buf[:SIZE_INT64])
		if err != nil {
			return err
		}
		fsnode.Inode = binary.LittleEndian.Uint64(buf[:SIZE_INT64])

		// offset
		err = read(r, buf[:SIZE_INT64])
		if err != nil {
			return err
		}
		fsnode.Size = int64(binary.LittleEndian.Uint64(buf[:SIZE_INT64]))

		db.Set(path, fsnode)
	}
}

func (db *Db) Set(path string, fsnode fsutil.Fsnode) {
	db.v[path] = fsnode
}

func (db *Db) Get(path string) (fsutil.Fsnode, bool) {
	fsnode, exist := db.v[path]
	return fsnode, exist
}

func (db *Db) IsExist(path string) bool {
	_, exist := db.v[path]
	return exist
}

func (db *Db) Save() (err error) {
	db.b.Reset()
	var buf [SIZE_INT64]byte
	for path, fsnode := range db.v {
		// path
		binary.LittleEndian.PutUint64(buf[:SIZE_INT64], uint64(len(path)))
		db.b.Write(buf[:SIZE_INT64])
		db.b.WriteString(path)
		// inode
		binary.LittleEndian.PutUint64(buf[:SIZE_INT64], fsnode.Inode)
		db.b.Write(buf[:SIZE_INT64])
		// offset
		binary.LittleEndian.PutUint64(buf[:SIZE_INT64], uint64(fsnode.Size))
		db.b.Write(buf[:SIZE_INT64])
	}
	if err = db.f.Truncate(int64(db.b.Len())); err != nil {
		return
	}
	db.f.Seek(0, 0)
	if _, err = db.f.Write(db.b.Bytes()); err != nil {
		return
	}
	err = db.f.Sync()
	return
}
