package fstatdb

import (
	"os"
	"testing"

	"github.com/msaf1980/log-exporter/pkg/fsutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDb(t *testing.T) {
	db := New()
	f, err := os.CreateTemp("", "fstatdb")
	path := f.Name()
	require.NoError(t, err)
	defer os.Remove(path)

	err = db.Open(path)
	assert.NoError(t, err)
	require.Equal(t, 0, len(db.v))

	files := map[string]fsutil.Fsnode{
		"/var/log/messages": {Inode: 1024, Size: 4096},
		"/var/log/yum.log":  {Inode: 2001, Size: 1},
	}

	for path, fsnode := range files {
		db.Set(path, fsnode)
	}
	assert.Equal(t, files, db.v)

	err = db.Save()
	assert.NoError(t, err)

	fsnode, exist := db.Get("/var/log/yum.log")
	if exist {
		assert.Equal(t, fsutil.Fsnode{Inode: 2001, Size: 1}, fsnode)
	} else {
		assert.True(t, exist)
	}
	if exist = db.IsExist("/var/log/messages"); !exist {
		assert.True(t, exist)
	}

	if _, exist = db.Get("none"); exist {
		assert.False(t, exist)
	}
	if exist = db.IsExist("none"); exist {
		assert.False(t, exist)
	}

	err = db.Close()
	require.NoError(t, err)
	require.Equal(t, 0, len(db.v))

	err = db.Open(path)
	assert.NoError(t, err)
	if err == nil {
		assert.Equal(t, files, db.v)
	}
	db.Close()
}
