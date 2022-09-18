package fstatdb

import (
	"context"
	"time"

	"github.com/msaf1980/log-exporter/pkg/fsutil"
	"github.com/rs/zerolog/log"
)

type StatEvent struct {
	Path string
	Stat fsutil.Fsnode
}

func (db *Db) Watch(ctx context.Context, typ string, statChan <-chan StatEvent, flush uint64, timeout time.Duration) error {
	var (
		i       uint64
		err     error
		changed bool
	)
	path := db.f.Name()
LOOP1:
	for {
		select {
		case <-ctx.Done():
			break LOOP1
		case stat, opened := <-statChan:
			if !opened {
				break LOOP1
			}
			changed = true
			db.Set(stat.Path, stat.Stat)
			if i%flush == 0 {
				if err = db.Save(); err == nil {
					changed = false
				} else {
					log.Error().Str("input", typ).Str("seek", path).Err(err).Msg("save stat")
				}
			}
		}
	}

	ticker := time.NewTicker(timeout)
LOOP2:
	for {
		select {
		case <-ticker.C:
			break LOOP2
		case stat, opened := <-statChan:
			if !opened {
				break LOOP2
			}
			i++
			changed = true
			db.Set(stat.Path, stat.Stat)
			if i%flush == 0 {
				if err = db.Save(); err == nil {
					changed = false
				} else {
					log.Error().Str("input", typ).Str("seek", path).Err(err).Msg("save stat failed")
				}
			}
		}
	}

	if changed {
		if err = db.Save(); err != nil {
			log.Error().Str("input", typ).Str("seek", path).Err(err).Msg("save stat failed on shutdown")
		}
	}
	db.Close()
	return err
}
