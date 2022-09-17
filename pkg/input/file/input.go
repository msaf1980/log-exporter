package file

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	jerrors "github.com/juju/errors"
	"github.com/msaf1980/go-stringutils"
	"github.com/msaf1980/log-exporter/pkg/codec"
	"github.com/msaf1980/log-exporter/pkg/config"
	"github.com/msaf1980/log-exporter/pkg/event"
	"github.com/msaf1980/log-exporter/pkg/fstatdb"
	"github.com/msaf1980/log-exporter/pkg/fsutil"
	"github.com/msaf1980/log-exporter/pkg/input"
	"github.com/msaf1980/log-exporter/pkg/lreader"
	"github.com/msaf1980/log-exporter/pkg/timeutil"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"
)

const Name = "file"

var errShutdown = errors.New("shutdown")
var errAlreadyStarted = errors.New("already started")

type Config struct {
	input.Config

	Path  string      `hcl:"path" yaml:"path" json:"path"`    // path glob
	Read  config.Size `hcl:"read" yaml:"read" json:"read"`    // read buffer size
	Codec string      `hcl:"codec" yaml:"codec" json:"codec"` // codec name (deefault - line)
	// Username string        `hcl:"username" yaml:"username"`
	// Pasword  string        `hcl:"usernam" yaml:"username"`
	Interval time.Duration `hcl:"interval" yaml:"interval" json:"interval"`
	StartEnd bool          `hcl:"start_end" yaml:"start_end" json:"start_end"`
	SeekFile string        `hcl:"seek_file" yaml:"seek_file" json:"seek_file"` // if not set, read from end after start
}

func defaultConfig() Config {
	return Config{
		Config:   input.Config{Type: Name},
		Interval: time.Second,
		Read:     config.Size(64 * 1024),
	}
}

type File struct {
	cfg    Config
	cfgRaw *config.ConfigRaw
	common *config.Common

	db     *fstatdb.Db
	mtx    sync.Mutex
	ctx    context.Context
	cancel context.CancelFunc
}

func New(cfg *config.ConfigRaw, common *config.Common) (input.Input, error) {
	in := &File{
		cfg:    defaultConfig(),
		cfgRaw: cfg,
		common: common,
	}

	if err := cfg.Decode(&in.cfg); err != nil {
		return nil, err
	}

	if in.cfg.Path == "" {
		return nil, errors.New("input '" + in.cfg.Type + "': path not set")
	}

	if in.cfg.Interval > 20*time.Second {
		return nil, errors.New("input '" + in.cfg.Type + "': interval must be <= 20s")
	}

	// Check codec config
	_, err := codec.New(in.cfgRaw, in.common, in.cfg.Path)
	if err != nil {
		return nil, jerrors.Annotate(err, "input '"+in.cfg.Type+"' path='"+in.cfg.Path+"'")
	}

	return in, nil
}

func (in *File) Start(ctx context.Context, outChan chan<- *event.Event) error {
	matches, err := filepath.Glob(in.cfg.Path)
	if err != nil {
		return jerrors.Annotate(err, "glob expand failed: "+in.cfg.Path)
	}

	if in.cfg.SeekFile == "" {
		if !in.cfg.StartEnd {
			log.Warn().Str("input", in.cfg.Type).Str("file", in.cfg.Path).Err(err).Msg("seek file not set, force start from end")
			in.cfg.StartEnd = true
		}
	} else {
		in.db = fstatdb.New()
		if err = in.db.Open(in.cfg.SeekFile); err != nil {
			return jerrors.Annotate(err, "open file failed: "+in.cfg.SeekFile)
		}
	}

	fnodes := make([]fsutil.Fsnode, len(matches))
	for i := range matches {
		fpath, err := evalSymlinks(ctx, matches[i])
		if err != nil {
			log.Error().Str("input", in.cfg.Type).Str("file", fpath).Err(err).Msg("eval symlink failed")
			continue
		}

		var fi os.FileInfo
		if fi, err = os.Stat(fpath); err != nil {
			log.Error().Str("input", in.cfg.Type).Str("file", fpath).Err(err).Msg("stat failed")
			continue
		}

		if fi.IsDir() {
			log.Warn().Str("input", in.cfg.Type).Str("file", fpath).Msg("dir skipping")
			continue
		}

		if in.cfg.StartEnd {
			// seek to the end
			fsutil.Stat(fi, &fnodes[i])
			if in.db != nil {
				in.db.Set(fpath, fnodes[i])
			}
		} else if fnode, exist := in.db.Get(fpath); exist {
			fnodes[i] = fnode
		} else {
			in.db.Set(fpath, fnodes[i])
		}

		matches[i] = fpath
	}

	var statChan chan fstatdb.StatEvent

	if in.db != nil {
		if err = in.db.Save(); err != nil {
			return err
		}
		statChan = make(chan fstatdb.StatEvent, 10*len(matches))
	}

	eg, ctx := errgroup.WithContext(ctx)

	if in.db != nil {
		eg.Go(func() error {
			return in.db.Watch(ctx, in.cfg.Type, statChan, uint64(len(matches)), 2*in.cfg.Interval)
		})
	}

	for i, fpath := range matches {
		path := fpath
		n := i
		eg.Go(func() error {
			return in.fileWatchLoop(ctx, path, fnodes[n], statChan, outChan)
		})
	}

	return eg.Wait()
}

func (in *File) fileWatchLoop(ctx context.Context, fpath string, fnode fsutil.Fsnode, statChan chan<- fstatdb.StatEvent, outChan chan<- *event.Event) error {
	var (
		err                  error
		fp                   *os.File
		size                 int64
		truncated, recreated bool
	)

	codec, err := codec.New(in.cfgRaw, in.common, fpath)
	if err != nil {
		log.Error().Str("input", in.cfg.Type).Str("codec", in.cfg.Codec).Str("file", fpath).Err(err).Msg("codec init failed")
		return err
	}

	fpath, err = evalSymlinks(ctx, fpath)
	if err != nil {
		log.Error().Str("input", in.cfg.Type).Str("file", fpath).Err(err).Msg("eval symlink failed")
		return err
	}

	var fi os.FileInfo
	if fi, err = os.Stat(fpath); err != nil {
		log.Error().Str("input", in.cfg.Type).Str("file", fpath).Err(err).Msg("stat failed")
		return err
	}

	if fi.IsDir() {
		log.Warn().Str("input", in.cfg.Type).Str("file", fpath).Msg("dir skipping")
		return err
	}

	bufSize := int(in.cfg.Read.Value())
	reader := lreader.New(fp, bufSize)

	if fp, truncated, recreated, err = in.openFile(fp, reader, fpath, &fnode); err == nil {
		if truncated {
			log.Debug().Str("input", in.cfg.Type).Str("file", fpath).Msg("reopen truncated")
		} else if recreated {
			log.Debug().Str("input", in.cfg.Type).Str("file", fpath).Msg("reopen recreated")
		}
	} else {
		log.Error().Str("input", in.cfg.Type).Str("file", fpath).Err(err).Msg("open failed")
		return err
	}

	defer func() {
		if fp != nil {
			fp.Close()
		}
	}()

	log.Trace().Str("input", in.cfg.Type).Str("file", fpath).Int64("offset", fnode.Size).Int64("size", fsutil.FSizeN(fp)).Msg("file read loop")
	if err = in.fileReadUntilEOF(ctx, reader, codec, fpath, &fnode, statChan, outChan); err != nil && err != io.EOF {
		log.Error().Str("input", in.cfg.Type).Str("file", fpath).Err(err).Msg("read failed")
		fp.Close()
		fp = nil
	}

	log.Trace().Str("input", in.cfg.Type).Str("file", fpath).Err(err).Msg("file watch started")
	t := time.NewTimer(in.cfg.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			log.Trace().Str("input", in.cfg.Type).Str("file", fpath).Int64("offset", fnode.Size).Int64("size", fsutil.FSizeN(fp)).Err(err).Msg("file watch shutdown")
			return nil
		case <-t.C:
			log.Trace().Str("input", in.cfg.Type).Str("file", fpath).Err(err).Msg("file watch timer")
			if fp != nil {
				size = fsutil.FSizeN(fp)
				if size > fnode.Size {
					log.Trace().Str("input", in.cfg.Type).Str("file", fpath).Int64("offset", fnode.Size).Int64("size", fsutil.FSizeN(fp)).Msg("file read loop")
					if err = in.fileReadUntilEOF(ctx, reader, codec, fpath, &fnode, statChan, outChan); err != nil && err != io.EOF {
						log.Error().Str("input", in.cfg.Type).Str("file", fpath).Err(err).Msg("read failed")
						fp.Close()
						fp = nil
					}
				}
			}
			if fp, truncated, recreated, err = in.openFile(fp, reader, fpath, &fnode); err == nil {
				if truncated {
					log.Debug().Str("input", in.cfg.Type).Str("file", fpath).Msg("reopen truncated")
				} else if recreated {
					log.Debug().Str("input", in.cfg.Type).Str("file", fpath).Msg("reopen recreated")
				}
				if truncated || recreated {
					log.Trace().Str("input", in.cfg.Type).Str("file", fpath).Int64("offset", fnode.Size).Int64("size", fsutil.FSizeN(fp)).Msg("file read loop")
					if err = in.fileReadUntilEOF(ctx, reader, codec, fpath, &fnode, statChan, outChan); err != nil && err != io.EOF {
						log.Error().Str("input", in.cfg.Type).Str("file", fpath).Err(err).Msg("read failed")
						fp.Close()
						fp = nil
					}
				}
			} else {
				log.Error().Str("input", in.cfg.Type).Str("file", fpath).Err(err).Msg("open failed")
			}
		}
		t.Reset(in.cfg.Interval)
		log.Trace().Str("input", in.cfg.Type).Str("file", fpath).Err(err).Msg("file watch timer reset")
	}
}

func (in *File) fileReadUntilEOF(ctx context.Context, reader *lreader.Reader, codec codec.Codec, fpath string, fnode *fsutil.Fsnode, statChan chan<- fstatdb.StatEvent, outChan chan<- *event.Event) (err error) {
	var (
		e    *event.Event
		data []byte
	)
	select {
	case <-ctx.Done():
		err = errShutdown
		log.Trace().Str("input", in.cfg.Type).Str("file", fpath).Msg("cancel")
		return
	default:
	}
	processed := 0
	ts := timeutil.Now()
	for {
		if data, err = reader.ReadUntil('\n'); err != nil {
			break
		}
		processed++
		fnode.Size += int64(len(data))
		if e, err = codec.Parse(ts, data); err == nil {
			log.Trace().Str("input", in.cfg.Type).Str("file", fpath).Str("text", stringutils.UnsafeString(data)).Str("event", event.String(e)).Err(err).Msg("parse")
			if e != nil {
				outChan <- e
			}
		} else {
			log.Debug().Str("input", in.cfg.Type).Str("file", fpath).Str("text", stringutils.UnsafeString(data)).Err(err).Msg("parse")
		}

		if processed > 20 {
			if statChan != nil {
				statChan <- fstatdb.StatEvent{Path: fpath, Stat: *fnode}
				processed = 0
			}
			if _, done := <-ctx.Done(); done {
				err = errShutdown
				break
			}
		}
	}
	if statChan != nil && processed > 0 {
		statChan <- fstatdb.StatEvent{Path: fpath, Stat: *fnode}
	}
	log.Trace().Str("input", in.cfg.Type).Str("file", fpath).Err(err).Msg("file read loop end")
	return err
}
