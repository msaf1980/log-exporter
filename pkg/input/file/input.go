package file

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
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
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"
)

const Name = "file"

var errShutdown = errors.New("shutdown")

type Mode int8

const (
	ModeTail Mode = iota
	ModeRead
)

var modeStrings []string = []string{"tail", "read"}

func (m *Mode) Set(value string) error {
	switch value {
	case "tail":
		*m = ModeTail
	case "read":
		*m = ModeRead
	default:
		return fmt.Errorf("invalid mode %s", value)
	}
	return nil
}

func (m *Mode) String() string {
	return modeStrings[*m]
}

// UnmarshalYAML for use Aggregation in yaml files
func (m *Mode) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var value string
	if err := unmarshal(&value); err != nil {
		return err
	}

	if err := m.Set(value); err != nil {
		return fmt.Errorf("failed to parse '%s' to Aggregation: %v", value, err)
	}

	return nil
}

type Config struct {
	input.Config

	Path       string      `hcl:"path" yaml:"path" json:"path"`                      // path glob
	ReadBuffer config.Size `hcl:"read_buffer" yaml:"read_buffer" json:"read_buffer"` // read buffer size
	Codec      string      `hcl:"codec" yaml:"codec" json:"codec"`                   // codec name (deefault - line)
	// Username string        `hcl:"username" yaml:"username"`
	// Pasword  string        `hcl:"usernam" yaml:"username"`
	Interval time.Duration `hcl:"interval" yaml:"interval" json:"interval"`
	// mode = tail If no file record in seek db, no shutdown on io.EOF.  If no file record in seek db, depend on start_end
	// mode = read If no file record in seek db, read from start and exit on io.OEF (for completed files), start_end is ignored
	Mode     Mode   `hcl:"mode" yaml:"mode" json:"mode"`
	StartEnd bool   `hcl:"start_end" yaml:"start_end" json:"start_end"` // read from end  if no file record in seek db
	SeekFile string `hcl:"seek_file" yaml:"seek_file" json:"seek_file"` // if not set, read from end after start
	// ExitAfterRead bool   `hcl:"exit_after_read" yaml:"exit_after_read" json:"exit_after_read"` // shutdown file watcher on io.EOF (for static files and bencmarks)
}

func defaultConfig() Config {
	return Config{
		Config:     input.Config{Type: Name},
		Interval:   time.Second,
		ReadBuffer: config.Size(64 * 1024),
	}
}

// File is file input reader.
//
// In some cases (partial write to file during log rotate) can read incomplete line.
//
// So use proper codec with format check (better, don't fire event) or do post validate in filters (slower)
type File struct {
	cfg    Config
	cfgRaw *config.ConfigRaw
	common *config.Common

	db      *fstatdb.Db
	running int32
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

	if in.cfg.Mode == ModeRead {
		// disable seek file and read from end
		in.cfg.StartEnd = false
	}

	// Check codec config
	_, err := codec.New(in.cfgRaw, in.common, in.cfg.Path)
	if err != nil {
		return nil, jerrors.Annotate(err, "input '"+in.cfg.Type+"' path='"+in.cfg.Path+"'")
	}

	return in, nil
}

func (in *File) Name() string {
	return Name
}

func (in *File) fileStatInit(fpath string, n int, fnodes []fsutil.Fsnode) {
	if in.cfg.Mode == ModeRead {
		if in.db != nil {
			if in.db != nil {
				if fnode, exist := in.db.Get(fpath); exist {
					// seek to the offset in seek db
					fnodes[n] = fnode
				}
			}
		}
	} else {
		// tail
		if in.cfg.StartEnd {
			// seek to the end, if no file in seek db or seek db is disabled
			if in.db != nil {
				if fnode, exist := in.db.Get(fpath); exist {
					// seek to the offset in seek db
					fnodes[n] = fnode
				} else {
					// seek to the end, if seek db is disabled
					fsutil.LStat(fpath, &fnodes[n])
					in.db.Set(fpath, fnodes[n])
				}
			}
		} else if fnode, exist := in.db.Get(fpath); exist {
			// seek to the offset in seek db
			fnodes[n] = fnode
		} else {
			// seek to the end
			in.db.Set(fpath, fnode)
		}
	}
}

func (in *File) Start(ctx context.Context, outChan chan<- *event.Event) error {
	matches, err := filepath.Glob(in.cfg.Path)
	if err != nil {
		return jerrors.Annotate(err, "glob expand failed: "+in.cfg.Path)
	}

	if in.cfg.SeekFile == "" {
		if !in.cfg.StartEnd {
			log.Warn().Str("config", in.common.Config).Str("input", in.cfg.Type).Str("file", in.cfg.Path).Err(err).Msg("seek file not set, force start from end")
			in.cfg.StartEnd = true
		}
	} else {
		in.db = fstatdb.New()
		if err = in.db.Open(in.cfg.SeekFile); err != nil {
			return jerrors.Annotate(err, "open file failed: "+in.cfg.SeekFile)
		}
	}

	files := make([]string, 0, len(matches))
	filesMap := make(map[string]bool)
	fnodes := make([]fsutil.Fsnode, len(matches))
	i := 0
	for _, match := range matches {
		var isDir bool
		fpath, err := evalSymlinks(ctx, match)
		if err != nil {
			log.Error().Str("config", in.common.Config).Str("input", in.cfg.Type).Str("file", fpath).Err(err).Msg("eval symlink failed")
			continue
		}

		if _, exist := filesMap[fpath]; exist {
			continue
		}

		if isDir, err = fsutil.IsDir(fpath); err != nil {
			log.Error().Str("config", in.common.Config).Str("input", in.cfg.Type).Str("file", fpath).Err(err).Msg("stat failed")
			return err
		} else if isDir {
			log.Warn().Str("config", in.common.Config).Str("input", in.cfg.Type).Str("file", fpath).Msg("dir skipping")
			return err
		}

		filesMap[fpath] = true
		files = append(files, fpath)
		in.fileStatInit(fpath, i, fnodes)
		i++
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

	for i, fpath := range files {
		path := fpath
		n := i
		atomic.AddInt32(&in.running, 1)
		eg.Go(func() error {
			defer func() {
				running := atomic.AddInt32(&in.running, -1)
				if running < 1 {
					if in.db != nil {
						close(statChan)
					}
				}
			}()
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
		isDir                bool
	)

	codec, err := codec.New(in.cfgRaw, in.common, fpath)
	if err != nil {
		log.Error().Str("config", in.common.Config).Str("input", in.cfg.Type).Str("codec", in.cfg.Codec).Str("file", fpath).Err(err).Msg("codec init failed")
		return err
	}

	fpath, err = evalSymlinks(ctx, fpath)
	if err != nil {
		log.Error().Str("config", in.common.Config).Str("input", in.cfg.Type).Str("file", fpath).Err(err).Msg("eval symlink failed")
		return err
	}

	if isDir, err = fsutil.IsDir(fpath); err != nil {
		log.Error().Str("config", in.common.Config).Str("input", in.cfg.Type).Str("file", fpath).Err(err).Msg("stat failed")
		return err
	} else if isDir {
		log.Warn().Str("config", in.common.Config).Str("input", in.cfg.Type).Str("file", fpath).Msg("dir skipping")
		return err
	}

	bufSize := int(in.cfg.ReadBuffer.Value())
	reader := lreader.New(fp, bufSize)

	if fp, truncated, recreated, err = in.openFile(fp, reader, fpath, &fnode); err == nil {
		if truncated {
			log.Debug().Str("config", in.common.Config).Str("input", in.cfg.Type).Str("file", fpath).Msg("reopen truncated")
		} else if recreated {
			log.Debug().Str("config", in.common.Config).Str("input", in.cfg.Type).Str("file", fpath).Msg("reopen recreated")
		}
	} else {
		log.Error().Str("config", in.common.Config).Str("input", in.cfg.Type).Str("file", fpath).Err(err).Msg("open failed")
		return err
	}

	defer func() {
		if fp != nil {
			fp.Close()
		}
	}()

	// log.Trace.Str("config", in.common.Config).Str("input", in.cfg.Type).Str("file", fpath).Int64("offset", fnode.Size).Int64("size", fsutil.FSizeN(fp)).Msg("file read loop")
	if err == nil {
		if err = in.fileReadUntilEOF(ctx, reader, codec, fpath, &fnode, statChan, outChan); err != nil {
			if err == errShutdown {
				return nil
			}
			if err != io.EOF {
				log.Error().Str("config", in.common.Config).Str("input", in.cfg.Type).Str("file", fpath).Err(err).Msg("read failed")
				fp.Close()
				fp = nil
			} else if in.cfg.Mode == ModeRead {
				log.Info().Str("config", in.common.Config).Str("input", in.cfg.Type).Str("file", fpath).Msg("read ended on EOF")
				return nil
			}
		}
	}
	if in.cfg.Mode == ModeRead {
		return nil
	}

	// log.Trace.Str("config", in.common.Config).Str("input", in.cfg.Type).Str("file", fpath).Err(err).Msg("file watch started")
	t := time.NewTimer(in.cfg.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			log.Info().Str("config", in.common.Config).Str("input", in.cfg.Type).Str("file", fpath).Msg("shutdown")
			return nil
		case <-t.C:
			// log.Trace.Str("config", in.common.Config).Str("input", in.cfg.Type).Str("file", fpath).Err(err).Msg("file watch timer")
			if fp != nil {
				size = fsutil.FSizeN(fp)
				if size > fnode.Size {
					// log.Trace.Str("config", in.common.Config).Str("input", in.cfg.Type).Str("file", fpath).Int64("offset", fnode.Size).Int64("size", fsutil.FSizeN(fp)).Msg("file read loop")
					if err = in.fileReadUntilEOF(ctx, reader, codec, fpath, &fnode, statChan, outChan); err != nil {
						if err == errShutdown {
							return nil
						}
						if err != io.EOF {
							log.Error().Str("config", in.common.Config).Str("input", in.cfg.Type).Str("file", fpath).Err(err).Msg("read failed")
							fp.Close()
							fp = nil
						}
					}
				}
			}
			if fp, truncated, recreated, err = in.openFile(fp, reader, fpath, &fnode); err == nil {
				if truncated {
					log.Debug().Str("config", in.common.Config).Str("input", in.cfg.Type).Str("file", fpath).Msg("reopen truncated")
				} else if recreated {
					log.Debug().Str("config", in.common.Config).Str("input", in.cfg.Type).Str("file", fpath).Msg("reopen recreated")
				}
				if truncated || recreated {
					// log.Trace.Str("config", in.common.Config).Str("input", in.cfg.Type).Str("file", fpath).Int64("offset", fnode.Size).Int64("size", fsutil.FSizeN(fp)).Msg("file read loop")
					if err = in.fileReadUntilEOF(ctx, reader, codec, fpath, &fnode, statChan, outChan); err != nil {
						if err == errShutdown {
							return nil
						}
						if err != io.EOF {
							log.Error().Str("config", in.common.Config).Str("input", in.cfg.Type).Str("file", fpath).Err(err).Msg("read failed")
							fp.Close()
							fp = nil
						}
					}
				}
			} else {
				log.Error().Str("config", in.common.Config).Str("input", in.cfg.Type).Str("file", fpath).Err(err).Msg("open failed")
			}
		}
		t.Reset(in.cfg.Interval)
		// log.Trace.Str("config", in.common.Config).Str("input", in.cfg.Type).Str("file", fpath).Err(err).Msg("file watch timer reset")
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
		log.Info().Str("config", in.common.Config).Str("input", in.cfg.Type).Str("file", fpath).Msg("shutdown")
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
			if e != nil {
				if zerolog.GlobalLevel() == zerolog.TraceLevel {
					log.Trace().Str("config", in.common.Config).Str("input", in.cfg.Type).Str("file", fpath).Str("text", stringutils.UnsafeString(data)).Str("event", event.String(e)).Err(err).Msg("parse")
				}
				outChan <- e
			}
		} else {
			log.Debug().Str("config", in.common.Config).Str("input", in.cfg.Type).Str("file", fpath).Str("text", stringutils.UnsafeString(data)).Err(err).Msg("parse")
		}

		if processed > 20 {
			if statChan != nil {
				statChan <- fstatdb.StatEvent{Path: fpath, Stat: *fnode}
				processed = 0
			}
			select {
			case <-ctx.Done():
				err = errShutdown
				log.Info().Str("config", in.common.Config).Str("input", in.cfg.Type).Str("file", fpath).Msg("shutdown")
				return
			default:
			}
		}
	}
	if statChan != nil && processed > 0 {
		statChan <- fstatdb.StatEvent{Path: fpath, Stat: *fnode}
	}
	// log.Trace.Str("config", in.common.Config).Str("input", in.cfg.Type).Str("file", fpath).Err(err).Msg("file read loop end")
	return err
}
