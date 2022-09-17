package file_test

import (
	"context"
	"os"
	"path"
	"reflect"
	"runtime"
	"sync"
	"testing"
	"time"

	_ "github.com/msaf1980/log-exporter/pkg/codec_init"
	"github.com/msaf1980/log-exporter/pkg/config"
	"github.com/msaf1980/log-exporter/pkg/event"
	"github.com/msaf1980/log-exporter/pkg/fsutil"
	"github.com/msaf1980/log-exporter/pkg/input"
	"github.com/msaf1980/log-exporter/pkg/input/file"
	_ "github.com/msaf1980/log-exporter/pkg/input_init"
	"github.com/msaf1980/log-exporter/pkg/test"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func TestNew(t *testing.T) {
	tests := []struct {
		name    string
		cfg     config.ConfigRaw
		want    *file.Config
		wantErr bool
	}{
		{
			name: "default",
			cfg: config.ConfigRaw{
				"type": "file",
				"path": "/var/log/*.log",
			},
			want: &file.Config{
				Config:   input.Config{Type: file.Name},
				Read:     65536,
				Interval: time.Second,
				Path:     "/var/log/*.log",
			},
			wantErr: false,
		},
		{
			name: "seekf",
			cfg: config.ConfigRaw{
				"type":      "file",
				"path":      "/var/log/*.log",
				"read":      "12k",
				"interval":  5 * time.Second,
				"start_end": true,
				"seek_file": "/var/lib/log-exporter/file/seek",
			},
			want: &file.Config{
				Config:   input.Config{Type: file.Name},
				Read:     12288,
				Interval: 5 * time.Second,
				Path:     "/var/log/*.log",
				StartEnd: true,
				SeekFile: "/var/lib/log-exporter/file/seek",
			},
			wantErr: false,
		},
	}
	common := &config.Common{Hostname: "localhost"}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := file.New(&tt.cfg, common)
			if (err != nil) != tt.wantErr {
				t.Fatalf("New() error = %v, wantErr %v", err, tt.wantErr)
			}
			cfg := got.(*file.File).Cfg()

			if !reflect.DeepEqual(cfg, tt.want) {
				t.Errorf("New().cfg =\n%#v\n, want\n%#v", cfg, tt.want)
			}
			if common != got.(*file.File).Common() {
				t.Errorf("New().common =\n%#v\n, want\n%#v", got.(*file.File).Common(), common)
			}
		})
	}
}

func TestFileFromEnd(t *testing.T) {
	interval := 100 * time.Millisecond
	cfg := config.ConfigRaw{
		"type":      "file",
		"path":      "*.log",
		"interval":  interval,
		"start_end": true,
		"seek_file": "seek.db",
	}
	common := &config.Common{Hostname: "localhost"}
	testDir, err := os.MkdirTemp("", "log-exporter")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(testDir)

	cfg["path"] = path.Join(testDir, cfg["path"].(string))
	cfg["seek_file"] = path.Join(testDir, cfg["seek_file"].(string))

	f1Path := path.Join(testDir, "f1.log")
	f2Path := path.Join(testDir, "f2")

	f1, err := os.Create(f1Path)
	if err != nil {
		t.Fatal(err)
	}

	f2, err := os.Create(f2Path)
	if err != nil {
		t.Fatal(err)
	}
	err = os.Symlink(f2Path, f2Path+".log")
	if err != nil {
		t.Fatal(err)
	}

	f1.WriteString("test 1 1\n")
	f1.Sync()
	// log.Trace().Str("file", f1.Name()).Int64("size", fsutil.FSizeN(f1)).Msg("write: test 1 1\n")
	f2.WriteString("test 2 message              ddhfhfhffh") // partial write, no endline
	// log.Trace().Str("file", f2.Name()).Int64("size", fsutil.FSizeN(f2)).Msg("write: test 2 ")
	f2.Sync()

	in, err := input.New(&cfg, common)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	fchan := make(chan *event.Event, 10)
	var (
		eq         bool
		diff       string
		events     []*event.Event
		wantEvents []*event.Event
		wg         sync.WaitGroup
		startErr   error
	)

	wg.Add(1)
	go func() {
		defer wg.Done()
		startErr = in.Start(ctx, fchan)
		log.Debug().Msg("shutdown")
		close(fchan)
	}()
	time.Sleep(10 * time.Millisecond)

	// Check for append
	if _, err = f1.WriteString("test 1 2\ntest 1 3"); err != nil {
		t.Fatal(err)
	}
	f1.Sync()
	log.Trace().Str("file", f1.Name()).Int64("size", fsutil.FSizeN(f1)).Msg("write: test 1 2\ntest 1 3")

	wantEvents = []*event.Event{
		{
			Fields: map[string]interface{}{"host": "localhost", "message": "test 1 2", "path": f1.Name(), "type": "file"},
		},
	}
	events = test.EventsFromChannel(fchan, 2*interval+1000*time.Millisecond)
	if eq, diff = test.EventsCmpWithoutTime(wantEvents, events); !eq {
		t.Errorf("events(want %d, got %d) mismatch:\n%s", len(wantEvents), len(events), diff)
	}
	// complete line
	if _, err = f1.WriteString("\n"); err != nil {
		t.Fatal(err)
	}
	f1.Sync()
	log.Trace().Str("file", f1.Name()).Int64("size", fsutil.FSizeN(f1)).Msg("write: \n, complete test 1 3\n")
	runtime.Gosched()
	time.Sleep(interval + 200*time.Millisecond)
	wantEvents = []*event.Event{
		{
			Fields: map[string]interface{}{"host": "localhost", "message": "test 1 3", "path": f1.Name(), "type": "file"},
		},
	}
	events = test.EventsFromChannel(fchan, 2*interval+100*time.Millisecond)
	if eq, diff = test.EventsCmpWithoutTime(wantEvents, events); !eq {
		t.Errorf("events(want %d, got %d) mismatch:\n%s", len(wantEvents), len(events), diff)
	}

	// Check for truncate and append
	f2.Truncate(0)
	f2.Seek(0, 0)
	if _, err = f2.WriteString("test 2 \n"); err != nil {
		t.Fatal(err)
	}
	f2.Sync()
	log.Trace().Str("file", f2.Name()).Int64("size", fsutil.FSizeN(f2)).Msg("truncate, write: test 2 \n")
	time.Sleep(interval + 200*time.Millisecond)
	wantEvents = []*event.Event{
		{
			Fields: map[string]interface{}{"host": "localhost", "message": "test 2 ", "path": f2.Name(), "type": "file"},
		},
	}
	events = test.EventsFromChannel(fchan, 2*interval+100*time.Millisecond)
	if eq, diff = test.EventsCmpWithoutTime(wantEvents, events); !eq {
		t.Errorf("events(want %d, got %d) mismatch:\n%s", len(wantEvents), len(events), diff)
	}

	time.Sleep(2*interval + 100*time.Millisecond)
	log.Trace().Msg("shutdown initiated")
	cancel()
	wg.Wait()

	events = test.EventsFromChannel(fchan, 2*interval+100*time.Millisecond)
	if eq, diff = test.EventsCmpWithoutTime(nil, events); !eq {
		t.Errorf("flush events(want %d, got %d) mismatch:\n%s", 0, len(events), diff)
	}

	if startErr != nil {
		t.Fatalf("in.Start() error = %v", startErr)
	}

	// start again, no new events
	fchan = make(chan *event.Event, 10)
	ctx, cancel = context.WithCancel(context.Background())

	wg.Add(1)
	go func() {
		defer wg.Done()
		startErr = in.Start(ctx, fchan)
		log.Debug().Msg("shutdown")
		close(fchan)
	}()
	time.Sleep(10 * time.Millisecond)

	// Check for append again
	if _, err = f1.WriteString("test 1 4\n"); err != nil {
		t.Fatal(err)
	}
	f1.Sync()
	log.Trace().Str("file", f1.Name()).Int64("size", fsutil.FSizeN(f1)).Msg("write: test 1 4\n")
	wantEvents = []*event.Event{
		{
			Fields: map[string]interface{}{"host": "localhost", "message": "test 1 4", "path": f1.Name(), "type": "file"},
		},
	}
	events = test.EventsFromChannel(fchan, 2*interval+100*time.Millisecond)
	if eq, diff = test.EventsCmpWithoutTime(wantEvents, events); !eq {
		t.Errorf("events(want %d, got %d) mismatch:\n%s", len(wantEvents), len(events), diff)
	}

	time.Sleep(2*interval + 100*time.Millisecond)
	log.Trace().Msg("shutdown initiated")
	cancel()
	wg.Wait()

	events = test.EventsFromChannel(fchan, 2*interval+100*time.Millisecond)
	if eq, diff = test.EventsCmpWithoutTime(nil, events); !eq {
		t.Errorf("second flush events(want %d, got %d) mismatch:\n%s", 0, len(events), diff)
	}

	if startErr != nil {
		t.Fatalf("second in.Start() error = %v", startErr)
	}
}

func init() {
	if os.Getenv("GO_TESTS_LEVEL") == "trace" {
		zerolog.SetGlobalLevel(zerolog.TraceLevel)
	} else if os.Getenv("GO_TESTS_LEVEL") == "debug" {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	} else {
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	}
}
