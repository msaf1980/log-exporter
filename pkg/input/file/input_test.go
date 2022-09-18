package file_test

import (
	"context"
	"os"
	"path"
	"reflect"
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
				Config:     input.Config{Type: file.Name},
				ReadBuffer: 65536,
				Interval:   time.Second,
				Path:       "/var/log/*.log",
			},
			wantErr: false,
		},
		{
			name: "seekf",
			cfg: config.ConfigRaw{
				"type":        "file",
				"path":        "/var/log/*.log",
				"read_buffer": "12k",
				"interval":    5 * time.Second,
				"start_end":   true,
				"seek_file":   "/var/lib/log-exporter/file/seek",
			},
			want: &file.Config{
				Config:     input.Config{Type: file.Name},
				ReadBuffer: 12288,
				Interval:   5 * time.Second,
				Path:       "/var/log/*.log",
				StartEnd:   true,
				SeekFile:   "/var/lib/log-exporter/file/seek",
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

func TestFileTail(t *testing.T) {
	interval := 100 * time.Millisecond
	cfg := config.ConfigRaw{
		"type":      "file",
		"path":      "*.log",
		"interval":  interval,
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
		{Fields: map[string]interface{}{"name": "line", "host": "localhost", "message": "test 1 1", "path": f1.Name(), "type": "file"}},
		{Fields: map[string]interface{}{"name": "line", "host": "localhost", "message": "test 1 2", "path": f1.Name(), "type": "file"}},
	}
	events = test.EventsFromChannel(fchan, 2*interval+100*time.Millisecond)
	if eq, diff = test.EventsCmpWithoutTime(wantEvents, events, true, false); !eq {
		t.Errorf("events(want %d, got %d) mismatch:\n%s", len(wantEvents), len(events), diff)
	}
	// complete line
	if _, err = f1.WriteString("\n"); err != nil {
		t.Fatal(err)
	}
	f1.Sync()
	log.Trace().Str("file", f1.Name()).Int64("size", fsutil.FSizeN(f1)).Msg("write: \n, complete test 1 3\n")
	wantEvents = []*event.Event{
		{Fields: map[string]interface{}{"name": "line", "host": "localhost", "message": "test 1 3", "path": f1.Name(), "type": "file"}},
	}
	events = test.EventsFromChannel(fchan, 2*interval+100*time.Millisecond)
	if eq, diff = test.EventsCmpWithoutTime(wantEvents, events, false, false); !eq {
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
	wantEvents = []*event.Event{
		{Fields: map[string]interface{}{"name": "line", "host": "localhost", "message": "test 2 ", "path": f2.Name(), "type": "file"}},
	}
	events = test.EventsFromChannel(fchan, 2*interval+100*time.Millisecond)
	if eq, diff = test.EventsCmpWithoutTime(wantEvents, events, true, false); !eq {
		t.Errorf("events(want %d, got %d) mismatch:\n%s", len(wantEvents), len(events), diff)
	}

	time.Sleep(100 * time.Millisecond)
	log.Trace().Msg("shutdown initiated")
	cancel()
	wg.Wait()

	events = test.EventsFromChannel(fchan, 100*time.Millisecond)
	if eq, diff = test.EventsCmpWithoutTime(nil, events, false, false); !eq {
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
		{Fields: map[string]interface{}{"name": "line", "host": "localhost", "message": "test 1 4", "path": f1.Name(), "type": "file"}},
	}
	events = test.EventsFromChannel(fchan, 2*interval+100*time.Millisecond)
	if eq, diff = test.EventsCmpWithoutTime(wantEvents, events, false, false); !eq {
		t.Errorf("events(want %d, got %d) mismatch:\n%s", len(wantEvents), len(events), diff)
	}

	time.Sleep(100 * time.Millisecond)
	log.Trace().Msg("shutdown initiated")
	cancel()
	wg.Wait()

	events = test.EventsFromChannel(fchan, 100*time.Millisecond)
	if eq, diff = test.EventsCmpWithoutTime(nil, events, false, false); !eq {
		t.Errorf("second flush events(want %d, got %d) mismatch:\n%s", 0, len(events), diff)
	}

	if startErr != nil {
		t.Fatalf("second in.Start() error = %v", startErr)
	}
}

func TestFileTailFromEnd(t *testing.T) {
	interval := 100 * time.Millisecond
	cfg := config.ConfigRaw{
		"name":      "syslog",
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
		{Fields: map[string]interface{}{"name": "syslog", "host": "localhost", "message": "test 1 2", "path": f1.Name(), "type": "file"}},
	}
	events = test.EventsFromChannel(fchan, 2*interval+100*time.Millisecond)
	if eq, diff = test.EventsCmpWithoutTime(wantEvents, events, false, false); !eq {
		t.Errorf("events(want %d, got %d) mismatch:\n%s", len(wantEvents), len(events), diff)
	}
	// complete line
	if _, err = f1.WriteString("\n"); err != nil {
		t.Fatal(err)
	}
	f1.Sync()
	log.Trace().Str("file", f1.Name()).Int64("size", fsutil.FSizeN(f1)).Msg("write: \n, complete test 1 3\n")
	wantEvents = []*event.Event{
		{Fields: map[string]interface{}{"name": "syslog", "host": "localhost", "message": "test 1 3", "path": f1.Name(), "type": "file"}},
	}
	events = test.EventsFromChannel(fchan, 2*interval+100*time.Millisecond)
	if eq, diff = test.EventsCmpWithoutTime(wantEvents, events, false, false); !eq {
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
	wantEvents = []*event.Event{
		{Fields: map[string]interface{}{"name": "syslog", "host": "localhost", "message": "test 2 ", "path": f2.Name(), "type": "file"}},
	}
	events = test.EventsFromChannel(fchan, 2*interval+100*time.Millisecond)
	if eq, diff = test.EventsCmpWithoutTime(wantEvents, events, false, false); !eq {
		t.Errorf("events(want %d, got %d) mismatch:\n%s", len(wantEvents), len(events), diff)
	}

	time.Sleep(100 * time.Millisecond)
	log.Trace().Msg("shutdown initiated")
	cancel()
	wg.Wait()

	events = test.EventsFromChannel(fchan, 100*time.Millisecond)
	if eq, diff = test.EventsCmpWithoutTime(nil, events, false, false); !eq {
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
		{Fields: map[string]interface{}{"name": "syslog", "host": "localhost", "message": "test 1 4", "path": f1.Name(), "type": "file"}},
	}
	events = test.EventsFromChannel(fchan, 2*interval+100*time.Millisecond)
	if eq, diff = test.EventsCmpWithoutTime(wantEvents, events, false, false); !eq {
		t.Errorf("events(want %d, got %d) mismatch:\n%s", len(wantEvents), len(events), diff)
	}

	time.Sleep(100 * time.Millisecond)
	log.Trace().Msg("shutdown initiated")
	cancel()
	wg.Wait()

	events = test.EventsFromChannel(fchan, 100*time.Millisecond)
	if eq, diff = test.EventsCmpWithoutTime(nil, events, false, false); !eq {
		t.Errorf("second flush events(want %d, got %d) mismatch:\n%s", 0, len(events), diff)
	}

	if startErr != nil {
		t.Fatalf("second in.Start() error = %v", startErr)
	}
}

func TestFileStress(t *testing.T) {
	testDir, err := os.MkdirTemp("", "log-exporter")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(testDir)

	f1Path := path.Join(testDir, "f1.log")
	f2Path := path.Join(testDir, "f2.log")

	f1, err := os.Create(f1Path)
	if err != nil {
		t.Fatal(err)
	}

	f2, err := os.Create(f2Path)
	if err != nil {
		t.Fatal(err)
	}

	interval := 100 * time.Millisecond
	cfg := config.ConfigRaw{
		"type":      "file",
		"path":      "*.log",
		"interval":  interval,
		"seek_file": "seek.db",
	}
	cfg["path"] = path.Join(testDir, cfg["path"].(string))
	cfg["seek_file"] = path.Join(testDir, cfg["seek_file"].(string))

	common := &config.Common{Hostname: "localhost"}

	in, err := input.New(&cfg, common)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	testData := test.Strings(256, 1024)
	wantEvents := make([]*event.Event, 0, len(testData))
	for _, s := range testData {
		e := &event.Event{
			Fields: map[string]interface{}{"name": "line", "host": "localhost", "message": s, "path": "", "type": "file"},
		}
		wantEvents = append(wantEvents, e)
	}

	var (
		wg       sync.WaitGroup
		startErr error
	)

	events := make([]*event.Event, 0, len(testData))

	ctx, cancel := context.WithCancel(context.Background())
	fchan := make(chan *event.Event, 10)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for e := range fchan {
			events = append(events, e)
			if len(events) == len(testData) {
				break
			}
		}
		cancel()
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		startErr = in.Start(ctx, fchan)
		log.Debug().Msg("shutdown")
		close(fchan)
	}()

	for i := range testData {
		if i%2 == 0 {
			f1.WriteString(testData[i])
			f1.WriteString("\n")
		} else {
			f2.WriteString(testData[i])
			f2.WriteString("\n")
		}
	}

	wg.Wait()

	if startErr != nil {
		t.Fatalf("second in.Start() error = %v", startErr)
	}

	if eq, diff := test.EventsCmpWithoutTime(wantEvents, events, true, true); !eq {
		t.Errorf("second flush events(want %d, got %d) mismatch:\n%s", 0, len(events), diff)
	}
}

func benchmarkFile(b *testing.B, testDir string, n int, readBuffer string) {
	interval := 100 * time.Millisecond
	cfg := config.ConfigRaw{
		"type":        "file",
		"path":        "*.log",
		"interval":    interval,
		"read_buffer": readBuffer,
		"seek_file":   "seek.db",
		"mode":        file.ModeRead,
	}
	cfg["path"] = path.Join(testDir, cfg["path"].(string))
	cfg["seek_file"] = path.Join(testDir, cfg["seek_file"].(string))

	common := &config.Common{Hostname: "localhost"}

	in, err := input.New(&cfg, common)
	if err != nil {
		b.Fatalf("New() error = %v", err)
	}

	// events := make([]*event.Event, 0, n)
	var count int

	start := time.Now()
	b.ResetTimer()
	// b.StopTimer()
	for i := 0; i < b.N; i++ {
		var (
			wg       sync.WaitGroup
			startErr error
		)

		// events = events[:0]
		ctx, cancel := context.WithCancel(context.Background())
		fchan := make(chan *event.Event, 10)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for e := range fchan {
				count++
				_ = e
				// events = append(events, e)
			}
			cancel()
		}()

		wg.Add(1)
		b.StartTimer()
		go func() {
			defer wg.Done()
			startErr = in.Start(ctx, fchan)
			log.Debug().Msg("shutdown")
			close(fchan)
		}()

		wg.Wait()
		// b.StopTimer()

		if startErr != nil {
			b.Fatalf("second in.Start() error = %v", startErr)
		}
		os.Remove(cfg["seek_file"].(string))
	}
	if count != n*b.N {
		b.Fatalf("events count want %d, got %d", n*b.N, count)
	}
	// At golang 1.19 only in master, so can't use at now
	// elapsed : = b.Elapsed()
	elapsed := time.Since(start)
	b.ReportMetric(float64(count/b.N), "events")
	b.ReportMetric(float64(count)/elapsed.Seconds(), "events/s")
	b.ReportMetric(float64(elapsed.Nanoseconds())/float64(count), "ns/event")

	// if eq, diff = test.EventsCmpWithoutTime(nil, events, false); !eq {
	// 	t.Errorf("second flush events(want %d, got %d) mismatch:\n%s", 0, len(events), diff)
	// }

}

func writeFiles2(name1, name2 string, testData []string) (string, error) {
	testDir, err := os.MkdirTemp("", "log-exporter")
	if err != nil {
		return "", err
	}

	f1Path := path.Join(testDir, "f1.log")
	f2Path := path.Join(testDir, "f2.log")

	f1, err := os.Create(f1Path)
	if err != nil {
		return testDir, err
	}
	defer f1.Close()

	f2, err := os.Create(f2Path)
	if err != nil {
		return testDir, err
	}

	for i := range testData {
		if i%2 == 0 {
			f1.WriteString(testData[i])
			f1.WriteString("\n")
		} else {
			f2.WriteString(testData[i])
			f2.WriteString("\n")
		}
	}

	if err = f1.Sync(); err != nil {
		return testDir, err
	}
	if err = f2.Sync(); err != nil {
		return testDir, err
	}
	return testDir, nil
}

func writeFiles4(name1, name2, name3, name4 string, testData []string) (string, error) {
	testDir, err := os.MkdirTemp("", "log-exporter")
	if err != nil {
		return "", err
	}

	f1Path := path.Join(testDir, "f1.log")
	f2Path := path.Join(testDir, "f2.log")
	f3Path := path.Join(testDir, "f3.log")
	f4Path := path.Join(testDir, "f4.log")

	f1, err := os.Create(f1Path)
	if err != nil {
		return testDir, err
	}
	defer f1.Close()

	f2, err := os.Create(f2Path)
	if err != nil {
		return testDir, err
	}
	defer f2.Close()

	f3, err := os.Create(f3Path)
	if err != nil {
		return testDir, err
	}
	defer f3.Close()

	f4, err := os.Create(f4Path)
	if err != nil {
		return testDir, err
	}
	defer f4.Close()

	for i := range testData {
		k := i % 2
		if k == 0 {
			f1.WriteString(testData[i])
			f1.WriteString("\n")
		} else if k == 1 {
			f2.WriteString(testData[i])
			f2.WriteString("\n")
		} else if k == 2 {
			f3.WriteString(testData[i])
			f3.WriteString("\n")
		} else {
			f4.WriteString(testData[i])
			f4.WriteString("\n")
		}
	}

	if err = f1.Sync(); err != nil {
		return testDir, err
	}
	if err = f2.Sync(); err != nil {
		return testDir, err
	}
	return testDir, nil
}

func BenchmarkFiles4_1000_Buf_64k(t *testing.B) {
	benchData := test.Strings(2560, 1000)

	testDir, err := writeFiles4("f1.log", "f2.log", "f3.log", "f4.log", benchData)
	if err != nil {
		if testDir != "" {
			os.RemoveAll(testDir)
		}
		t.Fatal(err)
	}
	defer os.RemoveAll(testDir)

	benchmarkFile(t, testDir, len(benchData), "64k")
}

func BenchmarkFiles2_1000_Buf_4k(t *testing.B) {
	benchData := test.Strings(2560, 1000)

	testDir, err := writeFiles2("f1.log", "f2.log", benchData)
	if err != nil {
		if testDir != "" {
			os.RemoveAll(testDir)
		}
		t.Fatal(err)
	}
	defer os.RemoveAll(testDir)

	benchmarkFile(t, testDir, len(benchData), "4k")
}

func BenchmarkFiles2_1000_Buf_16k(t *testing.B) {
	benchData := test.Strings(2560, 1000)

	testDir, err := writeFiles2("f1.log", "f2.log", benchData)
	if err != nil {
		if testDir != "" {
			os.RemoveAll(testDir)
		}
		t.Fatal(err)
	}
	defer os.RemoveAll(testDir)

	benchmarkFile(t, testDir, len(benchData), "16k")
}

func BenchmarkFiles2_1000_Buf_32k(t *testing.B) {
	benchData := test.Strings(2560, 1000)

	testDir, err := writeFiles2("f1.log", "f2.log", benchData)
	if err != nil {
		if testDir != "" {
			os.RemoveAll(testDir)
		}
		t.Fatal(err)
	}
	defer os.RemoveAll(testDir)

	benchmarkFile(t, testDir, len(benchData), "32k")
}

func BenchmarkFiles2_1000_Buf_64k(t *testing.B) {
	benchData := test.Strings(2560, 1000)

	testDir, err := writeFiles2("f1.log", "f2.log", benchData)
	if err != nil {
		if testDir != "" {
			os.RemoveAll(testDir)
		}
		t.Fatal(err)
	}
	defer os.RemoveAll(testDir)

	benchmarkFile(t, testDir, len(benchData), "64k")
}

func BenchmarkFiles2_1000_Buf_256k(t *testing.B) {
	benchData := test.Strings(2560, 100)

	testDir, err := writeFiles2("f1.log", "f2.log", benchData)
	if err != nil {
		if testDir != "" {
			os.RemoveAll(testDir)
		}
		t.Fatal(err)
	}
	defer os.RemoveAll(testDir)

	benchmarkFile(t, testDir, len(benchData), "256k")
}

func BenchmarkFiles2_1000_Buf_1M(t *testing.B) {
	benchData := test.Strings(2560, 1000)

	testDir, err := writeFiles2("f1.log", "f2.log", benchData)
	if err != nil {
		if testDir != "" {
			os.RemoveAll(testDir)
		}
		t.Fatal(err)
	}
	defer os.RemoveAll(testDir)

	benchmarkFile(t, testDir, len(benchData), "1M")
}

func init() {
	logLevel := os.Getenv("GO_TESTS_LEVEL")
	if logLevel == "trace" {
		zerolog.SetGlobalLevel(zerolog.TraceLevel)
	} else if logLevel == "debug" {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	} else if logLevel == "info" {
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	} else {
		zerolog.SetGlobalLevel(zerolog.WarnLevel)
	}
}
