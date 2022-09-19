package line_test

import (
	"testing"

	"github.com/msaf1980/log-exporter/pkg/codec"
	"github.com/msaf1980/log-exporter/pkg/codec/line"
	_ "github.com/msaf1980/log-exporter/pkg/codec_init"
	"github.com/msaf1980/log-exporter/pkg/config"
	"github.com/msaf1980/log-exporter/pkg/event"
	"github.com/msaf1980/log-exporter/pkg/test"
	"github.com/msaf1980/log-exporter/pkg/timeutil"
)

func TestLine_Parse(t *testing.T) {
	typ := "file"
	hostname := "abcd"
	path := "/var/log/messages"
	p, err := codec.New(&config.ConfigRaw{"type": typ}, &config.Common{Hostname: hostname}, path)
	if err != nil {
		t.Fatal(err)
	}
	ts := timeutil.Now()
	tests := []struct {
		name    string
		data    []byte
		want    *event.Event
		wantErr bool
	}{
		{
			name:    "empty #1",
			data:    []byte(""),
			want:    nil,
			wantErr: true,
		},
		{
			name:    "empty #2",
			data:    []byte("\n"),
			want:    nil,
			wantErr: true,
		},
		{
			name:    "empty #3",
			data:    []byte("\r\n"),
			want:    nil,
			wantErr: true,
		},
		{
			name:    "incomplete",
			data:    []byte("line"),
			want:    nil,
			wantErr: true,
		},
		{
			name: "line\\n",
			data: []byte("line\n"),
			want: &event.Event{
				Timestamp: ts,
				Fields:    map[string]interface{}{"message": "line", "type": typ, "name": "line", "host": hostname, "path": path},
				Tags:      map[string]int{},
			},
		},
		{
			name: "string\\r\\n",
			data: []byte("string\r\n"),
			want: &event.Event{
				Timestamp: ts,
				Fields:    map[string]interface{}{"message": "string", "type": typ, "name": "line", "host": hostname, "path": path},
				Tags:      map[string]int{},
			},
		},
	}
	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := p.Parse(ts, tt.data)
			if (err != nil) != tt.wantErr {
				t.Errorf("Line.Parse() error = %v, wantErr %v", err, tt.wantErr)
			}
			if eq, diff := test.EventCmp(got, tt.want, false, false); !eq {
				t.Errorf("event[%d] mismatch:\n%s", i, diff)
			}
			if got != nil {
				//for check clear reused maps
				got.Fields["add"] = "test"
				got.Tags["tag"] = 1
			}
			// put event to pool for reuse
			event.Put(got)
		})
	}
}

func TestLine_ParseWithName(t *testing.T) {
	typ := "file"
	name := "access"
	hostname := "abcd"
	path := "/var/log/messages"
	message := "test"
	ts := timeutil.Now()
	want := &event.Event{
		Timestamp: ts, Fields: map[string]interface{}{
			"name": name, "host": hostname, "message": message, "path": path, "timestamp": ts.String(), "type": typ,
		},
		Tags: map[string]int{},
	}

	p, err := codec.New(&config.ConfigRaw{"type": typ, "name": name}, &config.Common{Hostname: hostname}, path)
	if err != nil {
		t.Fatal(err)
	}
	got, err := p.Parse(ts, []byte(message+"\n"))
	if err != nil {
		t.Fatalf("Line.Parse() error = %v", err)
	}
	if eq, diff := test.EventCmp(got, want, false, false); !eq {
		t.Errorf("event mismatch:\n%s", diff)
	}
	event.Put(got)
}

func benchmarkPase(b *testing.B, data []byte) {
	ts := timeutil.Now()
	p, err := codec.New(&config.ConfigRaw{"type": "file", "codec": line.Name}, &config.Common{Hostname: "localhost"}, "/var/log/message")
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e, err := p.Parse(ts, data)
		if err != nil {
			b.Fatal(err)
		}
		event.Put(e)
	}
}

func BenchmarkParse(t *testing.B) {
	benchData := []byte("Apr 11 08:27:38 host daemon[1061]: [2022-04-11 08:27:38.392] Started\n")
	benchmarkPase(t, benchData)
}
