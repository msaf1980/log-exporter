package line_test

import (
	"testing"

	"github.com/msaf1980/log-exporter/pkg/codec"
	_ "github.com/msaf1980/log-exporter/pkg/codec_init"
	"github.com/msaf1980/log-exporter/pkg/config"
	"github.com/msaf1980/log-exporter/pkg/event"
	"github.com/msaf1980/log-exporter/pkg/timeutil"
	"github.com/stretchr/testify/assert"
)

func TestLine_Parse(t *testing.T) {
	typ := "file"
	hostname := "abcd"
	path := "/var/log/messages"
	p, err := codec.New(&config.ConfigRaw{"type": typ, "codec": "line"}, &config.Common{Hostname: hostname}, path)
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
				Timestamp: ts.Time(),
				Fields:    map[string]interface{}{"timestamp": ts.String(), "message": "line", "type": typ, "host": hostname, "path": path},
			},
		},
		{
			name: "string\\r\\n",
			data: []byte("string\r\n"),
			want: &event.Event{
				Timestamp: ts.Time(),
				Fields:    map[string]interface{}{"timestamp": ts.String(), "message": "string", "type": typ, "host": hostname, "path": path},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := p.Parse(ts, tt.data)
			if (err != nil) != tt.wantErr {
				t.Fatalf("Line.Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			assert.Equal(t, got, tt.want)
		})
	}
}
