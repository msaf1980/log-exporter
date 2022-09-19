package addfield_test

import (
	"strconv"
	"testing"

	"github.com/msaf1980/log-exporter/pkg/config"
	"github.com/msaf1980/log-exporter/pkg/event"
	"github.com/msaf1980/log-exporter/pkg/filter"
	_ "github.com/msaf1980/log-exporter/pkg/filter_init"
	"github.com/msaf1980/log-exporter/pkg/test"
	"github.com/msaf1980/log-exporter/pkg/timeutil"
)

func TestAddField(t *testing.T) {
	ts := timeutil.Now()

	hostname := "localhost"
	cfg := config.ConfigRaw{
		"type": "add_field",
		"fields": map[string]interface{}{
			"test1": "add 1",
			"test2": "%{host} 2 %{timestamp1} %{timestamp}",
		},
	}
	common := &config.Common{Hostname: hostname}

	tests := []struct {
		in       *event.Event
		want     *event.Event
		wantPart bool
	}{
		{
			in: &event.Event{
				Timestamp: ts.Time(),
				Fields: map[string]interface{}{
					"name": "line", "host": "localhost", "message": "test1", "path": "", "type": "file",
					"timestamp": ts.String(),
				},
				Tags: map[string]int{},
			},
			want: &event.Event{
				Timestamp: ts.Time(),
				Fields: map[string]interface{}{
					"name": "line", "host": "localhost", "message": "test1", "path": "", "type": "file",
					"timestamp": ts.String(),
					"test1":     "add 1",
					"test2":     "localhost 2 %{timestamp1} " + ts.String(),
				},
				Tags: map[string]int{},
			},
			wantPart: true,
		},
		{
			in: &event.Event{
				Timestamp: ts.Time(),
				Fields: map[string]interface{}{
					"name": "line", "host": "localhost", "message": "test1", "path": "", "type": "file",
					"timestamp":  ts.String(),
					"timestamp1": "test",
				},
				Tags: map[string]int{},
			},
			want: &event.Event{
				Timestamp: ts.Time(),
				Fields: map[string]interface{}{
					"name": "line", "host": "localhost", "message": "test1", "path": "", "type": "file",
					"timestamp":  ts.String(),
					"timestamp1": "test",
					"test1":      "add 1",
					"test2":      "localhost 2 test " + ts.String(),
				},
				Tags: map[string]int{},
			},
		},
		{
			in: &event.Event{
				Timestamp: ts.Time(),
				Fields: map[string]interface{}{
					"name": "access", "host": "localhost", "message": "test2", "path": "/var/log/messages", "type": "file",
					"timestamp": ts.String(),
				},
				Tags: map[string]int{},
			},
			want: &event.Event{
				Timestamp: ts.Time(),
				Fields: map[string]interface{}{
					"name": "access", "host": "localhost", "message": "test2", "path": "/var/log/messages", "type": "file",
					"timestamp": ts.String(),
					"test1":     "add 1",
					"test2":     "localhost 2 %{timestamp1} " + ts.String(),
				},
				Tags: map[string]int{},
			},
			wantPart: true,
		},
	}

	fi, err := filter.New(&cfg, common)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	for i, tt := range tests {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			e := test.EventClone(tt.in)
			err = fi.Parse(e)
			if tt.wantPart {
				if err != filter.ErrPartExpand {
					t.Fatalf("filter.Parse() want error '%v', got error '%v'", filter.ErrPartExpand, err)
				}
			} else if err != nil {
				t.Fatalf("filter.Parse() got error '%v'", err)
			}
			if eq, diff := test.EventCmp(tt.want, e, false, false); !eq {
				t.Errorf("event mismatch:\n%s", diff)
			}
		})
	}
}
