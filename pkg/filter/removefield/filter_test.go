package removefield_test

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

func TestRemoveField(t *testing.T) {
	ts := timeutil.Now()

	cfg := config.ConfigRaw{
		"type": "remove_field",
		"fields": []string{
			"test1",
			"test2",
		},
	}
	hostname := "localhost"
	common := &config.Common{Hostname: hostname}

	tests := []struct {
		in   *event.Event
		want *event.Event
	}{
		{
			in: &event.Event{
				Timestamp: ts.Time(),
				Fields: map[string]interface{}{
					"name": "line", "host": hostname, "message": "test1", "path": "", "type": "file",
					"timestamp": ts.String(),
					"test1":     "add 1",
				},
				Tags: map[string]int{},
			},
			want: &event.Event{
				Timestamp: ts.Time(),
				Fields: map[string]interface{}{
					"name": "line", "host": hostname, "message": "test1", "path": "", "type": "file",
					"timestamp": ts.String(),
				},
				Tags: map[string]int{},
			},
		},
		{
			in: &event.Event{
				Timestamp: ts.Time(),
				Fields: map[string]interface{}{
					"name": "access", "host": hostname, "message": "test2", "path": "/var/log/messages", "type": "file",
					"timestamp": ts.String(),
				},
				Tags: map[string]int{},
			},
			want: &event.Event{
				Timestamp: ts.Time(),
				Fields: map[string]interface{}{
					"name": "access", "host": hostname, "message": "test2", "path": "/var/log/messages", "type": "file",
					"timestamp": ts.String(),
				},
				Tags: map[string]int{},
			},
		},
		{
			in: &event.Event{
				Timestamp: ts.Time(),
				Fields: map[string]interface{}{
					"name": "access", "host": hostname, "message": "test2", "path": "/var/log/messages", "type": "file",
					"timestamp": ts.String(),
					"test1":     "add 1",
					"test2":     "add 2",
				},
				Tags: map[string]int{},
			},
			want: &event.Event{
				Timestamp: ts.Time(),
				Fields: map[string]interface{}{
					"name": "access", "host": hostname, "message": "test2", "path": "/var/log/messages", "type": "file",
					"timestamp": ts.String(),
				},
				Tags: map[string]int{},
			},
		},
	}

	fi, err := filter.New(&cfg, common)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	for i, tt := range tests {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			e := test.EventClone(tt.in)
			if err = fi.Parse(e); err != nil {
				t.Fatalf("filter.Parse() error = '%v', got #%v", err, tt.in)
			}
			if eq, diff := test.EventCmp(tt.want, e, false, false); !eq {
				t.Errorf("event mismatch:\n%s", diff)
			}
		})
	}
}
