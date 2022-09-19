package addfield_test

import (
	"sync"
	"testing"

	"github.com/msaf1980/log-exporter/pkg/config"
	"github.com/msaf1980/log-exporter/pkg/event"
	"github.com/msaf1980/log-exporter/pkg/filter"
	_ "github.com/msaf1980/log-exporter/pkg/filter_init"
	"github.com/msaf1980/log-exporter/pkg/test"
	"github.com/msaf1980/log-exporter/pkg/timeutil"
)

func TestAddField(t *testing.T) {
	inChan := make(chan *event.Event, 1)
	outChan := make(chan *event.Event, 1)

	ts := timeutil.Now()

	inEvents := []*event.Event{
		{
			Timestamp: ts.Time(),
			Fields: map[string]interface{}{
				"name": "line", "host": "localhost", "message": "test1", "path": "", "type": "file",
				"timestamp": ts.String(),
			},
			Tags: map[string]int{},
		},
		{
			Timestamp: ts.Time(),
			Fields: map[string]interface{}{
				"name": "access", "host": "localhost", "message": "test2", "path": "/var/log/messages", "type": "file",
				"timestamp": ts.String(),
			},
			Tags: map[string]int{},
		},
	}
	wantEvents := make([]*event.Event, 0, len(inEvents))
	gotEvents := make([]*event.Event, 0, len(inEvents))
	for _, e := range inEvents {
		c := test.EventClone(e)
		c.Fields["test1"] = "add 1"
		c.Fields["test2"] = "localhost 2 %{timestamp1} " + ts.String()
		wantEvents = append(wantEvents, c)
	}

	cfg := config.ConfigRaw{
		"type": "add_field",
		"fields": map[string]interface{}{
			"test1": "add 1",
			"test2": "%{host} 2 %{timestamp1} %{timestamp}",
		},
	}
	common := &config.Common{Hostname: "localhost"}

	fi, err := filter.New(&cfg, common)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for e := range outChan {
			gotEvents = append(gotEvents, e)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for _, e := range inEvents {
			inChan <- e
		}
		close(inChan)
	}()

	err = fi.Start(inChan, outChan)
	close(outChan)
	if err != nil {
		t.Fatalf("fi.Start() error = %v", err)
	}

	wg.Wait()

	if eq, diff := test.EventsCmp(wantEvents, gotEvents, true, true, true); !eq {
		t.Errorf("sevents (want %d, got %d) mismatch:\n%s", 0, len(gotEvents), diff)
	}
}
