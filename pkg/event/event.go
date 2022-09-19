package event

import (
	"fmt"

	"github.com/msaf1980/log-exporter/pkg/timeutil"
)

type Event struct {
	Data      []byte // pooled buffer
	Size      int    // size of pooled buffer
	Timestamp timeutil.Time
	Fields    map[string]interface{}
	Tags      map[string]int
}

func New(size int) *Event {
	return &Event{
		Data:   make([]byte, size),
		Size:   size,
		Fields: map[string]interface{}{},
		Tags:   map[string]int{},
	}
}

func String(e *Event) string {
	if e == nil {
		return "nil"
	}
	return fmt.Sprintf("{ timestamp: '%s', fields: %#v, tags: %#v }", e.Timestamp.String(), e.Fields, e.Tags)
}
