package event

import (
	"fmt"
	"time"
)

type Event struct {
	Data      []byte // pooled buffer
	Size      int    // size of pooled buffer
	Timestamp time.Time
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
	return fmt.Sprintf("{ timestamp: '%s', fields: %#v, tags: %#v }", e.Timestamp.Format(time.RFC3339Nano), e.Fields, e.Tags)
}
