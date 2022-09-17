package event

import (
	"fmt"
	"time"
)

type Event struct {
	Timestamp time.Time
	Fields    map[string]interface{}
	Tags      map[string]int
}

func String(e *Event) string {
	if e == nil {
		return "nil"
	}
	return fmt.Sprintf("{ timestamp: '%s', fields: %#v, tags: %#v }", e.Timestamp.Format(time.RFC3339Nano), e.Fields, e.Tags)
}
