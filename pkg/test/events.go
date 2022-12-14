package test

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/msaf1980/log-exporter/pkg/event"
)

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// EventClone clone event (not deep copy for fields)
func EventClone(e *event.Event) *event.Event {
	c := &event.Event{
		Timestamp: e.Timestamp,
		Fields:    map[string]interface{}{},
		Tags:      map[string]int{},
	}
	for k, v := range e.Fields {
		c.Fields[k] = v
	}
	for k, v := range e.Tags {
		c.Tags[k] = v
	}

	return c
}

func EventsDump(events []*event.Event) string {
	var sb strings.Builder
	if len(events) == 0 {
		return "{}"
	}
	last := len(events) - 1
	sb.WriteString("{\n")
	for i, e := range events {
		fmt.Fprintf(&sb, "[%d] = %s", i, event.String(e))
		if i < last {
			sb.WriteString(",")
		}
		sb.WriteString("\n")
	}
	sb.WriteString("}\n")
	return sb.String()
}

func EventCmp(want, got *event.Event, skipTime, skipPath bool) (bool, string) {
	if got == nil && want == nil {
		return true, ""
	}
	var sb strings.Builder
	if got == nil {
		fmt.Fprintf(&sb, "- tags = %#v\n", want.Tags)
	} else if want == nil {
		fmt.Fprintf(&sb, "+ tags =  %#v\n", got.Tags)
	} else if !reflect.DeepEqual(want.Tags, got.Tags) {
		fmt.Fprintf(&sb, "- tags = %#v\n", want.Tags)
		fmt.Fprintf(&sb, "+ tags =  %#v\n", got.Tags)
	}

	if want != nil {
		for k, wantv := range want.Fields {
			if k == "timestamp" && skipTime {
				continue
			}
			if skipPath && k == "path" {
				continue
			}
			if got == nil {
				fmt.Fprintf(&sb, "- %s = %#v\n", k, wantv)
			} else if gotv, exist := got.Fields[k]; exist {
				if !reflect.DeepEqual(wantv, gotv) {
					fmt.Fprintf(&sb, "- %s = %#v\n", k, wantv)
					fmt.Fprintf(&sb, "+ %s = %#v\n", k, gotv)
				}
			} else {
				fmt.Fprintf(&sb, "- %s = %#v\n", k, wantv)
			}
		}
	}
	if got != nil {
		for k, gotv := range got.Fields {
			if k == "timestamp" {
				continue
			}
			if want == nil {
				fmt.Fprintf(&sb, "+ %s = %#v\n", k, gotv)
			} else if _, exist := want.Fields[k]; !exist {
				fmt.Fprintf(&sb, "+ %s = %#v\n", k, gotv)
			}
		}
	}
	if sb.Len() == 0 {
		return true, ""
	}
	return false, sb.String()
}

func EventsCmp(want, got []*event.Event, sortNeed, skipTime, skipPath bool) (bool, string) {
	var sb strings.Builder

	if sortNeed {
		sort.Slice(want, func(i, j int) bool {
			wantI := want[i].Fields["message"].(string)
			wantJ := want[j].Fields["message"].(string)
			if wantI == wantJ && !skipPath {
				wantI = want[i].Fields["path"].(string)
				wantJ = want[j].Fields["path"].(string)
			}
			return wantI < wantJ
		})
		sort.Slice(got, func(i, j int) bool {
			gotI := got[i].Fields["message"].(string)
			gotJ := got[j].Fields["message"].(string)
			if gotI == gotJ && !skipPath {
				gotI = got[i].Fields["path"].(string)
				gotJ = got[j].Fields["path"].(string)
			}
			return gotI < gotJ
		})
	}

	maxLen := max(len(want), len(got))
	for i := 0; i < maxLen; i++ {
		if i >= len(got) {
			fmt.Fprintf(&sb, "- [%d] = %s\n", i, event.String(want[i]))
		} else if i >= len(want) {
			fmt.Fprintf(&sb, "+ [%d] = %s\n", i, event.String(got[i]))
		} else if eq, diff := EventCmp(want[i], got[i], skipTime, skipPath); !eq {
			fmt.Fprintf(&sb, "[%d] = { \n%s}\n", i, diff)
		}
	}

	if sb.Len() == 0 {
		return true, ""
	}
	return false, sb.String()
}

func EventsFromChannel(fchan <-chan *event.Event, timeout time.Duration) []*event.Event {
	events := make([]*event.Event, 0, 1)
	t := time.NewTimer(timeout)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			return events
		case e := <-fchan:
			if e != nil {
				events = append(events, e)
			}
		}
	}
}
