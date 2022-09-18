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

func EventCmpWithoutTime(want, got *event.Event, skipPath bool) (bool, string) {
	var sb strings.Builder
	if !reflect.DeepEqual(want.Tags, got.Tags) {
		fmt.Fprintf(&sb, "- tags = %#v\n", want.Tags)
		fmt.Fprintf(&sb, "+ tags =  %#v\n", got.Tags)
	}
	for k, wantv := range want.Fields {
		if k == "timestamp" {
			continue
		}
		if skipPath && k == "path" {
			continue
		}
		if gotv, exist := got.Fields[k]; exist {
			if !reflect.DeepEqual(wantv, gotv) {
				fmt.Fprintf(&sb, "- %s = %#v\n", k, wantv)
				fmt.Fprintf(&sb, "+ %s = %#v\n", k, gotv)
			}
		} else {
			fmt.Fprintf(&sb, "- %s = %#v\n", k, wantv)
		}
	}
	for k, gotv := range got.Fields {
		if k == "timestamp" {
			continue
		}
		if _, exist := want.Fields[k]; !exist {
			fmt.Fprintf(&sb, "+ %s = %#v\n", k, gotv)
		}
	}
	if sb.Len() == 0 {
		return true, ""
	}
	return false, sb.String()
}

func EventsCmpWithoutTime(want, got []*event.Event, sortNeed, skipPath bool) (bool, string) {
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
		} else if eq, diff := EventCmpWithoutTime(want[i], got[i], skipPath); !eq {
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
