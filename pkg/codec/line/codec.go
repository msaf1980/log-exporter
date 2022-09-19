package line

import (
	"bytes"

	"github.com/msaf1980/go-stringutils"
	"github.com/msaf1980/log-exporter/pkg/codec"
	"github.com/msaf1980/log-exporter/pkg/config"
	"github.com/msaf1980/log-exporter/pkg/event"
	"github.com/msaf1980/log-exporter/pkg/timeutil"
)

type Line struct {
	name   string
	typ    string
	path   string
	common *config.Common
}

const Name = "line"

func New(cfg *config.ConfigRaw, common *config.Common, path string) (codec.Codec, error) {
	return &Line{
		typ: cfg.GetStringWithDefault("type", ""), path: path, common: common, name: cfg.GetStringWithDefault("name", Name),
	}, nil
}

func (p *Line) Name() string {
	return p.name
}

func (p *Line) Parse(time timeutil.Time, data []byte) (*event.Event, error) {
	if len(data) == 0 {
		return nil, codec.ErrEmpty
	}
	if data[len(data)-1] != '\n' {
		return nil, codec.ErrIncomplete
	}
	data = bytes.TrimRight(data, "\r\n")
	if len(data) == 0 {
		return nil, codec.ErrEmpty
	}

	e := event.Get(data)
	if e == nil {
		// non-pooled
		message := string(data) // data is a slice of reader buffer and destroyed on next read
		return &event.Event{
			Timestamp: time,
			Fields: map[string]interface{}{
				"type":    p.typ,
				"name":    p.name,
				"message": message,
				"host":    p.common.Hostname,
				"path":    p.path,
			},
		}, nil
	} else {
		message := stringutils.UnsafeString(e.Data[:e.Size])
		for k := range e.Fields {
			delete(e.Fields, k)
		}
		e.Fields["type"] = p.typ
		e.Fields["name"] = p.name
		e.Fields["message"] = message
		e.Fields["host"] = p.common.Hostname
		e.Fields["path"] = p.path

		for k := range e.Tags {
			delete(e.Tags, k)
		}

		return e, nil
	}
}
