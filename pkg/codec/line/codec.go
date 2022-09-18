package line

import (
	"bytes"

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
	return &Line{typ: cfg.GetStringWithDefault("type", ""), path: path, common: common, name: cfg.GetStringWithDefault("name", Name)}, nil
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
	message := string(data)
	return &event.Event{
		Timestamp: time.Time(),
		Fields: map[string]interface{}{
			"type":      p.typ,
			"name":      p.name,
			"timestamp": time.String(),
			"message":   message,
			"host":      p.common.Hostname,
			"path":      p.path,
		},
	}, nil
}
