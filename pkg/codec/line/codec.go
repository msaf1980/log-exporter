package line

import (
	"bytes"

	"github.com/msaf1980/log-exporter/pkg/codec"
	"github.com/msaf1980/log-exporter/pkg/config"
	"github.com/msaf1980/log-exporter/pkg/event"
	"github.com/msaf1980/log-exporter/pkg/timeutil"
)

type Line struct {
	typ    string
	path   string
	common *config.Common
}

const Name = "line"

func New(cfg *config.ConfigRaw, common *config.Common, path string) (codec.Codec, error) {
	return &Line{typ: cfg.GetStringWithDefault("type", ""), path: path, common: common}, nil
}

func (p *Line) Parse(time timeutil.Time, data []byte) (*event.Event, error) {
	if len(data) == 0 || data[len(data)-1] != '\n' {
		return nil, codec.ErrCodecEmpty
	}
	data = bytes.TrimRight(data, "\r\n")
	if len(data) == 0 {
		return nil, codec.ErrCodecEmpty
	}
	message := string(data)
	return &event.Event{
		Timestamp: time.Time(),
		Fields: map[string]interface{}{
			"type":      p.typ,
			"timestamp": time.String(),
			"message":   message,
			"host":      p.common.Hostname,
			"path":      p.path,
		},
	}, nil
}
