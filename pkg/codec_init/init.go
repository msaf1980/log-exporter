package codec_init

import (
	"github.com/msaf1980/log-exporter/pkg/codec"
	"github.com/msaf1980/log-exporter/pkg/codec/line"
)

func init() {
	codec.Set("", line.New) // default codec
	codec.Set(line.Name, line.New)
}
