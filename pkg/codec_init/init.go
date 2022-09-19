package codec_init

import (
	"github.com/msaf1980/log-exporter/pkg/codec"
	"github.com/msaf1980/log-exporter/pkg/codec/line"
)

func init() {
	codec.Set(line.Name, line.New)
}
