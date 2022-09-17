package input_init

import (
	"github.com/msaf1980/log-exporter/pkg/input"
	"github.com/msaf1980/log-exporter/pkg/input/file"
)

func init() {
	input.Set(file.Name, file.New)
}
