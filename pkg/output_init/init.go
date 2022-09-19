package output_init

import (
	"github.com/msaf1980/log-exporter/pkg/output"
	"github.com/msaf1980/log-exporter/pkg/output/stdout"
)

func init() {
	output.Set(stdout.Name, stdout.New)
}
