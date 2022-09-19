package output_init

import (
	"github.com/msaf1980/log-exporter/pkg/filter"
	"github.com/msaf1980/log-exporter/pkg/output/stdout"
)

func init() {
	filter.Set(stdout.Name, stdout.New)
}
