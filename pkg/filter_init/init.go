package filter_init

import (
	"github.com/msaf1980/log-exporter/pkg/filter"
	"github.com/msaf1980/log-exporter/pkg/filter/addfield"
	"github.com/msaf1980/log-exporter/pkg/filter/removefield"
)

func init() {
	filter.Set(addfield.Name, addfield.New)
	filter.Set(removefield.Name, removefield.New)
}
