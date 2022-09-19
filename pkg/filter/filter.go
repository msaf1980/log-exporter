package filter

import (
	"errors"

	"github.com/msaf1980/log-exporter/pkg/config"
	"github.com/msaf1980/log-exporter/pkg/event"
)

type Filter interface {
	Name() string
	Start(inChan <-chan *event.Event, outChan chan<- *event.Event) error
}

type Config struct {
	Type string `hcl:"type" yaml:"type"` // filter type (from filters map)
}

type FilterFn func(*config.ConfigRaw, *config.Common) (Filter, error)

var filters = map[string]FilterFn{}

func New(cfg *config.ConfigRaw, common *config.Common) (Filter, error) {
	typ, err := cfg.GetString("type")
	if err != nil {
		return nil, err
	}

	if n, exist := filters[typ]; exist {
		return n(cfg, common)
	}
	return nil, errors.New("'" + typ + "' filter not exist")
}

func Set(name string, f FilterFn) {
	if _, exist := filters[name]; exist {
		panic(errors.New("'" + name + "' filter already exist"))
	}
	filters[name] = f
}
