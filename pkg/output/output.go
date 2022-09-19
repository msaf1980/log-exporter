package output

import (
	"errors"

	"github.com/msaf1980/log-exporter/pkg/config"
	"github.com/msaf1980/log-exporter/pkg/event"
)

type Output interface {
	Name() string
	Parse(e *event.Event) error
}

type Config struct {
	Type string `hcl:"type" yaml:"type"` // output type (from outputs map)
}

type OutputFn func(*config.ConfigRaw, *config.Common) (Output, error)

var outputs = map[string]OutputFn{}

func New(cfg *config.ConfigRaw, common *config.Common) (Output, error) {
	typ, err := cfg.GetString("type")
	if err != nil {
		return nil, err
	}

	if n, exist := outputs[typ]; exist {
		return n(cfg, common)
	}
	return nil, errors.New("'" + typ + "' output not exist")
}

func Set(name string, f OutputFn) {
	if _, exist := outputs[name]; exist {
		panic(errors.New("'" + name + "' output already exist"))
	}
	outputs[name] = f
}
