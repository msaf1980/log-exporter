package input

import (
	"context"
	"errors"

	"github.com/msaf1980/log-exporter/pkg/config"
	"github.com/msaf1980/log-exporter/pkg/event"
)

type Input interface {
	Name() string
	Start(ctx context.Context, outChan chan<- *event.Event) error
}

type Config struct {
	Type string `hcl:"type" yaml:"type"` // input type (from inputs map)
}

type InputFn func(*config.ConfigRaw, *config.Common) (Input, error)

var inputs = map[string]InputFn{}

func New(cfg *config.ConfigRaw, common *config.Common) (Input, error) {
	typ, err := cfg.GetString("type")
	if err != nil {
		return nil, err
	}

	if n, exist := inputs[typ]; exist {
		return n(cfg, common)
	}
	return nil, errors.New("'" + typ + "' input not exist")
}

func Set(name string, f InputFn) {
	if _, exist := inputs[name]; exist {
		panic(errors.New("'" + name + "' input already exist"))
	}
	inputs[name] = f
}
