package pipeline

import (
	"context"

	"github.com/msaf1980/log-exporter/pkg/config"
	"github.com/msaf1980/log-exporter/pkg/event"
	"github.com/msaf1980/log-exporter/pkg/input"
)

type Pipeline struct {
	inputs []input.Input

	fchan chan *event.Event
	ochan chan *event.Event
}

func New(ctx context.Context, common *config.Common, inputs []config.ConfigRaw, filters []config.ConfigRaw, outputs []config.ConfigRaw) (*Pipeline, error) {
	p := &Pipeline{
		inputs: make([]input.Input, 0, len(inputs)),
		// filters: filters,
		// outputs: outputs,

		fchan: make(chan *event.Event, 10*len(inputs)),
		ochan: make(chan *event.Event, 10*len(inputs)),
	}
	for i := range inputs {
		if in, err := input.New(&inputs[i], common); err == nil {
			p.inputs = append(p.inputs, in)
		} else {
			return nil, err
		}
	}

	return p, nil
}
