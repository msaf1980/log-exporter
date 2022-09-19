package stdout

import (
	"fmt"

	"github.com/msaf1980/log-exporter/pkg/config"
	"github.com/msaf1980/log-exporter/pkg/event"
	"github.com/msaf1980/log-exporter/pkg/output"
)

const Name = "stdout"

type Stdout struct {
}

func New(cfg *config.ConfigRaw, common *config.Common) (output.Output, error) {
	return &Stdout{}, nil
}

func (o *Stdout) Name() string {
	return Name
}

func (o *Stdout) Parse(e *event.Event) (err error) {
	_, err = fmt.Printf("%#v\n", *e)
	return
}
