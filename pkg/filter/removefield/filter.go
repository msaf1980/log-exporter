package removefield

import (
	"errors"

	"github.com/msaf1980/go-stringutils"
	"github.com/msaf1980/log-exporter/pkg/config"
	"github.com/msaf1980/log-exporter/pkg/event"
	"github.com/msaf1980/log-exporter/pkg/filter"
)

const Name = "remove_field"

type ft struct {
	value     string
	templated bool
	tpl       stringutils.Template
}

type Config struct {
	filter.Config

	Fields []string `hcl:"fields" yaml:"fields" json:"fields"`
}

func defaultConfig() Config {
	return Config{
		Config: filter.Config{Type: Name},
	}
}

// RemoveField is filter for append fields.
//
// Support template: like "%{host} %{timestamp}""
type RemoveField struct {
	cfg    Config
	cfgRaw *config.ConfigRaw
	common *config.Common
}

func New(cfg *config.ConfigRaw, common *config.Common) (filter.Filter, error) {
	fi := &RemoveField{
		cfg:    defaultConfig(),
		cfgRaw: cfg,
		common: common,
	}

	var err error
	if err = cfg.Decode(&fi.cfg); err != nil {
		return nil, err
	}

	if len(fi.cfg.Fields) == 0 {
		return nil, errors.New("filter '" + fi.cfg.Type + "': fields not set")
	}

	return fi, nil
}

func (fi *RemoveField) Name() string {
	return Name
}

func (fi *RemoveField) Start(inChan <-chan *event.Event, outChan chan<- *event.Event) error {
	for e := range inChan {
		for _, f := range fi.cfg.Fields {
			delete(e.Fields, f)
		}
		outChan <- e
	}
	return nil
}
