package addfield

import (
	"errors"
	"strings"

	"github.com/msaf1980/go-stringutils"
	"github.com/msaf1980/log-exporter/pkg/config"
	"github.com/msaf1980/log-exporter/pkg/event"
	"github.com/msaf1980/log-exporter/pkg/filter"
	"github.com/rs/zerolog/log"
)

const Name = "add_field"

type ft struct {
	value     string
	templated bool
	tpl       stringutils.Template
}

type Config struct {
	filter.Config

	Fields    map[string]string `hcl:"fields" yaml:"fields" json:"fields"`
	templated map[string]ft     `hcl:"-" yaml:"-" json:"-"`
}

func defaultConfig() Config {
	return Config{
		Config:    filter.Config{Type: Name},
		templated: map[string]ft{},
	}
}

// AddField is filter for append fields.
//
// Support template: like "%{host} %{timestamp}""
type AddField struct {
	cfg    Config
	cfgRaw *config.ConfigRaw
	common *config.Common
}

func New(cfg *config.ConfigRaw, common *config.Common) (filter.Filter, error) {
	fi := &AddField{
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
	for k, v := range fi.cfg.Fields {
		f := ft{value: v}
		if strings.Contains(v, "%{") {
			f.templated = true
			if f.tpl, err = stringutils.InitTemplate(f.value); err != nil {
				log.Error().Str("config", fi.common.Config).Str("input", fi.cfg.Type).Str("field", "format").Err(err).Msg("template init")
				return nil, err
			}
		}
		fi.cfg.templated[k] = f
	}

	return fi, nil
}

func (fi *AddField) Name() string {
	return Name
}

func (fi *AddField) Parse(e *event.Event) (err error) {
	for k, f := range fi.cfg.templated {
		if f.templated {
			s, part := f.tpl.ExecutePartial(e.Fields)
			if part {
				s = strings.ReplaceAll(s, "%{timestamp}", e.Timestamp.String())
				if strings.Contains(s, "%{") {
					err = filter.ErrPartExpand
				}
			}
			e.Fields[k] = s
		} else {
			e.Fields[k] = f.value
		}
	}
	return
}
