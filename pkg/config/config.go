package config

import (
	"errors"
	"os"

	"github.com/icza/dyno"
	json "github.com/json-iterator/go"
)

type ConfigRaw map[string]interface{}

func (r ConfigRaw) GetString(name string) (string, error) {
	if v, exist := r[name]; exist {
		if s, ok := v.(string); ok {
			return s, nil
		}
		return "", errors.New("'" + name + "' node not a string")
	}
	return "", errors.New("'" + name + "' node not exist")
}

func (r ConfigRaw) GetStringWithDefault(name, def string) string {
	if v, exist := r[name]; exist {
		if s, ok := v.(string); ok {
			return s
		}
		return def
	}
	return def
}

func (r ConfigRaw) Decode(conf interface{}) error {
	data, err := json.Marshal(dyno.ConvertMapI2MapS(r))
	if err != nil {
		return err
	}
	if err = json.Unmarshal(data, conf); err != nil {
		return err
	}

	return err
}

type Common struct {
	Hostname string `hcl:"hostname" yaml:"hostname" json:"hostname"`
	Config   string `hcl:"-" yaml:"-" json:"-"`
}

type Config struct {
	Inputs []ConfigRaw `hcl:"input" yaml:"input" json:"input"`
	Common Common      `hcl:"common" yaml:"common" json:"common"`
}

func LoadConfig(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	cfg := &Config{}
	if err = json.Unmarshal(b, cfg); err != nil {
		return nil, err
	}
	if cfg.Common.Hostname == "" {
		if cfg.Common.Hostname, err = os.Hostname(); err != nil {
			return nil, err
		}
	}
	cfg.Common.Config = path

	return cfg, nil
}
