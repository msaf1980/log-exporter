package file

import "github.com/msaf1980/log-exporter/pkg/config"

func (in *File) Cfg() *Config {
	return &in.cfg
}

func (in *File) Common() *config.Common {
	return in.common
}
