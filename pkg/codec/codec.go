package codec

import (
	"errors"

	"github.com/msaf1980/log-exporter/pkg/config"
	"github.com/msaf1980/log-exporter/pkg/event"
	"github.com/msaf1980/log-exporter/pkg/timeutil"
	// "github.com/msaf1980/log-exporter/pkg/input/nginx_access"
)

var ErrIncomplete = errors.New("codec line incomplete")
var ErrEmpty = errors.New("codec line empty")

type Codec interface {
	Name() string
	// Parse return event, praseErrror
	//
	// can return nil without parseErr (may be part of multiline event)
	Parse(time timeutil.Time, data []byte) (*event.Event, error)
}

type Config struct {
	Type string `hcl:"type" yaml:"type"` // input type (from codecs map)
}

type codecFn func(*config.ConfigRaw, *config.Common, string) (Codec, error)

var codecs = map[string]codecFn{}

func New(cfg *config.ConfigRaw, common *config.Common, path string) (Codec, error) {
	codec := cfg.GetStringWithDefault("codec", "")

	if n, exist := codecs[codec]; exist {
		return n(cfg, common, path)
	}
	return nil, errors.New("'" + codec + "' codec not exist")
}

func Set(name string, f codecFn) {
	if _, exist := codecs[name]; exist {
		panic(errors.New("'" + name + "' codec already exist"))
	}
	codecs[name] = f
}
