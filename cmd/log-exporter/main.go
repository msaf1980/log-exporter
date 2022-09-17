package main

import (
	"flag"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/msaf1980/log-exporter/pkg/config"
)

func main() {
	configPath := flag.String("config", "", "Path to the config file.")
	checkConfig := flag.Bool("check-config", false, "Check config file and exit.")
	debug := flag.Bool("debug", false, "sets log level to debug")

	flag.Parse()

	if *debug {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	} else {
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	}

	var (
		cfg *config.Config
		err error
	)

	if cfg, err = config.LoadConfig(*configPath); err != nil {
		log.Fatal().Err(err).Msg("load config")
	}

	if *checkConfig {
		return
	}

	_ = cfg
}
