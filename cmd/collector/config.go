package main

import (
	"flag"

	"github.com/datafocal/datafocal/internal/collector"
	"github.com/datafocal/datafocal/internal/config"
	"github.com/datafocal/datafocal/internal/ingest"
)

const defaultConfigPath = "/etc/config/config.json"

// settings is the collector process's operator surface: the collector's batching
// settings, plus where segments are written and which files are followed into them.
type settings struct {
	collector.Config
	config.Store
	Ingest ingest.Config `json:"ingest"`
}

// loadConfig layers the config file, the environment (DATAFOCAL_*) and args over the
// defaults, and validates the result.
func loadConfig(args []string) (settings, error) {
	cfg := settings{Config: collector.DefaultConfig()}
	flags := flag.NewFlagSet("collector", flag.ContinueOnError)
	flags.StringVar(&cfg.Tenant, "tenant", cfg.Tenant, "tenant the segments are written for")
	flags.StringVar(&cfg.Stream.Service, "service", cfg.Stream.Service, "stream service")
	flags.Func("labels", "comma-separated stream labels, as key=value,key=value", func(v string) error {
		labels, err := collector.ParseLabels(v)
		if err != nil {
			return err
		}
		collector.WithLabels(labels)(&cfg.Config)
		return nil
	})
	flags.DurationVar(&cfg.FlushInterval, "flush-interval", cfg.FlushInterval, "segment flush interval, such as 30s or 1m")
	flags.Int64Var(&cfg.FlushBytes, "flush-bytes", cfg.FlushBytes, "segment flush byte threshold")
	flags.IntVar(&cfg.MaxRows, "max-rows", cfg.MaxRows, "segment flush row threshold")
	cfg.Bind(flags)
	if err := config.Load(&cfg, defaultConfigPath, flags, args); err != nil {
		return settings{}, err
	}
	if err := cfg.Validate(); err != nil {
		return settings{}, err
	}
	return cfg, nil
}
