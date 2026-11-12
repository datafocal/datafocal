package collector

import (
	"errors"
	"fmt"
	"maps"
	"strings"
	"time"
)

const (
	defaultTenant        = "local"
	defaultService       = "demo"
	defaultFlushInterval = time.Minute
	defaultFlushBytes    = int64(50 << 20)
	defaultMaxRows       = 250_000
	// defaultFlushAlignment is what every deployment sets (deploy/k8s/base, deploy/local): a
	// backfill of days unaligned seals as one segment, which the summary refuses for its bins.
	defaultFlushAlignment = time.Hour
)

type Config struct {
	Tenant        string        `json:"tenant" envconfig:"TENANT"`
	Stream        StreamConfig  `json:"stream"`
	FlushInterval time.Duration `json:"flush_interval" envconfig:"FLUSH_INTERVAL"`
	FlushBytes    int64         `json:"flush_bytes" envconfig:"FLUSH_BYTES"`
	MaxRows       int           `json:"max_rows" envconfig:"MAX_ROWS"`
	// FlushAlignment seals a segment before it spans a boundary of this width, so no
	// sealed segment straddles a summary bin; it defaults to an hour, and zero disables it
	// (FlushPolicy.AlignTo).
	FlushAlignment time.Duration `json:"flush_alignment" envconfig:"FLUSH_ALIGNMENT"`
	// SealRetry bounds upload and commit retries; zero uses 10 minutes.
	SealRetry time.Duration `json:"seal_retry" envconfig:"SEAL_RETRY"`
	// SealInBackground seals a cut batch on its own goroutine while the next fills, for a
	// caller that saves its read position from CollectResult.Cut and the Descriptors that
	// arrive later rather than taking every cut as durable on return.
	SealInBackground bool `json:"-" ignored:"true"`
}

// StreamConfig is the configured stream identity as it binds from JSON and the
// environment. It is the config-file shape, not the domain type — segment.Stream
// is what the write path actually carries.
type StreamConfig struct {
	Service string            `json:"service" envconfig:"SERVICE"`
	Labels  map[string]string `json:"labels" envconfig:"LABELS"`
}

type Option func(*Config)

func WithTenant(tenant string) Option {
	return func(cfg *Config) {
		cfg.Tenant = tenant
	}
}

func WithService(service string) Option {
	return func(cfg *Config) {
		cfg.Stream.Service = service
	}
}

func WithLabels(labels map[string]string) Option {
	return func(cfg *Config) {
		if cfg.Stream.Labels == nil {
			cfg.Stream.Labels = make(map[string]string, len(labels))
		}
		maps.Copy(cfg.Stream.Labels, labels)
	}
}

func WithFlushInterval(interval time.Duration) Option {
	return func(cfg *Config) {
		cfg.FlushInterval = interval
	}
}

// WithFlushBytes seals on the uncompressed record payload the builder estimates
// (segment.Stats.EstimatedBytes), which bounds memory rather than the object's size.
func WithFlushBytes(bytes int64) Option {
	return func(cfg *Config) {
		cfg.FlushBytes = bytes
	}
}

// WithFlushAlignment sets the bucket width segments are sealed on.
func WithFlushAlignment(d time.Duration) Option {
	return func(cfg *Config) {
		cfg.FlushAlignment = d
	}
}

func WithSealRetry(d time.Duration) Option {
	return func(cfg *Config) { cfg.SealRetry = d }
}

func WithMaxRows(rows int) Option {
	return func(cfg *Config) {
		cfg.MaxRows = rows
	}
}

func DefaultConfig() Config {
	return Config{
		Tenant: defaultTenant,
		Stream: StreamConfig{
			Service: defaultService,
			Labels:  map[string]string{"env": "dev"},
		},
		FlushInterval:  defaultFlushInterval,
		FlushBytes:     defaultFlushBytes,
		MaxRows:        defaultMaxRows,
		FlushAlignment: defaultFlushAlignment,
	}
}

func (c Config) Validate() error {
	if c.Tenant == "" {
		return errors.New("tenant is required")
	}
	if c.Stream.Service == "" {
		return errors.New("stream service is required")
	}
	if c.FlushInterval <= 0 {
		return errors.New("flush interval must be positive")
	}
	if c.FlushBytes <= 0 {
		return errors.New("flush bytes must be positive")
	}
	if c.MaxRows <= 0 {
		return errors.New("max rows must be positive")
	}
	// FlushPolicy buckets by whole milliseconds, so a finer width would divide by zero.
	if c.FlushAlignment != 0 && c.FlushAlignment < time.Millisecond {
		return errors.New("flush alignment must be zero or at least 1ms")
	}
	return nil
}

func (c Config) FlushPolicy() FlushPolicy {
	return FlushPolicy{MaxAge: c.FlushInterval, MaxBytes: c.FlushBytes, MaxRows: c.MaxRows, AlignTo: c.FlushAlignment}
}

func ParseLabels(value string) (map[string]string, error) {
	labels := make(map[string]string)
	if strings.TrimSpace(value) == "" {
		return labels, nil
	}

	for part := range strings.SplitSeq(value, ",") {
		key, val, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok || key == "" {
			return nil, fmt.Errorf("label %q must be key=value", part)
		}
		labels[key] = val
	}
	return labels, nil
}
