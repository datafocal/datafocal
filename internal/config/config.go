// Package config loads a command's settings over its defaults: a JSON file, then the
// DATAFOCAL_* environment, then flags, each layer leaving alone what it does not state.
package config

import (
	"cmp"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"os"
	"time"

	"github.com/kelseyhightower/envconfig"

	"github.com/datafocal/datafocal/internal/storage"
)

// envPrefix starts every variable name, so a field tagged TENANT is read from DATAFOCAL_TENANT.
const envPrefix = "DATAFOCAL"

// Load decodes onto cfg, a pointer to a defaulted struct, the JSON file DATAFOCAL_CONFIG_FILE
// names (defaultFile, which may be missing, when it names none), then the environment, then
// args through flags, which the caller has bound to cfg's fields.
func Load(cfg any, defaultFile string, flags *flag.FlagSet, args []string) error {
	path := cmp.Or(os.Getenv(envPrefix+"_CONFIG_FILE"), defaultFile)
	if err := loadFile(cfg, path, path != defaultFile); err != nil {
		return err
	}
	if err := envconfig.Process(envPrefix, cfg); err != nil {
		return err
	}
	return flags.Parse(args)
}

// loadFile decodes the JSON file at path onto cfg, an absent key keeping cfg's value.
func loadFile(cfg any, path string, required bool) error {
	if path == "" {
		return nil
	}
	data, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) && !required {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read config file %q: %w", path, err)
	}
	if err := json.Unmarshal(data, cfg); err != nil {
		return fmt.Errorf("parse config file %q: %w", path, err)
	}
	return nil
}

// Store is where a tenant's segments and catalog live: an fs store under Root, or an s3
// store in S3's bucket.
type Store struct {
	Kind string           `json:"store" envconfig:"STORE"`
	Root string           `json:"root" envconfig:"ROOT"`
	S3   storage.S3Config `json:"s3"`
}

// Bind registers --root on flags.
func (s *Store) Bind(flags *flag.FlagSet) {
	flags.StringVar(&s.Root, "root", s.Root, "directory of the fs store")
}

// Open returns the store Kind names, "fs" when it is empty.
func (s Store) Open() (storage.Store, error) {
	switch s.Kind {
	case "", "fs":
		if s.Root == "" {
			return nil, errors.New("root is required")
		}
		return storage.NewFS(s.Root), nil
	case "s3":
		return storage.NewS3(s.S3)
	}
	return nil, fmt.Errorf("store %q is neither fs nor s3", s.Kind)
}

// Duration is a span stated as "15s" in a file, a variable or a flag.
type Duration time.Duration

// UnmarshalText parses a Go duration, such as "15s" or "2m30s".
func (d *Duration) UnmarshalText(text []byte) error {
	v, err := time.ParseDuration(string(text))
	if err != nil {
		return fmt.Errorf("parse duration %q: %w", text, err)
	}
	*d = Duration(v)
	return nil
}

// Set parses a flag's value as UnmarshalText does.
func (d *Duration) Set(s string) error { return d.UnmarshalText([]byte(s)) }

func (d *Duration) String() string { return time.Duration(*d).String() }
