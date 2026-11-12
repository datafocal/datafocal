package config

import (
	"flag"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testSettings struct {
	Name  string   `json:"name" envconfig:"NAME"`
	Every Duration `json:"every" envconfig:"EVERY"`
	Store
}

// load is Load over a testSettings defaulted to name "default" every second, with a
// --name flag.
func load(t *testing.T, args ...string) (testSettings, error) {
	cfg := testSettings{Name: "default", Every: Duration(time.Second)}
	flags := flag.NewFlagSet("test", flag.ContinueOnError)
	flags.StringVar(&cfg.Name, "name", cfg.Name, "")
	cfg.Bind(flags)
	err := Load(&cfg, filepath.Join(t.TempDir(), "absent.json"), flags, args)
	return cfg, err
}

// TestLoadLayers: each layer overrides the ones below it and keeps what it does not state.
func TestLoadLayers(t *testing.T) {
	file := filepath.Join(t.TempDir(), "config.json")
	require.NoError(t, os.WriteFile(file, []byte(`{"name": "file", "every": "1m", "root": "/file"}`), 0o600))

	got, err := load(t)
	require.NoError(t, err)
	assert.Equal(t, testSettings{Name: "default", Every: Duration(time.Second)}, got, "an absent default file")

	t.Setenv("DATAFOCAL_CONFIG_FILE", file)
	got, err = load(t)
	require.NoError(t, err)
	assert.Equal(t, testSettings{Name: "file", Every: Duration(time.Minute), Store: Store{Root: "/file"}}, got)

	t.Setenv("DATAFOCAL_NAME", "env")
	t.Setenv("DATAFOCAL_EVERY", "1h")
	got, err = load(t, "--name", "flag")
	require.NoError(t, err)
	assert.Equal(t, testSettings{Name: "flag", Every: Duration(time.Hour), Store: Store{Root: "/file"}}, got)

	t.Setenv("DATAFOCAL_CONFIG_FILE", filepath.Join(t.TempDir(), "missing.json"))
	_, err = load(t)
	assert.ErrorContains(t, err, "read config file", "a file the environment names must exist")
}

// TestStoreOpen opens an fs store under its root, and refuses one with no root or a kind
// it does not know.
func TestStoreOpen(t *testing.T) {
	_, err := Store{Root: t.TempDir()}.Open()
	require.NoError(t, err)
	_, err = Store{Kind: "fs"}.Open()
	assert.EqualError(t, err, "root is required")
	_, err = Store{Kind: "gcs"}.Open()
	assert.EqualError(t, err, `store "gcs" is neither fs nor s3`)
}
