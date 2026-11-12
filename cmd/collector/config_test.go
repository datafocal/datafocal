package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datafocal/datafocal/internal/collector"
	"github.com/datafocal/datafocal/internal/config"
	"github.com/datafocal/datafocal/internal/ingest"
	"github.com/datafocal/datafocal/internal/storage"
)

func TestLoadConfigPrecedence(t *testing.T) {
	// Unset rather than empty, since a set variable overrides the file even when empty.
	for _, key := range []string{"ROOT", "TENANT", "STREAM_LABELS", "FLUSH_INTERVAL", "FLUSH_BYTES", "MAX_ROWS", "FLUSH_ALIGNMENT", "INGEST_HOST"} {
		t.Setenv("DATAFOCAL_"+key, "")
		require.NoError(t, os.Unsetenv("DATAFOCAL_"+key))
	}
	path := filepath.Join(t.TempDir(), "config.json")
	require.NoError(t, os.WriteFile(path, []byte(`{"tenant":"shop","stream":{"service":"from-file"},"max_rows":12,"flush_alignment":60000000000,
		"root":"/data","ingest":{"sources":[{"path":"/var/log/app.log"}],"state_dir":"/state","host":"file-host"},"store":"s3","s3":{"endpoint":"from-file","bucket":"segments"}}`), 0600))
	t.Setenv("DATAFOCAL_CONFIG_FILE", path)
	t.Setenv("DATAFOCAL_S3_ENDPOINT", "minio:9000")
	t.Setenv("DATAFOCAL_SEAL_RETRY", "2s")
	for _, tc := range []struct {
		name          string
		env           string
		args          []string
		service, root string
		host          string
	}{
		{name: "file overrides default", service: "from-file", root: "/data", host: "file-host"},
		{name: "environment overrides file", env: "payments", service: "payments", root: "/data", host: "payments"},
		{name: "flags override environment", env: "payments", args: []string{"--service", "checkout", "--root", "/flag"}, service: "checkout", root: "/flag", host: "payments"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("DATAFOCAL_STREAM_SERVICE", tc.env)
			t.Setenv("DATAFOCAL_INGEST_HOST", tc.env)
			if tc.env == "" {
				require.NoError(t, os.Unsetenv("DATAFOCAL_STREAM_SERVICE"))
				require.NoError(t, os.Unsetenv("DATAFOCAL_INGEST_HOST"))
			}
			got, err := loadConfig(tc.args)
			require.NoError(t, err)
			want := settings{Config: collector.DefaultConfig(), Ingest: ingest.Config{Sources: []ingest.Source{{Path: "/var/log/app.log"}}, StateDir: "/state", Host: tc.host},
				Store: config.Store{Kind: "s3", Root: tc.root, S3: storage.S3Config{Endpoint: "minio:9000", Bucket: "segments"}}}
			want.Tenant, want.MaxRows, want.FlushAlignment = "shop", 12, time.Minute
			want.Stream.Service = tc.service
			want.SealRetry = 2 * time.Second
			assert.Equal(t, want, got)
		})
	}
	t.Setenv("DATAFOCAL_CONFIG_FILE", filepath.Join(t.TempDir(), "missing.json"))
	_, err := loadConfig(nil)
	require.ErrorContains(t, err, "read config file")
}
