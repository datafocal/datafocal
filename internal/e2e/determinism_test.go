package e2e

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datafocal/datafocal/internal/seedcorpus"
	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/value"
)

// TestSeedIsDeterministic proves the seed corpus reproduces byte-for-byte
// across two independent builds: every .seg object is identical, and the
// per-tenant manifest is deep-equal once Object.Bucket and Object.URI (which
// legitimately embed the output-dir absolute path) are normalized. This is
// the acceptance criterion "Segment-byte determinism asserted across two
// seeds; manifest compared with path fields normalized."
func TestSeedIsDeterministic(t *testing.T) {
	// The first corpus already exists (TestMain built it into corpusDir). Build
	// a second one into a separate temp dir so both trees live side by side
	// during the comparison.
	dir2, err := os.MkdirTemp("", "datafocal-e2e-det-*")
	require.NoError(t, err, "mkdirtemp for second build")
	t.Cleanup(func() { _ = os.RemoveAll(dir2) })

	other, err := seedcorpus.Build(context.Background(), dir2, seedcorpus.ScenarioAll)
	require.NoError(t, err, "second seedcorpus.Build")

	// Same number of descriptors is the first-order determinism check; without
	// it the per-descriptor compare would either miss extra segments in one or
	// index past the end of the other.
	require.Equalf(t, len(buildResult.Descriptors), len(other.Descriptors),
		"two seed builds produced different descriptor counts (%d vs %d)",
		len(buildResult.Descriptors), len(other.Descriptors))

	// Manifest comparison: same descriptors in the same order, once the path
	// fields on ObjectLocation are stripped (Bucket is the output dir and URI
	// is file:// + full path, both of which legitimately differ).
	for i, want := range buildResult.Descriptors {
		got := other.Descriptors[i]
		normalized := normalizeDescriptorPaths(want)
		otherNormalized := normalizeDescriptorPaths(got)
		require.Equalf(t, normalized, otherNormalized,
			"descriptor %d diverged between two seed builds after path normalization", i)
	}

	// Patterns map — same set of pattern ids → pattern strings.
	assert.Equal(t, buildResult.Patterns, other.Patterns,
		"pattern ids or pattern strings diverged between two seed builds")

	// Per-.seg byte compare. Every .seg file in the primary corpus must exist
	// in the second corpus at the same tenant-prefixed key, with identical bytes.
	assertSegmentsEqual(t, corpusDir, dir2)
}

// TestBuilderIsDeterministicForShuffledColumns checks the contract the corpus
// test alone cannot isolate: the same logical record must seal to identical
// bytes even when its input columns arrive in a different order.
func TestBuilderIsDeterministicForShuffledColumns(t *testing.T) {
	base := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
	event := segment.LogRecord(base, "info", "request completed", map[string]string{
		"service":  "checkout",
		"trace_id": "trace-0001",
	})

	seal := func(cols []segment.Column) []byte {
		builder := segment.NewBuilder(segment.BuilderConfig{
			Tenant:    tenant,
			Stream:    segment.Stream{Service: "checkout"},
			CreatedAt: base,
		})
		parsed := segment.ParsedPattern{
			PatternID: "tmpl-1",
			Pattern:   "request <*> completed",
			Columns:   cols,
		}
		require.NoError(t, builder.Append(event, parsed))
		sealed := builder.Seal()
		obj, err := sealed.Encode()
		require.NoError(t, err)
		return obj
	}

	left := seal([]segment.Column{
		{Name: "duration_ms", Type: value.TypeInt, Value: value.Value{Int: 42}},
		{Name: "attempt", Type: value.TypeInt, Value: value.Value{Int: 3}},
	})
	right := seal([]segment.Column{
		{Name: "attempt", Type: value.TypeInt, Value: value.Value{Int: 3}},
		{Name: "duration_ms", Type: value.TypeInt, Value: value.Value{Int: 42}},
	})

	require.Equal(t, left, right)
}

// normalizeDescriptorPaths returns d with the two absolute-path fields on
// ObjectLocation zeroed. Bucket embeds the output root and URI embeds the full
// file path; both are correct outputs of the storage layer, but they are the
// only thing that varies between two independent builds and comparing them
// would defeat the point of the test.
func normalizeDescriptorPaths(d segment.Descriptor) segment.Descriptor {
	d.Object.Bucket = ""
	d.Object.URI = ""
	return d
}

// assertSegmentsEqual walks aRoot for .seg files, computes each file's path
// relative to aRoot, and compares its bytes to the file at the same relative
// path under bRoot. Segments are content-addressed under a
// tenant/YYYY/MM/DD/HH/fingerprint/<id>.seg key by BuildSegmentKey, so the two
// trees must have the same relative-path set for the same inputs.
func assertSegmentsEqual(t *testing.T, aRoot, bRoot string) {
	t.Helper()

	var relPaths []string
	err := filepath.WalkDir(aRoot, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(d.Name(), ".seg") {
			return nil
		}
		rel, err := filepath.Rel(aRoot, path)
		if err != nil {
			return err
		}
		relPaths = append(relPaths, rel)
		return nil
	})
	require.NoError(t, err, "walk primary corpus for .seg files")
	require.NotEmpty(t, relPaths, "primary corpus contains no .seg files")

	for _, rel := range relPaths {
		aBytes, err := os.ReadFile(filepath.Join(aRoot, rel))
		require.NoErrorf(t, err, "read primary segment %s", rel)
		bBytes, err := os.ReadFile(filepath.Join(bRoot, rel))
		require.NoErrorf(t, err, "read second-build segment %s (must exist at same key)", rel)
		require.Equalf(t, aBytes, bBytes,
			"segment %s bytes diverged between two seed builds (len %d vs %d)",
			rel, len(aBytes), len(bBytes))
	}
}
