package web

import (
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// rawColor matches a colour written as a literal rather than through a token.
var rawColor = regexp.MustCompile(`#[0-9a-fA-F]{3,8}\b|\b(?:rgba?|hsla?|hwb|lab|lch|oklab|oklch|color)\(`)

// TestCSSUsesTokens holds every stylesheet to the design tokens: only the token file may
// write a colour literal.
func TestCSSUsesTokens(t *testing.T) {
	err := filepath.WalkDir(".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() && (d.Name() == "node_modules" || d.Name() == "dist") {
			return filepath.SkipDir
		}
		if d.IsDir() || filepath.Ext(path) != ".css" || path == "packages/ui/src/tokens.css" {
			return nil
		}
		css, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for i, line := range strings.Split(string(css), "\n") {
			assert.False(t, rawColor.MatchString(line), "%s:%d: raw colour %q; use a token from tokens.css",
				path, i+1, rawColor.FindString(line))
		}
		return nil
	})
	require.NoError(t, err)
}
