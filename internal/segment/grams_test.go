package segment

import (
	"context"
	"testing"
	"time"

	"github.com/datafocal/datafocal/internal/storage"
	"github.com/datafocal/datafocal/internal/value"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGramsAdmitEverySubstring(t *testing.T) {
	cfg := BuilderConfig{Tenant: "test", CreatedAt: time.Now()}
	b := NewBuilder(cfg)
	w := NewWriter(cfg, map[string]value.Type{ColMessage: value.TypeString, ColTimestamp: value.TypeTimestamp})
	w.(*streamWriter).cells = 2
	for _, message := range []string{"slow query 123", "Mixed CASE café 世界", "xy"} {
		r := LogRecord(cfg.CreatedAt, "", message, nil)
		require.NoError(t, b.Append(r, ParsedPattern{}))
		require.NoError(t, w.Append(r, ParsedPattern{}))
	}
	sealed, err := b.Seal().Encode()
	require.NoError(t, err)
	written, _, err := w.Close(PatternProfile{})
	require.NoError(t, err)
	for _, body := range [][]byte{sealed, written} {
		store := storage.NewFS(t.TempDir())
		ctx, loc := context.Background(), ObjectLocation{Key: "seg"}
		require.NoError(t, store.Put(ctx, loc.Key, body))
		rd := NewReader(store)
		footer, _, err := rd.Footer(ctx, loc)
		require.NoError(t, err)
		grams, _, err := rd.Grams(ctx, loc, footer)
		require.NoError(t, err)
		for group := range footer.RowGroups {
			rows, _, err := rd.RowGroupRecords(ctx, loc, footer, group)
			require.NoError(t, err)
			for _, row := range rows {
				texts := textsOf([]column{{name: ColMessage, typ: value.TypeString}}, tableOf([]Record{row}))[0].texts
				for _, message := range texts {
					for lo := range len(message) {
						for hi := lo + 3; hi <= len(message); hi++ {
							assert.True(t, grams.Admits(group, message[lo:hi]))
						}
					}
				}
			}
			assert.False(t, grams.Admits(group, "zzzzzz"))
			assert.False(t, grams.Admits(group, "SLOW QUERY"))
		}
		filters, err := footer.Filters(body)
		require.NoError(t, err)
		assert.NotContains(t, filters, GramsSection)
	}
}

func TestGramsCapAndEmpty(t *testing.T) {
	empty := buildGrams([]string{"", "ab"})
	assert.False(t, (Grams{empty}).Admits(0, "abc"))
	assert.True(t, Grams(nil).Admits(0, "abc"))
	messages := make([]string, 65537)
	for i := range messages {
		messages[i] = string([]byte{byte(i >> 16), byte(i >> 8), byte(i)})
	}
	capped := buildGrams(messages)
	assert.Nil(t, capped)
	assert.True(t, (Grams{capped}).Admits(0, "anything"))
}
