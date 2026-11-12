package tools

import (
	"encoding/json"

	lru "github.com/hashicorp/golang-lru/v2"

	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/selector"
	"github.com/datafocal/datafocal/internal/sketch"
)

// maxCachedSegmentFields bounds the segments' field stats kept, each a selector's columns of
// one segment: a few kilobytes, so a few megabytes in all.
const maxCachedSegmentFields = 4096

// segmentFields is one segment's stats of the fields of the rows a selector matched: the
// rows matched, and the columns they held.
type segmentFields struct {
	accs map[string]*fieldAcc
	held map[string]bool
	rows int
}

// fieldsCacheKey is a segment's field stats under one selector. A segment never changes,
// so an entry is never stale.
type fieldsCacheKey struct{ segment, selector string }

func newFieldsCache() *lru.Cache[fieldsCacheKey, segmentFields] {
	c, _ := lru.New[fieldsCacheKey, segmentFields](maxCachedSegmentFields) // errs only for a size below 1
	return c
}

// segmentFieldsKey is what a segment's field stats depend on besides the segment: the
// selector's predicates and the derived columns' definitions.
func segmentFieldsKey(sel selector.Resolved, derived string) string {
	where, _ := json.Marshal(sel.Where) // predicates marshal whatever they hold
	return derived + "\x00" + string(where)
}

// wholly reports whether every row of d is inside w, so the rows w matches in d are all of
// those its predicates do.
func wholly(w selector.Window, d segment.Descriptor) bool {
	return w.Contains(d.TimeRange.Min) && w.Contains(d.TimeRange.Max)
}

// mergeAccs folds src's accumulators into dst's, leaving src's as they were.
func mergeAccs(dst, src map[string]*fieldAcc) {
	for name, s := range src {
		d := accFor(dst, name)
		d.present += s.present
		d.lines += s.lines
		d.hasDDSketch = d.hasDDSketch || s.hasDDSketch
		d.filtered, d.unfiltered = d.filtered || s.filtered, d.unfiltered || s.unfiltered
		if d.valueType == 0 {
			d.valueType = s.valueType
		}
		if s.hll != nil {
			if d.hll == nil {
				d.hll = sketch.NewHLL()
			}
			_ = d.hll.Merge(s.hll) // never errs (sketch.HLL.Merge)
		}
	}
}
