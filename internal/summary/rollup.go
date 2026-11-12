package summary

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"slices"
	"time"

	"github.com/klauspost/compress/zstd"

	"github.com/datafocal/datafocal/internal/segment"
)

// Rollup is a merged stream-hour's counts and sketches per bin, which the compactor writes beside
// the merged segment so the summary reads neither the segment's footer nor its columns.
type Rollup struct {
	// BinWidth is the width it counts by; a summary of another width reads the segment instead.
	BinWidth time.Duration
	// Footer is the merged segment's footer without its row groups' indexes or segment-wide sketches.
	Footer segment.Footer
	Counts BinCounts
	Values ValueCounts
	// Sketches holds each (service, bin)'s sketches of Columns, the sketch columns it was built for.
	Sketches map[SketchKey]*SketchPoint
	Columns  []string
}

var (
	// One encoder, since the history a rollup grows an encoder to is kept: each encoder of a
	// pool would come to hold its own.
	zstdEncoder, _ = zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1))
	zstdDecoder, _ = zstd.NewReader(nil, zstd.WithDecoderConcurrency(0))
)

// BuildRollup is the rollup of the merged segment desc, whose footer the caller read, by cfg's
// bins and sketch columns.
func BuildRollup(ctx context.Context, rd segment.Reader, desc segment.Descriptor, footer segment.Footer, cfg Config) (Rollup, error) {
	cfg = cfg.withDefaults()
	r, err := countRows(ctx, rd, desc, footer, cfg.BinWidth, cfg.SketchColumns)
	footer.RowGroupZoneMaps, footer.Offsets, footer.DistinctCounts, footer.Distributions = nil, nil, nil, nil
	r.BinWidth, r.Footer, r.Columns = cfg.BinWidth, footer, cfg.SketchColumns
	return r, err
}

// Encode is the rollup's object body.
func (r Rollup) Encode() ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(r); err != nil {
		return nil, fmt.Errorf("summary: encode rollup: %w", err)
	}
	return zstdEncoder.EncodeAll(buf.Bytes(), nil), nil
}

// DecodeRollup decodes what Rollup.Encode encodes.
func DecodeRollup(body []byte) (Rollup, error) {
	var r Rollup
	raw, err := zstdDecoder.DecodeAll(body, nil)
	if err == nil {
		err = gob.NewDecoder(bytes.NewReader(raw)).Decode(&r)
	}
	if err != nil {
		return Rollup{}, fmt.Errorf("summary: decode rollup: %w", err)
	}
	return r, nil
}

// countRows counts a segment's rows per (pattern, service, bin) and per value series of each column
// its footer counts, leaving out a row without a service or a pattern as its footer's counts do, and
// sketches each (service, bin)'s values of sketchColumns.
func countRows(ctx context.Context, rd segment.Reader, desc segment.Descriptor, footer segment.Footer, width time.Duration, sketchColumns []string) (Rollup, error) {
	r := Rollup{Counts: BinCounts{}, Values: ValueCounts{}, Sketches: map[SketchKey]*SketchPoint{}}
	columns := CountedColumns(footer)
	read := slices.Concat([]string{segment.ColTimestamp, segment.ColService, segment.ColPatternID}, columns, sketchColumns)
	for g := range footer.RowGroups {
		cols, _, err := rd.Columns(ctx, desc.Object, footer, g, nil, read...)
		if err != nil {
			return Rollup{}, err
		}
		ts, services, patterns := cols[segment.ColTimestamp], cols[segment.ColService], cols[segment.ColPatternID]
		if services.Len() == 0 || patterns.Len() == 0 {
			continue
		}
		for i := range ts.Len() {
			t, ok := ts.Time(i)
			service, pattern := services.Text(i), patterns.Text(i)
			if !ok || len(service) == 0 || len(pattern) == 0 {
				continue
			}
			bin := segment.FloorBin(t.UnixMilli(), width.Milliseconds())
			r.Counts[CountKey{PatternID: string(pattern), Service: string(service), Bin: bin}]++
			countValues(r.Values, cols, columns, i, ValueKey{PatternID: string(pattern), Bin: bin})
			sketchValues(r.Sketches, cols, sketchColumns, footer, i, SketchKey{Service: string(service), Bin: bin})
		}
	}
	return r, nil
}

// sketchValues folds row i's value of each of columns into key's point, as a footer's service view
// sketches a column of its type, an id among them where footer filters it.
func sketchValues(points map[SketchKey]*SketchPoint, cols map[string]segment.ColumnChunk, columns []string, footer segment.Footer, i int, key SketchKey) {
	for _, column := range columns {
		if c := cols[column]; i < c.Len() {
			if points[key] == nil {
				points[key] = newSketchPoint(key)
			}
			view := segment.Footer{DistinctCounts: points[key].Distinct, Distributions: points[key].Quantiles}
			segment.IndexServiceColumn(&view, column, footer.ZoneMaps[column].Type, footer.HasFilter(column), []string{string(c.Text(i))})
		}
	}
}
