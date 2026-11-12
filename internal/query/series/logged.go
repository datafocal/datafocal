package series

import (
	"context"
	"log/slog"
	"time"
)

// Logged is r logging each read as a step of the request it serves: how long it took, what it
// asked for and what it read.
func Logged(r Reader) Reader { return logged{r} }

type logged struct{ Reader }

func (l logged) Read(ctx context.Context, req Request) (Result, error) {
	start := time.Now()
	res, err := l.Reader.Read(ctx, req)
	attrs := []any{
		"duration_ms", float64(time.Since(start).Microseconds()) / 1000,
		"measure", req.Measure.Kind.String(), "group_by", req.GroupBy, "measures", len(req.Measures),
		"bins", req.Bins.NumBins(), "segments", res.Cost.Segments, "gets", res.Cost.GETs,
		"bytes_read", res.Cost.BytesRead, "footers_decoded", res.Cost.FootersDecoded,
	}
	if err != nil {
		attrs = append(attrs, "error", err.Error())
	}
	slog.InfoContext(ctx, "series read", attrs...)
	return res, err
}
