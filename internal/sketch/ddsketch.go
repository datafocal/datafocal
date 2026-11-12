package sketch

import (
	"encoding"

	"github.com/DataDog/sketches-go/ddsketch"
	"github.com/DataDog/sketches-go/ddsketch/store"
)

// DDSketch answers quantiles with a relative-error guarantee: a thin wrapper
// over github.com/DataDog/sketches-go. For relative accuracy alpha the returned
// quantile v~ satisfies |v~ - v| <= alpha*v, which matches how latency SLOs are
// expressed. Merge is bucket-wise and foldable across segments.
type DDSketch struct {
	inner *ddsketch.DDSketch
}

// QuantileAccuracy is the relative accuracy every quantile sketch in the engine is built
// with, one constant because two sketches merge only when their accuracies match.
const QuantileAccuracy = 0.01

// NewDDSketch returns an empty sketch with the given relative accuracy
// (e.g. 0.01 for 1%). Two sketches must share alpha to be mergeable.
func NewDDSketch(relativeAccuracy float64) *DDSketch {
	s, err := ddsketch.NewDefaultDDSketch(relativeAccuracy)
	if err != nil {
		// relativeAccuracy is a fixed engine constant; a bad value is a
		// programming error, not a runtime condition.
		panic("sketch: invalid DDSketch relative accuracy: " + err.Error())
	}
	return &DDSketch{inner: s}
}

// Add folds one observation into the sketch. The library handles the full real
// line (positive/negative stores + a zero count); a value it cannot represent
// (NaN/Inf) is dropped.
func (d *DDSketch) Add(v float64) { _ = d.inner.Add(v) }

// Merge combines o into d (bucket-wise). It returns an error if the two sketches
// have incompatible index mappings (e.g. different relative accuracies) — in
// which case the library merges nothing, so the caller must not treat a failed
// merge as a completed fold.
func (d *DDSketch) Merge(o *DDSketch) error { return d.inner.MergeWith(o.inner) }

// Quantile returns the estimated value at quantile q in [0,1], or 0 for an
// empty sketch.
func (d *DDSketch) Quantile(q float64) float64 {
	if d.inner.GetCount() == 0 {
		return 0
	}
	v, err := d.inner.GetValueAtQuantile(q)
	if err != nil {
		return 0
	}
	return v
}

// MarshalBinary serializes the sketch, embedding the index mapping so decode is
// self-contained; the footer stores these bytes.
func (d *DDSketch) MarshalBinary() ([]byte, error) {
	var b []byte
	d.inner.Encode(&b, false)
	return b, nil
}

func (d *DDSketch) UnmarshalBinary(data []byte) error {
	// The mapping is embedded (Encode with omitIndexMapping=false), so a nil
	// mapping argument is decoded from the bytes.
	s, err := ddsketch.DecodeDDSketch(data, store.DefaultProvider, nil)
	if err != nil {
		return err
	}
	d.inner = s
	return nil
}

var (
	_ encoding.BinaryMarshaler   = (*DDSketch)(nil)
	_ encoding.BinaryUnmarshaler = (*DDSketch)(nil)
)
