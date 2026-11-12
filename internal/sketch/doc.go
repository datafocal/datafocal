// Package sketch is the shared, mergeable-sketch codec imported by BOTH the
// collector (write path) and the querier (read path). Because a version
// mismatch between writer and reader would silently corrupt every aggregate,
// there is exactly ONE encode/decode implementation and it lives here.
//
// Two sketches are provided, each fixed-size, incrementally updatable, and
// mergeable so the query plane can compute global aggregates by combining
// per-segment footers without reading data blocks:
//
//   - HLL      — count-distinct; merge is register-wise max.
//   - DDSketch — quantiles with a relative-error guarantee; merge is
//     bucket-wise sum.
//
// Each type implements encoding.BinaryMarshaler / encoding.BinaryUnmarshaler;
// that binary form IS the on-disk footer representation shared by both sides.
package sketch
