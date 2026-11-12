package sketch

// Hash64 is the single 64-bit hash shared across the engine — the sketches, the
// segment membership filter, and the collector's pattern ids. It is FNV-1a
// (stdlib semantics, deterministic across processes) computed inline with no
// per-call allocation, finished with a splitmix64 finalizer because FNV's
// avalanche is weak in exactly the high bits consumers rely on.
//
// This one function is deliberately ours: it is on the hot path and trivially
// small, and keeping it fixed makes hashing consistent across every component.
func Hash64(b []byte) uint64 {
	const (
		fnvOffset uint64 = 14695981039346656037
		fnvPrime  uint64 = 1099511628211
	)
	x := fnvOffset
	for _, c := range b {
		x ^= uint64(c)
		x *= fnvPrime
	}
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}
