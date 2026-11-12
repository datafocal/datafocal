// Package stack runs the shipped binaries end to end, from seeded log files through the
// collector and querier processes over MinIO; its tests need docker, so they sit behind
// the stack build tag, which make e2e and make bench-e2e set. This file carries no tag,
// so the package still builds for the tools that vet every package; nor does cost.go,
// which reads the benchmark's reports.
package stack
