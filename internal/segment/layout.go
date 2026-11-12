package segment

// ByteRange locates a contiguous span within a segment object. It is what turns
// a column-chunk read into a single ranged GET instead of a full-object read.
type ByteRange struct {
	Offset int64
	Length int64
}

// ChunkKey identifies one column chunk of one row group within a segment.
type ChunkKey struct {
	RowGroup int
	Column   string
}

// OffsetTable maps (row group, column) to the byte range of that column chunk
// inside the segment object. It is the footer index a reader consults to fetch
// exactly the bytes a query needs.
type OffsetTable map[ChunkKey]ByteRange

// Get returns the byte range for a column chunk, if present.
func (t OffsetTable) Get(rowGroup int, column string) (ByteRange, bool) {
	r, ok := t[ChunkKey{RowGroup: rowGroup, Column: column}]
	return r, ok
}

// Set records the byte range for a column chunk.
func (t OffsetTable) Set(rowGroup int, column string, r ByteRange) {
	t[ChunkKey{RowGroup: rowGroup, Column: column}] = r
}

// PresentSet is the set of values known to be present in a segment (e.g. the
// pattern_id present-set in the footer). A negative membership test lets the
// planner skip a segment without any data GET.
type PresentSet map[string]struct{}

// NewPresentSet returns an empty, ready-to-use PresentSet.
func NewPresentSet() PresentSet {
	return make(PresentSet)
}

// Add records that value is present.
func (s PresentSet) Add(value string) {
	s[value] = struct{}{}
}

// Contains reports whether value was recorded as present.
func (s PresentSet) Contains(value string) bool {
	_, ok := s[value]
	return ok
}
