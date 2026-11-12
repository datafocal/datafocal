// Package slo keeps each SLO's good and valid counts per hour of segments, as they land, and
// decides its status from them.
package slo

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/datafocal/datafocal/internal/catalog"
	"github.com/datafocal/datafocal/internal/definitions"
	"github.com/datafocal/datafocal/internal/query"
	"github.com/datafocal/datafocal/internal/query/series"
	"github.com/datafocal/datafocal/internal/selector"
)

// Config bounds the store; a zero field takes its default.
type Config struct {
	// Retention bounds an SLO's window, as it bounds what the querier reads; 0 bounds none.
	Retention time.Duration
	// FineHorizon is how far back counts stay in 1-minute bins rather than hours.
	FineHorizon time.Duration
	// MaxSeries caps the groups the grouped SLOs track together, and MaxGroups the groups of
	// one SLO; an ungrouped SLO always tracks its one series.
	MaxSeries, MaxGroups int
	// BackfillBytes is the segment bytes one Refresh reads to count hours not yet counted.
	BackfillBytes int64
}

// Defaults of Config.
const (
	DefaultFineHorizon         = 24 * time.Hour
	DefaultMaxGroups           = 500
	DefaultMaxSeries           = 4 * DefaultMaxGroups
	DefaultBackfillBytes int64 = 256 << 20
)

// Limits are the bounds a definition read under cfg must fit.
func (cfg Config) Limits() definitions.Limits {
	return definitions.Limits{Retention: cfg.Retention, FineHorizon: cmp.Or(cfg.FineHorizon, DefaultFineHorizon)}
}

// Store keeps the SLOs' counts and answers their status.
type Store interface {
	// Refresh counts the segments the catalog gained, and backfills within BackfillBytes.
	Refresh(ctx context.Context) error
	// Watch reloads the definitions whenever their directory changes, until ctx is done.
	Watch(ctx context.Context) error
	// List is every SLO's status at v, firing first, and the definitions version.
	List(ctx context.Context, v catalog.Version) ([]Status, string, error)
	// Detail is the named SLO's, or its group's, status and series over w.
	Detail(ctx context.Context, v catalog.Version, name, group string, w selector.Window) (Detail, error)
	// Import classifies content's documents against the directory's, saving them when save is set.
	Import(ctx context.Context, v catalog.Version, content []byte, save bool) ([]Imported, error)
	// Export writes the documents named by kind/name or by a name of any kind, or every
	// one, in format.
	Export(names []string, format string) ([]byte, error)
	// Views are the directory's saved views, by name.
	Views() []definitions.View
	// View is the named view's document, or a not-found error.
	View(name string) (definitions.Document, error)
	Writable() bool
	// Version is the version of the definitions loaded.
	Version() string
}

type store struct {
	dir definitions.Directory
	cat catalog.Catalog
	rd  series.Reader
	cfg Config

	// mu serializes loading, counting and reading the counts.
	mu      sync.Mutex
	docs    []definitions.Document
	version string
	slos    []*tracked // by name
	// gen counts changes to the SLOs or their cells; fresh and listed hold only at one gen.
	gen uint64
	// fresh is the last snapshot refreshed whole, which a read at the same catalog version
	// and gen reuses rather than walking every SLO's hours again.
	fresh *fresh
	// listed is every SLO's status at one snapshot time and gen, computed once for them.
	listed *listed
}

// fresh is a snapshot refreshed at catalog version v when the cells were at gen.
type fresh struct {
	v    catalog.Version
	gen  uint64
	snap snapshot
}

// listed is the SLOs' statuses at time at and gen, listed in order, and the groups each
// SLO, in name order, keeps under MaxSeries and MaxGroups.
type listed struct {
	at       time.Time
	gen      uint64
	statuses []Status
	keeps    []int
}

// tracked is an SLO and its cells by hour, which an SLO counting the same SLI inherits.
type tracked struct {
	def   definitions.SLO
	sli   string
	cells map[int64]cell
}

// cell is a Cell, the sorted IDs of the segments it counted, and whether its groups are
// collapsed to one bin each.
type cell struct {
	Cell
	ids    []string
	coarse bool
}

// New returns a Store over the definitions in dir, loaded now, counting the catalog's
// segments through rd.
func New(dir definitions.Directory, cat catalog.Catalog, rd series.Reader, cfg Config) Store {
	cfg.FineHorizon = cmp.Or(cfg.FineHorizon, DefaultFineHorizon)
	cfg.MaxSeries, cfg.MaxGroups = cmp.Or(cfg.MaxSeries, DefaultMaxSeries), cmp.Or(cfg.MaxGroups, DefaultMaxGroups)
	cfg.BackfillBytes = cmp.Or(cfg.BackfillBytes, DefaultBackfillBytes)
	s := &store{dir: dir, cat: cat, rd: rd, cfg: cfg}
	s.load()
	return s
}

func (s *store) Writable() bool { return s.dir.Writable() }

func (s *store) Version() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.version
}

func (s *store) Watch(ctx context.Context) error {
	return s.dir.Watch(ctx, func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.load()
	})
}

// load swaps in the directory's definitions whole, an SLO whose SLI is unchanged keeping its cells.
func (s *store) load() {
	docs, errs := s.dir.Load()
	for _, e := range errs {
		slog.Default().Warn("definition rejected", "path", e.Path, "name", e.Name, "field", e.Field, "error", e.Message)
	}
	old := s.slos
	s.docs, s.slos = docs, nil
	s.gen++
	h := sha256.New()
	for _, d := range docs {
		_, _ = fmt.Fprintf(h, "%s/%s/%s\n", d.Kind, d.Name, d.Version)
		if d.SLO != nil {
			s.slos = append(s.slos, track(*d.SLO, old))
		}
	}
	slices.SortFunc(s.slos, func(a, b *tracked) int { return strings.Compare(a.def.Name, b.def.Name) })
	s.version = hex.EncodeToString(h.Sum(nil)[:8])
}

// track is def, sharing the cells of the SLO in old that counts the same SLI.
func track(def definitions.SLO, old []*tracked) *tracked {
	b, _ := json.Marshal([]any{def.Name, def.Method, def.SliceWindow, def.Op, def.Value, def.GroupBy, def.Total, def.Good, def.Bad, def.Metric})
	t := &tracked{def: def, sli: string(b), cells: map[int64]cell{}}
	if i := slices.IndexFunc(old, func(o *tracked) bool { return o.sli == t.sli }); i >= 0 {
		t.cells = old[i].cells
	}
	return t
}

func (s *store) List(ctx context.Context, v catalog.Version) ([]Status, string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	snap, err := s.refresh(ctx, v, false)
	if err != nil {
		return nil, "", err
	}
	return slices.Clone(s.listing(snap).statuses), s.version, nil
}

// listing is every SLO's status at snap, firing first, with its budget's trend, and the
// groups each keeps; computed once for a snapshot time and gen of the counts.
func (s *store) listing(snap snapshot) *listed {
	if l := s.listed; l != nil && l.at.Equal(snap.at) && l.gen == s.gen {
		return l
	}
	folded, untracked, keeps := s.folded(snap)
	out := make([]Status, len(s.slos))
	for i, t := range s.slos {
		out[i] = folded[i].evaluate(t.def, snap.at)
		out[i].Untracked = untracked[i]
		out[i].BudgetTrend = folded[i].total.trend(t.def, snap.at)
	}
	sortByHealth(out)
	s.listed = &listed{at: snap.at, gen: s.gen, statuses: out, keeps: keeps}
	return s.listed
}

// sortByHealth orders firing first, then pending, then by budget remaining, one without data last.
func sortByHealth(out []Status) {
	rank := func(st Status) int {
		switch {
		case st.State == StateFiring:
			return 0
		case st.Pending:
			return 1
		}
		return 2
	}
	// Then by budget remaining, one without data last.
	budget := func(st Status) float64 { return *cmp.Or(st.BudgetRemaining, ptr(2.0)) }
	slices.SortStableFunc(out, func(a, b Status) int {
		return cmp.Or(cmp.Compare(rank(a), rank(b)), cmp.Compare(budget(a), budget(b)))
	})
}

func (s *store) Detail(ctx context.Context, v catalog.Version, name, group string, w selector.Window) (Detail, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	snap, err := s.refresh(ctx, v, false)
	if err != nil {
		return Detail{}, err
	}
	i := slices.IndexFunc(s.slos, func(t *tracked) bool { return t.def.Name == name })
	if i < 0 {
		return Detail{}, query.Invalidf("slo_status: no SLO is named %q", name)
	}
	t := s.slos[i]
	if !w.To.IsZero() {
		snap.at = w.To.UTC().Truncate(time.Minute)
	}
	from := cmp.Or(w.From, t.def.Window.From(snap.at))
	// Only this SLO's lines: the groups it keeps under the caps, which the SLOs before it
	// decide, come from the listing.
	keep := s.listing(snap).keeps[i]
	l := newLines(s.cells(t, snap, min(t.def.Window.From(snap.at).UnixMilli(), t.def.Window.From(from).UnixMilli())))
	_, untracked := l.fold(keep)
	d := Detail{Status: l.evaluate(t.def, snap.at), BadEvents: badEvents(t.def)}
	d.Untracked = untracked
	line := l.total
	if group != "" {
		j := slices.IndexFunc(d.Groups, func(st Status) bool { return st.Group == group })
		if j < 0 {
			return Detail{}, query.Invalidf("slo_status: SLO %q has no group %q", name, group)
		}
		d.Status, line = d.Groups[j], l.groups[group]
	}
	step := time.Minute
	if snap.at.Sub(from) > s.cfg.FineHorizon {
		step = time.Hour
	}
	for at := from.Truncate(step).Add(step); !at.After(snap.at); at = at.Add(step) {
		d.Points = append(d.Points, line.point(t.def, at, step))
	}
	return d, nil
}

// folded is the SLOs' lines over each one's window to snap's time, keeping one group of an
// ungrouped SLO and otherwise MaxGroups of what MaxSeries leaves after the grouped SLOs before it;
// how many each folded, and how many each kept.
func (s *store) folded(snap snapshot) (folded []lines, untracked, keeps []int) {
	left := s.cfg.MaxSeries
	n := len(s.slos)
	folded, untracked, keeps = make([]lines, n), make([]int, n), make([]int, n)
	for i, t := range s.slos {
		from := t.def.Window.From(snap.at).UnixMilli()
		keep := 1
		if t.def.GroupBy != "" {
			keep = min(s.cfg.MaxGroups, left)
		}
		var groups int
		keeps[i] = keep
		folded[i] = newLines(s.cells(t, snap, from))
		groups, untracked[i] = folded[i].fold(keep)
		if t.def.GroupBy != "" {
			left = max(left-min(keep, groups), 0)
		}
	}
	return folded, untracked, keeps
}

// negations are each operator's opposite, for the bad-event selector.
var negations = map[selector.Op]selector.Op{
	selector.OpEq: selector.OpNe, selector.OpNe: selector.OpEq, selector.OpLt: selector.OpGe, selector.OpGe: selector.OpLt,
	selector.OpLe: selector.OpGt, selector.OpGt: selector.OpLe, selector.OpMatch: selector.OpNoMatch, selector.OpNoMatch: selector.OpMatch,
	selector.OpRegex: selector.OpNoRegex, selector.OpNoRegex: selector.OpRegex,
}

// badEvents is an event ratio's valid with good's added predicates negated, or its bad query;
// for a time-slice SLO over a field's quantile, its events whose field is past the slice
// threshold, the events that push a slice bad; nil for any other time-slice SLO, or a good
// query an Any group cannot negate.
func badEvents(def definitions.SLO) []selector.Predicate {
	if def.Metric != nil {
		return pastThreshold(def)
	}
	if def.Total == nil || def.Bad != nil {
		return cmp.Or(def.Bad, &selector.Query{}).Where
	}
	var negated []selector.Predicate
	for _, p := range added(def) {
		op, ok := negations[p.Op]
		if !ok || len(p.Any) > 0 {
			return nil
		}
		p.Op = op
		negated = append(negated, p)
	}
	if len(negated) == 1 {
		return append(slices.Clone(def.Total.Where), negated[0])
	}
	return append(slices.Clone(def.Total.Where), selector.Predicate{Any: negated})
}

// sliceBad is the field comparison that fails each time-slice threshold op.
var sliceBad = map[string]selector.Op{"lt": selector.OpGe, "lte": selector.OpGt, "gt": selector.OpLe, "gte": selector.OpLt}

// pastThreshold is a time-slice SLO's events whose unwrapped field fails its threshold when
// the slice's value is a quantile of that field; nil for a count or a sum, which no one event fails.
func pastThreshold(def definitions.SLO) []selector.Predicate {
	if def.Metric.Stage == nil {
		return nil
	}
	name, field, _ := strings.Cut(def.Metric.Stage.Measure, ":")
	op, known := sliceBad[def.Op]
	if _, err := strconv.ParseFloat(strings.TrimPrefix(name, "p"), 64); !strings.HasPrefix(name, "p") || err != nil || field == "" || !known {
		return nil
	}
	return append(slices.Clone(def.Metric.Where), selector.Predicate{Field: field, Op: op, Value: strconv.FormatFloat(def.Value, 'f', -1, 64)})
}

// added is the predicates an event ratio's good or bad query adds to its total's.
func added(def definitions.SLO) []selector.Predicate {
	return slices.DeleteFunc(slices.Clone(cmp.Or(def.Good, def.Bad).Where), func(p selector.Predicate) bool {
		return slices.ContainsFunc(def.Total.Where, func(t selector.Predicate) bool { return reflect.DeepEqual(t, p) })
	})
}

func (s *store) Import(ctx context.Context, v catalog.Version, content []byte, save bool) ([]Imported, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	snap, err := s.refresh(ctx, v, false)
	if err != nil {
		return nil, err
	}
	docs, errs := definitions.Parse(content, s.docs, s.cfg.Limits())
	var out []Imported
	for _, d := range docs {
		im := Imported{Document: d.Raw, Kind: d.Kind, Name: d.Name, Version: d.Version, Notes: d.Notes, Class: s.classify(d), Panels: panelKinds(d.View)}
		if d.SLO != nil {
			preview, err := s.preview(ctx, snap, *d.SLO)
			if err != nil {
				return nil, err
			}
			im.Preview = &preview
		}
		out = append(out, im)
	}
	for _, e := range errs {
		out = append(out, Imported{Name: e.Name, Class: "invalid", Error: e.Error()})
	}
	if !save || len(docs) == 0 {
		return out, nil
	}
	if err := s.dir.Save(docs); err != nil {
		return nil, query.Invalidf("import_definitions: %v", err)
	}
	s.load()
	return out, nil
}

// panelKinds are v's panels' kinds, a View's import preview.
func panelKinds(v *definitions.View) []string {
	var kinds []string
	for _, p := range cmp.Or(v, &definitions.View{}).Panels {
		kinds = append(kinds, p.Kind)
	}
	return kinds
}

// classify is d as new, changed or unchanged against the directory's documents, or unbound.
func (s *store) classify(d definitions.Document) string {
	i := slices.IndexFunc(s.docs, func(o definitions.Document) bool { return o.Kind == d.Kind && o.Name == d.Name })
	switch {
	case d.SLO != nil && d.SLO.Unbound:
		return StateUnbound
	case i < 0:
		return "new"
	case s.docs[i].Version == d.Version:
		return "unchanged"
	}
	return "changed"
}

// preview is def's status over the 24 hours to snap's time, from the cells of an SLO
// counting the same SLI, and a read of the hours they lack.
func (s *store) preview(ctx context.Context, snap snapshot, def definitions.SLO) (Status, error) {
	if def.Unbound {
		return Evaluate(def, nil, snap.at), nil
	}
	def.Window = definitions.Window{Rolling: true, Duration: 24 * time.Hour}
	t, from := track(def, s.slos), hourOf(def.Window.From(snap.at))
	for h, hr := range snap.hours {
		if h >= from && !slices.Equal(t.cells[h].ids, hr.ids) {
			if err := s.read(ctx, snap.v, h, hr, []*tracked{t}, false); err != nil {
				return Status{}, err
			}
		}
	}
	keep := 1
	if def.GroupBy != "" {
		keep = s.cfg.MaxGroups
	}
	l := newLines(s.cells(t, snap, from))
	_, untracked := l.fold(keep)
	st := l.evaluate(def, snap.at)
	st.Untracked = untracked
	return st, nil
}

func (s *store) Export(names []string, format string) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	named := func(name string) func(definitions.Document) bool {
		return func(d definitions.Document) bool { return name == d.Name || name == d.Kind+"/"+d.Name }
	}
	var picked []definitions.Document
	for _, d := range s.docs {
		if len(names) == 0 || slices.ContainsFunc(names, func(name string) bool { return named(name)(d) }) {
			picked = append(picked, d)
		}
	}
	for _, name := range names {
		if !slices.ContainsFunc(picked, named(name)) {
			return nil, query.Invalidf("export_definitions: no definition is named %q", name)
		}
	}
	content, err := definitions.Format(picked, format)
	if err != nil {
		return nil, query.Invalidf("export_definitions: %v", err)
	}
	return content, nil
}

func (s *store) Views() []definitions.View {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []definitions.View
	for _, d := range s.docs {
		if d.View != nil {
			out = append(out, *d.View)
		}
	}
	slices.SortFunc(out, func(a, b definitions.View) int { return strings.Compare(a.Name, b.Name) })
	return out
}

func (s *store) View(name string) (definitions.Document, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	i := slices.IndexFunc(s.docs, func(d definitions.Document) bool { return d.View != nil && d.Name == name })
	if i < 0 {
		return definitions.Document{}, query.Invalidf("get_view: no view is named %q", name)
	}
	return s.docs[i], nil
}
