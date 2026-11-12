package tools

import (
	"context"
	"slices"
	"strings"

	"github.com/datafocal/datafocal/internal/definitions"
	"github.com/datafocal/datafocal/internal/query"
	"github.com/datafocal/datafocal/internal/selector"
	"github.com/datafocal/datafocal/internal/slo"
)

// ListSLOsRequest asks for every SLO's status, or those in State, of Service or matching
// Query, the first Limit of them, with their budget's trend when Trend is set.
type ListSLOsRequest struct {
	State    string `json:"state,omitempty" jsonschema:"ok|firing|no_data|unbound|partial"`
	Service  string `json:"service,omitempty"`
	Query    string `json:"query,omitempty" jsonschema:"text the SLO's name or service contains, ignoring case"`
	Limit    int    `json:"limit,omitempty" jsonschema:"the most SLOs to return, firing first; 0 returns every one"`
	Trend    bool   `json:"trend,omitempty" jsonschema:"include each SLO's budget remaining at 48 steps across its window, for charting"`
	Snapshot string `json:"snapshot,omitempty"`
}

// ListSLOsResponse is the SLOs' status, firing first, then pending, then by budget remaining.
type ListSLOsResponse struct {
	SLOs []slo.Status `json:"slos"`
	// Total is how many SLOs matched, of which Limit are listed.
	Total int `json:"total"`
	// Writable is whether import_definitions can save into the definitions directory.
	Writable bool `json:"writable"`
}

// SLOStatusRequest asks for one SLO's, or one of its groups', page over TimeRange, its window by default.
type SLOStatusRequest struct {
	Name      string            `json:"name"`
	Group     string            `json:"group,omitempty"`
	TimeRange selector.TimeSpec `json:"time_range,omitempty"`
	Snapshot  string            `json:"snapshot,omitempty"`
}

type SLOStatusResponse struct {
	SLO slo.Detail `json:"slo"`
}

// ImportDefinitionsRequest imports OpenSLO documents, saving them only when DryRun is false.
type ImportDefinitionsRequest struct {
	Content  string `json:"content" jsonschema:"OpenSLO v1 documents as YAML or JSON"`
	Format   string `json:"format,omitempty" jsonschema:"yaml|json, the normalized file's format, yaml by default"`
	DryRun   *bool  `json:"dry_run,omitempty" jsonschema:"false saves the documents into a writable definitions directory; true by default"`
	Snapshot string `json:"snapshot,omitempty"`
}

// ImportDefinitionsResponse classifies each document and holds the valid ones as the normalized file.
type ImportDefinitionsResponse struct {
	Documents []slo.Imported `json:"documents"`
	Content   string         `json:"content"`
	Saved     bool           `json:"saved"`
}

// ExportDefinitionsRequest exports the named documents, or every one.
type ExportDefinitionsRequest struct {
	Names  []string `json:"names,omitempty"`
	Format string   `json:"format,omitempty" jsonschema:"yaml|json, yaml by default"`
}

type ExportDefinitionsResponse struct {
	Content string `json:"content"`
}

// ListViewsRequest asks for the saved views whose name or title contains Query.
type ListViewsRequest struct {
	Query string `json:"query,omitempty" jsonschema:"text the view's name or title contains, ignoring case"`
}

// ListViewsResponse is the saved views by name, each with its panel count.
type ListViewsResponse struct {
	Views []ViewSummary `json:"views"`
	// Writable is whether import_definitions can save into the definitions directory.
	Writable bool `json:"writable"`
}

type ViewSummary struct {
	Name   string `json:"name"`
	Title  string `json:"title"`
	Panels int    `json:"panels"`
}

type GetViewRequest struct {
	Name string `json:"name"`
}

// GetViewResponse is a view's normalized document, and the view compiled with each panel's
// canonical query.
type GetViewResponse struct {
	Document map[string]any   `json:"document"`
	View     definitions.View `json:"view"`
}

// SLOStates are the states list_slos filters by.
var SLOStates = []string{slo.StateOK, slo.StateFiring, slo.StateNoData, slo.StateUnbound, slo.StatePartial}

// matches reports whether st is in req's state, of its service and matching its query.
func (req ListSLOsRequest) matches(st slo.Status) bool {
	q := strings.ToLower(req.Query)
	return (req.State == "" || st.State == req.State) && (req.Service == "" || st.Service == req.Service) &&
		(strings.Contains(strings.ToLower(st.Name), q) || strings.Contains(strings.ToLower(st.Service), q))
}

func (t toolSet) ListSLOs(ctx context.Context, req ListSLOsRequest) (query.Envelope[ListSLOsResponse], error) {
	const tool = "list_slos"
	if req.State != "" && !slices.Contains(SLOStates, req.State) {
		return query.Envelope[ListSLOsResponse]{}, query.Invalidf("%s: state %q is not one of %v", tool, req.State, SLOStates)
	}
	snap, err := t.resolveSnapshot(ctx, nil, tool, req.Snapshot)
	if err != nil {
		return query.Envelope[ListSLOsResponse]{}, err
	}
	statuses, version, err := t.SLOs.List(ctx, snap.catalog)
	if err != nil {
		return query.Envelope[ListSLOsResponse]{}, err
	}
	matched := slices.DeleteFunc(statuses, func(st slo.Status) bool { return !req.matches(st) })
	resp := ListSLOsResponse{SLOs: matched, Total: len(matched), Writable: t.SLOs.Writable()}
	if req.Limit > 0 && req.Limit < len(matched) {
		resp.SLOs = matched[:req.Limit]
	}
	if !req.Trend {
		for i := range resp.SLOs {
			resp.SLOs[i].BudgetTrend = nil
		}
	}
	coverage := 1.0
	for _, st := range resp.SLOs {
		if st.State != slo.StateUnbound {
			coverage = min(coverage, st.Coverage)
		}
	}
	return sloEnvelope(snap, version, coverage, resp), nil
}

func (t toolSet) SLOStatus(ctx context.Context, req SLOStatusRequest) (query.Envelope[SLOStatusResponse], error) {
	const tool = "slo_status"
	resolved, err := validateSelector(ctx, tool, selector.Selector{Time: req.TimeRange})
	if err != nil {
		return query.Envelope[SLOStatusResponse]{}, err
	}
	snap, err := t.resolveSnapshot(ctx, nil, tool, req.Snapshot)
	if err != nil {
		return query.Envelope[SLOStatusResponse]{}, err
	}
	detail, err := t.SLOs.Detail(ctx, snap.catalog, req.Name, req.Group, resolved.Window)
	if err != nil {
		return query.Envelope[SLOStatusResponse]{}, err
	}
	coverage := detail.Coverage
	if detail.State == slo.StateUnbound {
		coverage = 1
	}
	return sloEnvelope(snap, t.SLOs.Version(), coverage, SLOStatusResponse{SLO: detail}), nil
}

func (t toolSet) ImportDefinitions(ctx context.Context, req ImportDefinitionsRequest) (query.Envelope[ImportDefinitionsResponse], error) {
	const tool = "import_definitions"
	snap, err := t.resolveSnapshot(ctx, nil, tool, req.Snapshot)
	if err != nil {
		return query.Envelope[ImportDefinitionsResponse]{}, err
	}
	save := req.DryRun != nil && !*req.DryRun
	imported, err := t.SLOs.Import(ctx, snap.catalog, []byte(req.Content), save)
	if err != nil {
		return query.Envelope[ImportDefinitionsResponse]{}, err
	}
	var docs []definitions.Document
	for _, im := range imported {
		if im.Document != nil {
			docs = append(docs, definitions.Document{Raw: im.Document})
		}
	}
	content, err := definitions.Format(docs, req.Format)
	if err != nil {
		return query.Envelope[ImportDefinitionsResponse]{}, query.Invalidf("%s: %v", tool, err)
	}
	resp := ImportDefinitionsResponse{Documents: imported, Content: string(content), Saved: save && len(docs) > 0}
	return sloEnvelope(snap, t.SLOs.Version(), 1, resp), nil
}

func (t toolSet) ExportDefinitions(ctx context.Context, req ExportDefinitionsRequest) (query.Envelope[ExportDefinitionsResponse], error) {
	const tool = "export_definitions"
	snap, err := t.resolveSnapshot(ctx, nil, tool, "")
	if err != nil {
		return query.Envelope[ExportDefinitionsResponse]{}, err
	}
	content, err := t.SLOs.Export(req.Names, req.Format)
	if err != nil {
		return query.Envelope[ExportDefinitionsResponse]{}, err
	}
	return sloEnvelope(snap, t.SLOs.Version(), 1, ExportDefinitionsResponse{Content: string(content)}), nil
}

func (t toolSet) ListViews(ctx context.Context, req ListViewsRequest) (query.Envelope[ListViewsResponse], error) {
	snap, err := t.resolveSnapshot(ctx, nil, "list_views", "")
	if err != nil {
		return query.Envelope[ListViewsResponse]{}, err
	}
	resp := ListViewsResponse{Views: []ViewSummary{}, Writable: t.SLOs.Writable()}
	q := strings.ToLower(req.Query)
	for _, v := range t.SLOs.Views() {
		if strings.Contains(strings.ToLower(v.Name), q) || strings.Contains(strings.ToLower(v.Title), q) {
			resp.Views = append(resp.Views, ViewSummary{Name: v.Name, Title: v.Title, Panels: len(v.Panels)})
		}
	}
	return sloEnvelope(snap, t.SLOs.Version(), 1, resp), nil
}

func (t toolSet) GetView(ctx context.Context, req GetViewRequest) (query.Envelope[GetViewResponse], error) {
	snap, err := t.resolveSnapshot(ctx, nil, "get_view", "")
	if err != nil {
		return query.Envelope[GetViewResponse]{}, err
	}
	doc, err := t.SLOs.View(req.Name)
	if err != nil {
		return query.Envelope[GetViewResponse]{}, err
	}
	return sloEnvelope(snap, t.SLOs.Version(), 1, GetViewResponse{Document: doc.Raw, View: *doc.View}), nil
}

func sloEnvelope[T any](snap snapshot, definitions string, coverage float64, data T) query.Envelope[T] {
	return query.Envelope[T]{Data: data, Meta: query.Meta{Snapshot: snap.String(), Definitions: definitions, Coverage: coverage, Tokens: query.EstimateTokens(data)}}
}
