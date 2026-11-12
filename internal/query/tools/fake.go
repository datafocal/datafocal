package tools

import (
	"context"

	"github.com/datafocal/datafocal/internal/query"
)

// Fake is a Tools whose methods call the matching Fn field; a nil field returns zero
// values.
type Fake struct {
	AggregateFn       func(req AggregateRequest) (query.Envelope[AggregateResponse], error)
	AttributeWritesFn func(req AttributeWritesRequest) (query.Envelope[AttributeWritesResponse], error)
	CoOccurrenceFn    func(req CoOccurrenceRequest) (query.Envelope[CoOccurrenceResponse], error)
	CorrelateFn       func(req CorrelateRequest) (query.Envelope[CorrelateResponse], error)
	ExplainSpikeFn    func(req ExplainSpikeRequest) (query.Envelope[ExplainSpikeResponse], error)
	FindAnomaliesFn   func(req FindAnomaliesRequest) (query.Envelope[FindAnomaliesResponse], error)
	FocusFn           func(req FocusRequest) (query.Envelope[FocusResponse], error)
	OverviewFn        func(req OverviewRequest) (query.Envelope[OverviewResponse], error)
	FunnelFn          func(req FunnelRequest) (query.Envelope[FunnelResponse], error)
	SearchLogsFn      func(req SearchRequest) (query.Envelope[SearchResponse], error)
	FollowSearchFn    func(req SearchRequest, emit func(query.Envelope[SearchResponse]) error) error
	RankFieldsFn      func(req FieldsRequest) (query.Envelope[FieldsResponse], error)
	DescribeSchemaFn  func(req FieldsRequest) (query.Envelope[DescribeSchemaResponse], error)
	QueryFn           func(req QueryRequest) (query.Envelope[QueryResponse], error)
	PatternsFn        func(req PatternsRequest) (query.Envelope[PatternsResponse], error)
	GetTraceFn        func(req TraceRequest) (query.Envelope[TraceResponse], error)
	TraceShapeFn      func(req TraceShapeRequest) (query.Envelope[TraceShapeResponse], error)
	ResourceUsageFn   func(req ResourceUsageRequest) (query.Envelope[ResourceUsageResponse], error)
	ServiceGraphFn    func(req ServiceGraphRequest) (query.Envelope[ServiceGraphResponse], error)
	RCAWalkFn         func(req RCAWalkRequest) (query.Envelope[RCAWalkResponse], error)

	ListSLOsFn          func(req ListSLOsRequest) (query.Envelope[ListSLOsResponse], error)
	SLOStatusFn         func(req SLOStatusRequest) (query.Envelope[SLOStatusResponse], error)
	ImportDefinitionsFn func(req ImportDefinitionsRequest) (query.Envelope[ImportDefinitionsResponse], error)
	ExportDefinitionsFn func(req ExportDefinitionsRequest) (query.Envelope[ExportDefinitionsResponse], error)
	ListViewsFn         func(req ListViewsRequest) (query.Envelope[ListViewsResponse], error)
	GetViewFn           func(req GetViewRequest) (query.Envelope[GetViewResponse], error)
	SnapshotFn          func() (string, error)
}

var _ Tools = Fake{}

func (f Fake) Aggregate(_ context.Context, req AggregateRequest) (query.Envelope[AggregateResponse], error) {
	if f.AggregateFn == nil {
		return query.Envelope[AggregateResponse]{}, nil
	}
	return f.AggregateFn(req)
}

func (f Fake) AttributeWrites(_ context.Context, req AttributeWritesRequest) (query.Envelope[AttributeWritesResponse], error) {
	if f.AttributeWritesFn == nil {
		return query.Envelope[AttributeWritesResponse]{}, nil
	}
	return f.AttributeWritesFn(req)
}

func (f Fake) CoOccurrence(_ context.Context, req CoOccurrenceRequest) (query.Envelope[CoOccurrenceResponse], error) {
	if f.CoOccurrenceFn == nil {
		return query.Envelope[CoOccurrenceResponse]{}, nil
	}
	return f.CoOccurrenceFn(req)
}

func (f Fake) Correlate(_ context.Context, req CorrelateRequest) (query.Envelope[CorrelateResponse], error) {
	if f.CorrelateFn == nil {
		return query.Envelope[CorrelateResponse]{}, nil
	}
	return f.CorrelateFn(req)
}

func (f Fake) ExplainSpike(_ context.Context, req ExplainSpikeRequest) (query.Envelope[ExplainSpikeResponse], error) {
	if f.ExplainSpikeFn == nil {
		return query.Envelope[ExplainSpikeResponse]{}, nil
	}
	return f.ExplainSpikeFn(req)
}

func (f Fake) FindAnomalies(_ context.Context, req FindAnomaliesRequest) (query.Envelope[FindAnomaliesResponse], error) {
	if f.FindAnomaliesFn == nil {
		return query.Envelope[FindAnomaliesResponse]{}, nil
	}
	return f.FindAnomaliesFn(req)
}

func (f Fake) Overview(_ context.Context, req OverviewRequest) (query.Envelope[OverviewResponse], error) {
	if f.OverviewFn == nil {
		return query.Envelope[OverviewResponse]{}, nil
	}
	return f.OverviewFn(req)
}

func (f Fake) Focus(_ context.Context, req FocusRequest) (query.Envelope[FocusResponse], error) {
	if f.FocusFn == nil {
		return query.Envelope[FocusResponse]{}, nil
	}
	return f.FocusFn(req)
}

func (f Fake) Funnel(_ context.Context, req FunnelRequest) (query.Envelope[FunnelResponse], error) {
	if f.FunnelFn == nil {
		return query.Envelope[FunnelResponse]{}, nil
	}
	return f.FunnelFn(req)
}

func (f Fake) SearchLogs(_ context.Context, req SearchRequest) (query.Envelope[SearchResponse], error) {
	if f.SearchLogsFn == nil {
		return query.Envelope[SearchResponse]{}, nil
	}
	return f.SearchLogsFn(req)
}

func (f Fake) FollowSearch(_ context.Context, req SearchRequest, emit func(query.Envelope[SearchResponse]) error) error {
	if f.FollowSearchFn == nil {
		return nil
	}
	return f.FollowSearchFn(req, emit)
}

func (f Fake) RankFields(_ context.Context, req FieldsRequest) (query.Envelope[FieldsResponse], error) {
	if f.RankFieldsFn == nil {
		return query.Envelope[FieldsResponse]{}, nil
	}
	return f.RankFieldsFn(req)
}

func (f Fake) DescribeSchema(_ context.Context, req FieldsRequest) (query.Envelope[DescribeSchemaResponse], error) {
	if f.DescribeSchemaFn == nil {
		return query.Envelope[DescribeSchemaResponse]{}, nil
	}
	return f.DescribeSchemaFn(req)
}

func (f Fake) Query(_ context.Context, req QueryRequest) (query.Envelope[QueryResponse], error) {
	if f.QueryFn == nil {
		return query.Envelope[QueryResponse]{}, nil
	}
	return f.QueryFn(req)
}

func (f Fake) Patterns(_ context.Context, req PatternsRequest) (query.Envelope[PatternsResponse], error) {
	if f.PatternsFn == nil {
		return query.Envelope[PatternsResponse]{}, nil
	}
	return f.PatternsFn(req)
}

func (f Fake) GetTrace(_ context.Context, req TraceRequest) (query.Envelope[TraceResponse], error) {
	if f.GetTraceFn == nil {
		return query.Envelope[TraceResponse]{}, nil
	}
	return f.GetTraceFn(req)
}

func (f Fake) TraceShape(_ context.Context, req TraceShapeRequest) (query.Envelope[TraceShapeResponse], error) {
	if f.TraceShapeFn == nil {
		return query.Envelope[TraceShapeResponse]{}, nil
	}
	return f.TraceShapeFn(req)
}

func (f Fake) ServiceGraph(_ context.Context, req ServiceGraphRequest) (query.Envelope[ServiceGraphResponse], error) {
	if f.ServiceGraphFn == nil {
		return query.Envelope[ServiceGraphResponse]{}, nil
	}
	return f.ServiceGraphFn(req)
}

func (f Fake) RCAWalk(_ context.Context, req RCAWalkRequest) (query.Envelope[RCAWalkResponse], error) {
	if f.RCAWalkFn == nil {
		return query.Envelope[RCAWalkResponse]{}, nil
	}
	return f.RCAWalkFn(req)
}

func (f Fake) ResourceUsage(_ context.Context, req ResourceUsageRequest) (query.Envelope[ResourceUsageResponse], error) {
	if f.ResourceUsageFn == nil {
		return query.Envelope[ResourceUsageResponse]{}, nil
	}
	return f.ResourceUsageFn(req)
}

func (f Fake) ListSLOs(_ context.Context, req ListSLOsRequest) (query.Envelope[ListSLOsResponse], error) {
	if f.ListSLOsFn == nil {
		return query.Envelope[ListSLOsResponse]{}, nil
	}
	return f.ListSLOsFn(req)
}

func (f Fake) SLOStatus(_ context.Context, req SLOStatusRequest) (query.Envelope[SLOStatusResponse], error) {
	if f.SLOStatusFn == nil {
		return query.Envelope[SLOStatusResponse]{}, nil
	}
	return f.SLOStatusFn(req)
}

func (f Fake) ImportDefinitions(_ context.Context, req ImportDefinitionsRequest) (query.Envelope[ImportDefinitionsResponse], error) {
	if f.ImportDefinitionsFn == nil {
		return query.Envelope[ImportDefinitionsResponse]{}, nil
	}
	return f.ImportDefinitionsFn(req)
}

func (f Fake) ExportDefinitions(_ context.Context, req ExportDefinitionsRequest) (query.Envelope[ExportDefinitionsResponse], error) {
	if f.ExportDefinitionsFn == nil {
		return query.Envelope[ExportDefinitionsResponse]{}, nil
	}
	return f.ExportDefinitionsFn(req)
}

func (f Fake) ListViews(_ context.Context, req ListViewsRequest) (query.Envelope[ListViewsResponse], error) {
	if f.ListViewsFn == nil {
		return query.Envelope[ListViewsResponse]{}, nil
	}
	return f.ListViewsFn(req)
}

func (f Fake) GetView(_ context.Context, req GetViewRequest) (query.Envelope[GetViewResponse], error) {
	if f.GetViewFn == nil {
		return query.Envelope[GetViewResponse]{}, nil
	}
	return f.GetViewFn(req)
}

func (f Fake) Snapshot(context.Context) (string, error) {
	if f.SnapshotFn == nil {
		return "", nil
	}
	return f.SnapshotFn()
}
