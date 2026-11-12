package tools

import (
	"context"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"

	"github.com/datafocal/datafocal/internal/catalog"
	"github.com/datafocal/datafocal/internal/query"
	"github.com/datafocal/datafocal/internal/query/lang"
	"github.com/datafocal/datafocal/internal/query/plan"
	"github.com/datafocal/datafocal/internal/query/scan"
	"github.com/datafocal/datafocal/internal/query/series"
	"github.com/datafocal/datafocal/internal/slo"
	"github.com/datafocal/datafocal/internal/storage"
	"github.com/datafocal/datafocal/internal/summary"
)

// Tools is the query tool surface: one method per tool, each a request, then a plan,
// then the summary or the scan, then a response.
type Tools interface {
	Aggregate(ctx context.Context, req AggregateRequest) (query.Envelope[AggregateResponse], error)
	AttributeWrites(ctx context.Context, req AttributeWritesRequest) (query.Envelope[AttributeWritesResponse], error)
	CoOccurrence(ctx context.Context, req CoOccurrenceRequest) (query.Envelope[CoOccurrenceResponse], error)
	Correlate(ctx context.Context, req CorrelateRequest) (query.Envelope[CorrelateResponse], error)
	ExplainSpike(ctx context.Context, req ExplainSpikeRequest) (query.Envelope[ExplainSpikeResponse], error)
	FindAnomalies(ctx context.Context, req FindAnomaliesRequest) (query.Envelope[FindAnomaliesResponse], error)
	Focus(ctx context.Context, req FocusRequest) (query.Envelope[FocusResponse], error)
	Overview(ctx context.Context, req OverviewRequest) (query.Envelope[OverviewResponse], error)
	Funnel(ctx context.Context, req FunnelRequest) (query.Envelope[FunnelResponse], error)
	SearchLogs(ctx context.Context, req SearchRequest) (query.Envelope[SearchResponse], error)
	FollowSearch(ctx context.Context, req SearchRequest, emit func(query.Envelope[SearchResponse]) error) error
	RankFields(ctx context.Context, req FieldsRequest) (query.Envelope[FieldsResponse], error)
	DescribeSchema(ctx context.Context, req FieldsRequest) (query.Envelope[DescribeSchemaResponse], error)
	Query(ctx context.Context, req QueryRequest) (query.Envelope[QueryResponse], error)
	Patterns(ctx context.Context, req PatternsRequest) (query.Envelope[PatternsResponse], error)
	GetTrace(ctx context.Context, req TraceRequest) (query.Envelope[TraceResponse], error)
	TraceShape(ctx context.Context, req TraceShapeRequest) (query.Envelope[TraceShapeResponse], error)
	ServiceGraph(ctx context.Context, req ServiceGraphRequest) (query.Envelope[ServiceGraphResponse], error)
	RCAWalk(ctx context.Context, req RCAWalkRequest) (query.Envelope[RCAWalkResponse], error)
	ResourceUsage(ctx context.Context, req ResourceUsageRequest) (query.Envelope[ResourceUsageResponse], error)
	ListSLOs(ctx context.Context, req ListSLOsRequest) (query.Envelope[ListSLOsResponse], error)
	SLOStatus(ctx context.Context, req SLOStatusRequest) (query.Envelope[SLOStatusResponse], error)
	ImportDefinitions(ctx context.Context, req ImportDefinitionsRequest) (query.Envelope[ImportDefinitionsResponse], error)
	ExportDefinitions(ctx context.Context, req ExportDefinitionsRequest) (query.Envelope[ExportDefinitionsResponse], error)
	ListViews(ctx context.Context, req ListViewsRequest) (query.Envelope[ListViewsResponse], error)
	GetView(ctx context.Context, req GetViewRequest) (query.Envelope[GetViewResponse], error)
	// Snapshot names the snapshot a call pinning none reads now.
	Snapshot(ctx context.Context) (string, error)
}

// Deps is what the tools read through.
type Deps struct {
	// Catalog resolves the current snapshot for a request that pins none.
	Catalog catalog.Catalog
	// Series answers every measure over bins.
	Series  series.Reader
	Planner plan.Planner
	Scanner scan.Scanner
	// Summary is the configured summary or nil, read here only for its shape; its
	// contents are read through Planner.
	Summary summary.Store
	// Languages are the query languages the query tool parses.
	Languages lang.Registry
	// Store holds the segment objects the live tail reads whole.
	Store storage.Store
	// SLOs are the definitions' SLOs and their counts.
	SLOs slo.Store
}

type toolSet struct {
	Deps
	tail *tail
	// fieldsCache keeps scanned segments' field stats, shared by every copy of the set.
	fieldsCache *lru.Cache[fieldsCacheKey, segmentFields]
	// columns, when set, are the only attributes a search reads beside a line's own fields.
	columns []string
}

// New returns the tools reading through deps.
func New(deps Deps) Tools {
	// Every 5s, the catalog's default refresh.
	tl := &tail{catalog: deps.Catalog, store: deps.Store, every: 5 * time.Second, followers: map[chan []LogLine]<-chan struct{}{}}
	return toolSet{Deps: deps, tail: tl, fieldsCache: newFieldsCache()}
}
