package tools

import "github.com/datafocal/datafocal/internal/query/plan"

var (
	ExportGroupFromSubject    = groupFromSubject
	ExportGroupsPlacedOn      = groupsPlacedOn
	ExportAttribution         = attribution
	ExportDensify             = densify
	ExportDescriptorGroupKey  = plan.SegmentValue
	ExportEntityRelationFor   = entityRelationFor
	ExportEntityOwned         = entityOwned
	ExportEntityIndeterminate = entityIndeterminate
	ExportSpanName            = spanName
)
