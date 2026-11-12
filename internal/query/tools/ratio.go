package tools

import (
	"github.com/datafocal/datafocal/internal/query"
	"github.com/datafocal/datafocal/internal/query/series"
	"github.com/datafocal/datafocal/internal/selector"
)

// isRatio parses the measure, since "ratio:duration_ms" is a ratio a name compare
// misses.
func isRatio(measure string) bool {
	m, err := series.ParseMeasure(measure)
	return err == nil && m.Kind == series.KindRatio
}

// validateNumerator refuses the two mismatches that would otherwise be answered: an
// empty numerator, which makes the ratio a constant 1.0, and a numerator on any other
// measure, which would be silently dropped. tool names the caller because aggregate
// takes the numerator in its own field and correlate takes it in a series' where.
func validateNumerator(tool string, kind series.Kind, numerator []selector.Predicate) error {
	ratio := series.KindRatio.String()
	if kind == series.KindRatio && len(numerator) == 0 {
		return query.Invalidf("%s: measure %q needs numerator predicates; the denominator is "+
			"the selector scope, so an empty numerator is 1.0 in every bin", tool, ratio)
	}
	if kind != series.KindRatio && len(numerator) > 0 {
		return query.Invalidf("%s: numerator predicates apply only to measure %q; "+
			"narrow the selector instead", tool, ratio)
	}
	return nil
}
