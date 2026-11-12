package slo

import (
	"time"

	"github.com/datafocal/datafocal/internal/selector"
)

// States a status is in.
const (
	StateOK      = "ok"
	StateFiring  = "firing"
	StateNoData  = "no_data"
	StateUnbound = "unbound"
	// StatePartial is an ok or no_data read before the window is fully counted.
	StatePartial = "partial"
)

// Status is an SLO's, or one of its groups', state and budget at a time.
type Status struct {
	Name    string `json:"name"`
	Service string `json:"service,omitempty"`
	Group   string `json:"group,omitempty"`
	State   string `json:"state"`
	// Pending is set when a condition holds and has not yet held for its alertAfter.
	Pending bool `json:"pending,omitempty"`
	// Since is when the state began: a firing condition's firing, or a pending one's holding.
	Since           *time.Time `json:"since,omitempty"`
	Objective       float64    `json:"objective"`
	SLI             *float64   `json:"sli,omitempty"`
	BudgetRemaining *float64   `json:"budget_remaining,omitempty"`
	BurnRate1h      *float64   `json:"burn_rate_1h,omitempty"`
	BurnRate6h      *float64   `json:"burn_rate_6h,omitempty"`
	// Coverage is the fraction of the window's hours counted, below 1 until backfill is done.
	Coverage float64 `json:"coverage"`
	// WindowStart is where the window's counts begin, on an hour past the fine horizon.
	WindowStart time.Time         `json:"window_start,omitzero"`
	Conditions  []ConditionStatus `json:"conditions,omitempty"`
	// Forecast is where the budget is heading at its recent burn.
	Forecast *Forecast `json:"forecast,omitempty"`
	Groups   []Status  `json:"groups,omitempty"`
	// BudgetTrend is list_slos' budget remaining at even steps across the window, each over
	// the window ending there, null where it held no valid events.
	BudgetTrend []*float64 `json:"budget_trend,omitempty"`
	// Untracked is how many groups past slo.max_groups or slo.max_series fold into the group "untracked".
	Untracked int `json:"untracked,omitempty"`
	// Query is an unbound SLO's original query.
	Query string `json:"query,omitempty"`
}

// Forecast is the budget a horizon on at the burn over twice the horizon before: its burn
// rate, the budget remaining it leaves, and when it would spend the budget, at an even rate of
// events. Absent values are for a window with no events in that span.
type Forecast struct {
	Horizon string `json:"horizon"`
	// Lookback is the span the burn is read over, twice the horizon.
	Lookback        string     `json:"lookback"`
	BurnRate        *float64   `json:"burn_rate,omitempty"`
	BudgetRemaining *float64   `json:"budget_remaining,omitempty"`
	ExhaustedAt     *time.Time `json:"exhausted_at,omitempty"`
}

// ConditionStatus is one alert condition's state, its burn rates over its two windows at the time.
type ConditionStatus struct {
	Policy    string     `json:"policy"`
	Name      string     `json:"name,omitempty"`
	Severity  string     `json:"severity,omitempty"`
	State     string     `json:"state" jsonschema:"ok|pending|firing"`
	Since     *time.Time `json:"since,omitempty"`
	Threshold float64    `json:"threshold"`
	// Lookback and ShortWindow are the condition's two windows, as 1h or 5m.
	Lookback      string   `json:"lookback"`
	ShortWindow   string   `json:"short_window,omitempty"`
	BurnRate      *float64 `json:"burn_rate,omitempty"`
	ShortBurnRate *float64 `json:"short_burn_rate,omitempty"`
}

// Detail is one SLO's, or one group's, status with its series over a range.
type Detail struct {
	Status `tstype:",extends"`
	// Points are the SLI, budget remaining and each condition's burn rate at each step's end.
	Points []Point `json:"points"`
	// BadEvents selects the bad events: valid with good's added predicates negated.
	BadEvents []selector.Predicate `json:"bad_events,omitempty"`
}

// Point is a Detail's values at one time.
type Point struct {
	At time.Time `json:"at"`
	// Good and Valid are the step's own events, or slices, ending at At: what a chart of the
	// bad ones over time reads, at any range, without a scan.
	Good            int64      `json:"good"`
	Valid           int64      `json:"valid"`
	SLI             *float64   `json:"sli,omitempty"`
	BudgetRemaining *float64   `json:"budget_remaining,omitempty"`
	BurnRates       []*float64 `json:"burn_rates" tstype:"(number | null)[]"`
}

// Imported is one imported document, how it compares with the directory's, and its status
// over the last 24 hours.
type Imported struct {
	// Document is the normalized document, absent for an invalid one; the import's response
	// holds them as one file.
	Document map[string]any `json:"-"`
	Kind     string         `json:"kind,omitempty"`
	Name     string         `json:"name,omitempty"`
	Version  string         `json:"version,omitempty"`
	Class    string         `json:"class" jsonschema:"new|changed|unchanged|unbound|invalid"`
	Notes    []string       `json:"notes,omitempty"`
	Preview  *Status        `json:"preview,omitempty"`
	// Panels are an imported View's panel kinds, in order.
	Panels []string `json:"panels,omitempty"`
	Error  string   `json:"error,omitempty"`
}
