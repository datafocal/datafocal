package slo

import (
	"maps"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/datafocal/datafocal/internal/definitions"
)

var hour0 = time.Date(2026, 7, 27, 0, 0, 0, 0, time.UTC)

// hourCell is the cell of hour h after hour0 whose group's minute m holds bad(m) bad of 8 events.
func hourCell(h int, group string, bad func(m time.Time) int64) Cell {
	c := Counts{Good: make([]int64, 60), Valid: make([]int64, 60)}
	for m := range 60 {
		c.Valid[m] = 8
		c.Good[m] = 8 - bad(hour0.Add(time.Duration(h)*time.Hour+time.Duration(m)*time.Minute))
	}
	return Cell{Hour: hour0.Add(time.Duration(h) * time.Hour).UnixMilli(), Groups: map[string]Counts{group: c}}
}

// grouped is a's and b's cells of each hour as one cell.
func grouped(a, b []Cell) []Cell {
	for i := range a {
		maps.Copy(a[i].Groups, b[i].Groups)
	}
	return a
}

func TestEvaluate(t *testing.T) {
	at := hour0.Add(3 * time.Hour)
	// badSince marks every event bad from the minute d before at, and none before.
	badSince := func(d time.Duration) func(time.Time) int64 {
		return func(m time.Time) int64 {
			if m.Before(at.Add(-d)) {
				return 0
			}
			return 8
		}
	}
	steady := func(time.Time) int64 { return 1 }
	three := func(group string, bad func(time.Time) int64) []Cell {
		return []Cell{hourCell(0, group, bad), hourCell(1, group, bad), hourCell(2, group, bad)}
	}
	slo := func(alertAfter time.Duration, noData bool) definitions.SLO {
		return definitions.SLO{Name: "checkout", Service: "checkout", Target: 0.75, Window: definitions.Window{Rolling: true, Duration: 3 * time.Hour},
			Policies: []definitions.Policy{{Name: "burn", AlertWhenNoData: noData, Conditions: []definitions.Condition{
				{Name: "fast", Severity: "page", Op: "gte", Threshold: 2, Lookback: 64 * time.Minute, Short: 4 * time.Minute, AlertAfter: alertAfter}}}}}
	}
	// forecast is the budget an hour on at the burn over the two hours before at, of a
	// three-hour window: a burn of 1 spends a third of the budget an hour.
	forecast := func(bad2h, budget float64) *Forecast {
		burn := bad2h / (120 * 8) / 0.25
		f := &Forecast{Horizon: "1h", Lookback: "2h", BurnRate: ptr(burn), BudgetRemaining: ptr(budget - burn/3)}
		if burn > 0 && budget > 0 {
			f.ExhaustedAt = ptr(at.Add(time.Duration(budget / burn * float64(3*time.Hour))))
		}
		return f
	}
	// status is the status the cells give: firing for the 40 minutes before at, ok otherwise.
	status := func(state string, bad float64, longBad, shortBad float64) Status {
		budget := 1 - bad/(0.25*1440)
		return Status{Name: "checkout", Service: "checkout", State: state, Objective: 0.75, SLI: ptr((1440 - bad) / 1440), BudgetRemaining: ptr(budget),
			BurnRate1h: ptr(float64(min(bad, 60*8)) / (60 * 8) / 0.25), BurnRate6h: ptr(float64(bad) / (180 * 8) / 0.25), Coverage: 1, WindowStart: hour0,
			Conditions: []ConditionStatus{{Policy: "burn", Name: "fast", Severity: "page", State: StateOK, Threshold: 2, Lookback: "64m", ShortWindow: "4m",
				BurnRate: ptr(float64(longBad) / (64 * 8) / 0.25), ShortBurnRate: ptr(float64(shortBad) / (4 * 8) / 0.25)}},
			Forecast: forecast(min(bad, 120*8), budget)}
	}
	since := func(st Status, state string, at time.Time) Status {
		st.Conditions = slices.Clone(st.Conditions)
		st.Since, st.Conditions[0].State, st.Conditions[0].Since = &at, state, &at
		st.Pending = state == statePending
		return st
	}
	ok := status(StateOK, 180, 64, 4)
	ok.BurnRate1h, ok.BurnRate6h = ptr(float64(60)/(480)/0.25), ptr(float64(180)/(1440)/0.25)
	ok.Conditions[0].BurnRate, ok.Conditions[0].ShortBurnRate = ptr(float64(64)/(512)/0.25), ptr(float64(4)/(32)/0.25)
	ok.Forecast = forecast(120, 0.5)
	firing := status(StateFiring, 40*8, 40*8, 4*8)
	noData := Status{Name: "checkout", Service: "checkout", State: StateNoData, Objective: 0.75, SLI: ptr(1.0), BudgetRemaining: ptr(1.0),
		Coverage: 1, WindowStart: hour0, Conditions: []ConditionStatus{{Policy: "burn", Name: "fast", Severity: "page", State: StateOK, Threshold: 2, Lookback: "64m", ShortWindow: "4m"}},
		Forecast: &Forecast{Horizon: "1h", Lookback: "2h"}}
	partial := ok
	partial.State, partial.Coverage = StatePartial, 2.0/3
	partial.SLI, partial.BudgetRemaining, partial.BurnRate6h = ptr(7.0/8), ptr(0.5), ptr(float64(120)/(960)/0.25)
	partial.Forecast = forecast(120, 0.5)
	group := status(StateOK, 40*8, 40*8, 4*8)
	group.SLI, group.BudgetRemaining = ptr((2880-320.0)/2880), ptr(1-320.0/(0.25*2880))
	group.BurnRate1h, group.BurnRate6h = ptr(float64(320)/(960)/0.25), ptr(float64(320)/(2880)/0.25)
	group.Conditions[0].BurnRate, group.Conditions[0].ShortBurnRate = ptr(float64(320)/(1024)/0.25), ptr(float64(32)/(64)/0.25)
	group.Forecast = forecast(320/2.0, 1-320.0/(0.25*2880))
	group.Groups = []Status{status(StateOK, 0, 0, 0), since(status(StateFiring, 40*8, 40*8, 4*8), StateFiring, at.Add(-8*time.Minute))}
	group.Groups[0].Group, group.Groups[1].Group = "a", "b"
	for _, tc := range []struct {
		name  string
		def   definitions.SLO
		cells []Cell
		want  Status
	}{
		{"budget, SLI and burn rate", slo(0, false), three("", steady), ok},
		{"the long window holds and the short does not", slo(0, false), three("", func(m time.Time) int64 { return badSince(64*time.Minute)(m) - badSince(4*time.Minute)(m) }),
			func() Status {
				st := status(StateOK, 60*8, 60*8, 0)
				st.BurnRate1h = ptr(float64(56*8) / 480 / 0.25)
				st.Conditions[0].ShortBurnRate = ptr(float64(0) / 32 / 0.25)
				return st
			}()},
		{"the short window holds and the long does not", slo(0, false), three("", badSince(4*time.Minute)), status(StateOK, 4*8, 4*8, 4*8)},
		// Both windows hold from 8 minutes before at, when the long one reaches half its events bad.
		{"both hold: firing since they began holding", slo(0, false), three("", badSince(40*time.Minute)), since(firing, StateFiring, at.Add(-8*time.Minute))},
		{"held less than alertAfter: pending since it began", slo(10*time.Minute, false), three("", badSince(40*time.Minute)),
			since(status(StateOK, 40*8, 40*8, 4*8), statePending, at.Add(-8*time.Minute))},
		{"held past alertAfter: firing since alertAfter after it began", slo(5*time.Minute, false), three("", badSince(40*time.Minute)), since(firing, StateFiring, at.Add(-3*time.Minute))},
		{"no valid events over the longest lookback: no_data", slo(0, false), []Cell{hourCell(0, "", steady), {Hour: hourCell(1, "", steady).Hour}, {Hour: hourCell(2, "", steady).Hour}},
			func() Status {
				st := noData
				st.SLI, st.BudgetRemaining, st.BurnRate6h = ptr(7.0/8), ptr(0.5), ptr(float64(60)/(480)/0.25)
				return st
			}()},
		{"no_data fires with alertWhenNoData", slo(0, true), []Cell{{Hour: hour0.UnixMilli()}, {Hour: hourCell(1, "", steady).Hour}, {Hour: hourCell(2, "", steady).Hour}},
			func() Status {
				st := noData
				st.State, st.SLI, st.BudgetRemaining = StateFiring, nil, nil
				return st
			}()},
		{"an hour not yet counted: partial", slo(0, false), []Cell{hourCell(1, "", steady), hourCell(2, "", steady)}, partial},
		{"an unbound SLO", definitions.SLO{Name: "checkout", Target: 0.99, Unbound: true, Source: "sum(rate(x[5m]))"}, nil,
			Status{Name: "checkout", State: StateUnbound, Objective: 0.99, Query: "sum(rate(x[5m]))"}},
		{"each group's own state", slo(0, false), grouped(three("a", func(time.Time) int64 { return 0 }), three("b", badSince(40*time.Minute))), group},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, Evaluate(tc.def, tc.cells, at))
		})
	}
}
