package llm

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/value"
)

// usedShare is a quota window's used or remaining percentage: <window>.used_percent(age) or
// <window>.remainingPercentage.
var usedShare = regexp.MustCompile(`^(.*\.)?([^.]+)\.(used|remaining)_?percent(age)?$`)

// windowNames are the spelled-out windows a source names, as the window_minutes others give.
var windowNames = map[string]string{"five_hour": "5h", "seven_day": "7d"}

var unsafe = regexp.MustCompile(`[^a-z0-9]+`)

// quotaKind is what a column says about an account's quota, classified once per name.
type quotaKind int8

const (
	notQuota quotaKind = iota
	quotaUsed
	quotaRemaining
	quotaPlan
	quotaReached
	quotaStatus
	quotaError
	quotaEvent
	quotaCredits
)

func quotaKindOf(name, key string) quotaKind {
	if m := usedShare.FindStringSubmatch(strings.ToLower(name)); m != nil {
		return map[string]quotaKind{"used": quotaUsed, "remaining": quotaRemaining}[m[3]]
	}
	suffixes := []struct {
		suffix string
		kind   quotaKind
	}{
		{"plantype", quotaPlan}, {"subscriptiontype", quotaPlan}, {"usertier", quotaPlan},
		{"ratelimitreachedtype", quotaReached}, {"statuscode", quotaStatus}, {"eventname", quotaEvent},
		{"creditsremaining", quotaCredits}, {"creditsbalance", quotaCredits},
	}
	for _, s := range suffixes {
		if strings.HasSuffix(key, s.suffix) {
			return s.kind
		}
	}
	if key == "error" {
		return quotaError
	}
	return notQuota
}

// quotaColumns adds each quota window a record reports as quota_<window>_used_pct (0 to 100)
// and quota_<window>_resets_at (RFC 3339), and the plan, a limit hit and the credits left.
func quotaColumns(r segment.Record) segment.Record {
	n := len(r.Columns)
	for i := 0; i < n; i++ {
		col := r.Columns[i]
		switch kind := classify(col.Name).quota; kind {
		case notQuota:
		case quotaUsed, quotaRemaining:
			r = windowColumns(r, col, kind)
		default:
			r = accountColumn(r, col, kind)
		}
	}
	return r
}

func windowColumns(r segment.Record, col segment.Column, kind quotaKind) segment.Record {
	n, ok := quotaNumber(col)
	if !ok {
		return r
	}
	m := usedShare.FindStringSubmatch(strings.ToLower(col.Name))
	prefix := col.Name[:len(m[1])+len(m[2])+1]
	if noAllowance(r, prefix) {
		return r
	}
	if kind == quotaRemaining {
		n = 100 - n
	}
	window := windowName(r, prefix, m[2])
	r = Set(r, segment.Column{Name: "quota_" + window + "_used_pct", Type: value.TypeFloat, Value: value.Value{Float: n}})
	if at := resetTime(r, prefix); at != "" {
		r = Set(r, textColumn("quota_"+window+"_resets_at", at))
	}
	return r
}

// accountColumn adds what col says about the account: its plan, a limit it hit, or the
// credits it has left.
func accountColumn(r segment.Record, col segment.Column, kind quotaKind) segment.Record {
	if kind == quotaStatus {
		if n, ok := quotaNumber(col); ok && n == 429 && hasModel(r) {
			r = Set(r, textColumn("quota_limited", "429"))
		}
		return r
	}
	text := strings.TrimSpace(value.Format(col.Type, col.Value))
	switch {
	case text == "" || text == "null":
	case kind == quotaPlan:
		r = Set(r, textColumn("plan", strings.ToLower(text)))
	case kind == quotaReached, kind == quotaError && text == "rate_limit", kind == quotaEvent && strings.HasSuffix(text, "flash_fallback"):
		r = Set(r, textColumn("quota_limited", text))
	case kind == quotaCredits:
		if n, err := strconv.ParseFloat(text, 64); err == nil {
			r = Set(r, segment.Column{Name: "credits_remaining", Type: value.TypeFloat, Value: value.Value{Float: n}})
		}
	}
	return r
}

// hasModel is whether the record is a model call's, so its 429 is the account's quota and
// not another service's.
func hasModel(r segment.Record) bool {
	for _, col := range r.Columns {
		if col.Name == "model" {
			return true
		}
	}
	return false
}

func textColumn(name, text string) segment.Column {
	return segment.Column{Name: name, Type: value.TypeString, Value: value.Value{String: text}}
}

func quotaNumber(col segment.Column) (float64, bool) {
	if col.Type == value.TypeString {
		n, err := strconv.ParseFloat(col.Value.String, 64)
		return n, err == nil
	}
	return usageNumber(col)
}

func sibling(r segment.Record, prefix string, leaves ...string) (segment.Column, bool) {
	for _, col := range r.Columns {
		if !strings.HasPrefix(col.Name, prefix) {
			continue
		}
		leaf := names.Replace(strings.ToLower(strings.TrimPrefix(col.Name, prefix)))
		for _, want := range leaves {
			if leaf == want {
				return col, true
			}
		}
	}
	return segment.Column{}, false
}

// noAllowance is a window that is unlimited or allows nothing, whose percentage is no quota.
func noAllowance(r segment.Record, prefix string) bool {
	if col, ok := sibling(r, prefix, "isunlimitedentitlement", "unlimited"); ok && value.Format(col.Type, col.Value) == "true" {
		return true
	}
	col, ok := sibling(r, prefix, "entitlementrequests", "entitlement")
	n, numeric := quotaNumber(col)
	return ok && numeric && n == 0
}

// windowName is the window's length when the record gives it, else the name it is logged by.
func windowName(r segment.Record, prefix, logged string) string {
	if col, ok := sibling(r, prefix, "windowminutes"); ok {
		if minutes, numeric := quotaNumber(col); numeric && minutes > 0 {
			return duration(int64(minutes))
		}
	}
	if name, ok := windowNames[logged]; ok {
		return name
	}
	return strings.Trim(unsafe.ReplaceAllString(logged, "_"), "_")
}

func duration(minutes int64) string {
	switch {
	case minutes%1440 == 0:
		return fmt.Sprintf("%dd", minutes/1440)
	case minutes%60 == 0:
		return fmt.Sprintf("%dh", minutes/60)
	}
	return fmt.Sprintf("%dm", minutes)
}

// resetTime is when the window resets, from epoch seconds or milliseconds or a timestamp.
func resetTime(r segment.Record, prefix string) string {
	col, ok := sibling(r, prefix, "resetsat", "resetat", "resetdate")
	if !ok {
		return ""
	}
	if n, numeric := quotaNumber(col); numeric {
		if n > 1e12 {
			return time.UnixMilli(int64(n)).UTC().Format(time.RFC3339)
		}
		return time.Unix(int64(n), 0).UTC().Format(time.RFC3339)
	}
	if t, err := time.Parse(time.RFC3339, col.Value.String); err == nil {
		return t.UTC().Format(time.RFC3339)
	}
	return ""
}
