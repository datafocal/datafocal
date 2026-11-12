package llm_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datafocal/datafocal/internal/llm"
	"github.com/datafocal/datafocal/internal/logformat"
	"github.com/datafocal/datafocal/internal/value"
)

func TestQuotaColumns(t *testing.T) {
	p, err := logformat.New(logformat.Config{Format: "json"})
	require.NoError(t, err)
	for _, tc := range []struct {
		name, line string
		want       map[string]string
	}{
		{"windows by length", `{"type":"event_msg","payload":{"type":"token_count","rate_limits":{"primary":{"used_percent":42.5,"window_minutes":300,"resets_at":1793491200},"secondary":{"used_percent":12,"window_minutes":10080,"resets_at":1793923200},"credits":{"has_credits":true,"unlimited":false,"balance":"4.50"},"plan_type":"Plus","rate_limit_reached_type":null}}}`, map[string]string{
			"quota_5h_used_pct": "42.5", "quota_5h_resets_at": "2026-11-01T00:00:00Z",
			"quota_7d_used_pct": "12", "quota_7d_resets_at": "2026-11-06T00:00:00Z",
			"plan": "plus", "credits_remaining": "4.5",
		}},
		{"limit reached", `{"payload":{"rate_limits":{"primary":{"used_percent":100,"window_minutes":300},"rate_limit_reached_type":"rate_limit_reached"}}}`, map[string]string{"quota_5h_used_pct": "100", "quota_limited": "rate_limit_reached"}},
		{"remaining share, no allowance skipped", `{"type":"assistant.usage","data":{"model":"gpt-5-mini","quotaSnapshots":{"chat":{"isUnlimitedEntitlement":false,"entitlementRequests":200,"remainingPercentage":90.5,"resetDate":"2026-08-01T00:00:00Z"},"premium_interactions":{"isUnlimitedEntitlement":false,"entitlementRequests":0,"remainingPercentage":0},"completions":{"isUnlimitedEntitlement":true,"entitlementRequests":0,"remainingPercentage":100}}}}`, map[string]string{"quota_chat_used_pct": "9.5", "quota_chat_resets_at": "2026-08-01T00:00:00Z"}},
		{"named windows", `{"session_id":"s","rate_limits":{"five_hour":{"used_percentage":23.5,"resets_at":1793491200},"seven_day":{"used_percentage":41}}}`, map[string]string{"quota_5h_used_pct": "23.5", "quota_5h_resets_at": "2026-11-01T00:00:00Z", "quota_7d_used_pct": "41"}},
		{"a model call answering 429", `{"event.name":"gemini_cli.api_error","model":"gemini-3-flash-preview","status_code":429}`, map[string]string{"quota_limited": "429"}},
		{"another service's 429 is no quota", `{"service":"api","status_code":429}`, map[string]string{}},
		{"fallback and credits", `{"event.name":"gemini_cli.flash_fallback","auth_type":"oauth-personal"}`, map[string]string{"quota_limited": "gemini_cli.flash_fallback"}},
		{"credits left", `{"event.name":"gemini_cli.credits_used","credits_consumed":2,"credits_remaining":98}`, map[string]string{"credits_remaining": "98"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, ok := p.Parse(tc.line)
			if !ok {
				r, ok = p.Flush()
			}
			require.True(t, ok)
			got := map[string]string{}
			for _, col := range llm.Normalize(r).Columns {
				if strings.HasPrefix(col.Name, "quota_") || col.Name == "plan" || col.Name == "credits_remaining" {
					got[col.Name] = value.Format(col.Type, col.Value)
				}
			}
			assert.Equal(t, tc.want, got)
		})
	}
}
