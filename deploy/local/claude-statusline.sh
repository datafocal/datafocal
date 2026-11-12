#!/usr/bin/env bash
# Claude Code's status line, installed by scripts/install-local.sh. Claude Code hands the
# account's quota only to this command, on each message and every refreshInterval seconds,
# so each reading is appended to a file the collector follows,
# then the line shows the model and both windows.
input=$(cat)
jq -c --arg ts "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
	'select(.rate_limits) | {timestamp: $ts, session_id, rate_limits}' \
	<<<"$input" >>"$HOME/.claude/claude-quota.jsonl"
jq -r '[.model.display_name // "Claude"]
	+ [.rate_limits | objects | to_entries[] | select(.value.used_percentage)
		| "\(.key | sub("five_hour"; "5h") | sub("seven_day"; "7d")) \(.value.used_percentage | floor)%"]
	| join(" · ")' <<<"$input"
