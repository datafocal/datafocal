#!/usr/bin/env bash
# web-preview-check.sh - shows make preview shooting each view's scenario populated, and
# failing on a page with no scenario and on a request with no engine answer. Given a directory,
# it reads the PNGs make preview wrote there for the views it shoots by default, and shoots only
# the views' other routes.
set -uo pipefail
cd "$(dirname "$0")/.."

out=$(mktemp -d)
trap 'rm -rf "$out"' EXIT
failures=0
shoot() {
    PREVIEW_PAGES="$1" PREVIEW_OUT="$out" node web/preview/shoot.ts 2>&1
}

shot=${1:-$out}
defaults='{"name":"ai","path":"/ui/ai"},{"name":"logs","path":"/ui/logs"},{"name":"graph","path":"/ui/graph"},{"name":"trace","path":"/ui/trace"},{"name":"views","path":"/ui/views"}'
routes='{"name":"view","path":"/ui/views/name=querier"},{"name":"builder","path":"/ui/views/mode=new"}'
views="[$routes]"
[[ -n ${1:-} ]] || views="[$defaults,$routes]"
if ! got=$(shoot "$views"); then
    echo "FAIL: the views' scenarios did not shoot:"
    echo "$got"
    failures=$((failures + 1))
fi
check() {
    [[ -s "$1/$2.png" ]] && echo "ok: $2.png" || { echo "FAIL: no $1/$2.png"; failures=$((failures + 1)); }
}
for view in ai logs graph trace views; do check "$shot" "$view"; done
for view in view builder; do check "$out" "$view"; done

# expect <want> <pages>: shooting pages must fail and say want.
expect() {
    local got
    if got=$(shoot "$2"); then
        echo "FAIL: $2 shot; want $1"
        failures=$((failures + 1))
    elif ! grep -qF "$1" <<<"$got"; then
        echo "FAIL: $2 failed without $1:"
        echo "$got"
        failures=$((failures + 1))
    else
        echo "ok: $1"
    fi
}

expect "no preview scenario for /ui/nope" '[{"name":"nope","path":"/ui/nope"}]'
expect "no answer for query" '[{"name":"logs","path":"/ui/logs/time=2026-07-18T14%3A00%3A00Z%2C2026-07-18T14%3A10%3A00Z/query=%7Buser_id%3D%22user-00043%22%7D"}]'

exit $((failures > 0))
