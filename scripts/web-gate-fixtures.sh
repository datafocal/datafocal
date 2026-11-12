#!/usr/bin/env bash
# web-gate-fixtures.sh - shows each make web-check rule failing on its fixture under
# web/gate-fixtures, which the gate itself never reads.
set -uo pipefail
cd "$(dirname "$0")/../web"

failures=0
# expect <rule> <file> <command...>: the command must fail on file and name rule.
expect() {
    local rule="$1" file="$2" out
    shift 2
    if out=$("$@" "$file" 2>&1); then
        echo "FAIL: $file passed; want $rule"
        failures=$((failures + 1))
    elif ! grep -qF "$rule" <<<"$out"; then
        echo "FAIL: $file failed without $rule:"
        echo "$out"
        failures=$((failures + 1))
    else
        echo "ok: $rule on $file"
    fi
}

lint=(corepack pnpm exec oxlint --type-aware --deny-warnings)
cruise=(corepack pnpm exec depcruise)
ui=gate-fixtures/packages/ui/src

expect "eslint(complexity)" "$ui/complexity.ts" "${lint[@]}"
expect "eslint(max-lines-per-function)" "$ui/function-length.ts" "${lint[@]}"
expect "eslint(max-depth)" "$ui/depth.ts" "${lint[@]}"
expect "eslint(max-params)" "$ui/params.ts" "${lint[@]}"
expect "typescript(no-explicit-any)" "$ui/any.ts" "${lint[@]}"
expect "typescript(no-floating-promises)" "$ui/floating-promise.ts" "${lint[@]}"
expect "import(no-cycle)" "$ui/cycle-a.ts" "${lint[@]}"
expect "no-circular" "$ui/cycle-a.ts" "${cruise[@]}"
expect "oxc(no-barrel-file)" "$ui/barrel.ts" "${lint[@]}"
expect "react(forbid-dom-props)" "$ui/inline-style.tsx" "${lint[@]}"
expect "eslint(no-restricted-globals)" "$ui/restricted-global.ts" "${lint[@]}"
expect "no-third-party" gate-fixtures/packages/render-core/src/react-import.ts "${cruise[@]}"
expect "api-is-a-leaf" gate-fixtures/packages/api/src/wrong-direction.ts "${cruise[@]}"

exit $((failures > 0))
