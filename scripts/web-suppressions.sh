#!/usr/bin/env bash
# web-suppressions.sh - fails when web/packages has more oxlint-disable comments than
# BASELINE, so a PR that adds one must raise it in review.
set -euo pipefail

BASELINE=0

cd "$(dirname "$0")/.."
count=$({ git grep --untracked -o 'oxlint-disable' -- web/packages || true; } | wc -l)
if (( count > BASELINE )); then
    echo "web/packages has $count oxlint-disable comments, over the baseline of $BASELINE:"
    git grep --untracked -n 'oxlint-disable' -- web/packages
    exit 1
fi
