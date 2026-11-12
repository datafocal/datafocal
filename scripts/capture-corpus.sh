#!/usr/bin/env bash
# capture-corpus.sh captures a corpus the benchmarks replay (make bench-e2e CORPUS=<dir>): its
# files as the system wrote them and a manifest.json naming how a collector reads them.
#
#   ai [hours]          this machine's agent transcripts (~/.claude, ~/.codex) and its journal's
#                       last hours (default 48), as a host collector reads them, into
#                       .localdata/corpus/ai-host-<date>
#   pods <node> <out>   a kind node's container logs, each container's rotated files before its
#                       current one and named as /var/log/containers names it, into <out>
#
# A capture holds what the system logged, prompts and code included: it stays under .localdata,
# which git ignores, and reports name it by its fingerprint.
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)

ai() {
  local hours=${1:-48} out
  out=$ROOT/.localdata/corpus/ai-host-$(date +%Y%m%d)
  rm -rf "$out" && mkdir -p "$out"
  (cd "$HOME/.claude/projects" 2>/dev/null && find . -path '*.jsonl' -type f | cpio -pdm --quiet "$out/claude") || true
  (cd "$HOME/.codex/sessions" 2>/dev/null && find . -name '*.jsonl' -type f | cpio -pdm --quiet "$out/codex") || true
  journalctl --output=json --all --since "-${hours}h" --until now >"$out/journal.jsonl"
  cat >"$out/manifest.json" <<'EOF'
{"name": "ai-host", "kind": "ai", "copies": 1, "sources": [
  {"path": "claude/*/*.jsonl", "parse": {"format": "json"}},
  {"path": "claude/*/*/subagents/*.jsonl", "parse": {"format": "json"}},
  {"path": "codex/*/*/*/*.jsonl", "parse": {"format": "json"}},
  {"path": "journal.jsonl", "parse": {"format": "journal"}}]}
EOF
  du -sh "$out"
}

pods() {
  local node=${1:?pods needs a kind node container} out=${2:?pods needs an output directory}
  rm -rf "$out" && mkdir -p "$out/containers"
  # In the node: each /var/log/containers link's file, after the files the kubelet rotated it
  # to (N.log.<time>, the older ones gzipped), so a replay reads what a collector would have.
  docker exec "$node" bash -c '
    set -euo pipefail
    tmp=$(mktemp -d)
    for link in /var/log/containers/*.log; do
      current=$(readlink -f "$link")
      { for old in $(ls "$current".* 2>/dev/null | sort); do
          case $old in *.gz) zcat "$old" ;; *) cat "$old" ;; esac
        done
        cat "$current"; } >"$tmp/$(basename "$link")"
    done
    tar -C "$tmp" -cf - . && rm -rf "$tmp"' | tar -C "$out/containers" -xf -
  cat >"$out/manifest.json" <<EOF
{"name": "$(basename "$out")", "kind": "mesh", "copies": 1, "sources": [
  {"path": "containers/*.log", "parse": {"format": "auto", "envelope": "cri"}}]}
EOF
  du -sh "$out"
}

case ${1:-} in
ai) shift && ai "$@" ;;
pods) shift && pods "$@" ;;
*) sed -n '2,13p' "$0" | sed 's/^# \{0,1\}//' && exit 64 ;;
esac
