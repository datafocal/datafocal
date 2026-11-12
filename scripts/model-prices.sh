#!/usr/bin/env bash
# model-prices.sh OUT [CATALOG] writes model prices to OUT, a TSV of one model per line sorted
# by id, whose header names the lookup table each column fills: price_in, price_out,
# price_cache_read and price_cache_write, in USD per million tokens, empty where the catalog
# has no price. CATALOG is models.dev's api.json (https://models.dev, MIT), fetched when not
# given; the file is a copy, so the querier never reads models.dev itself. OUT is
# internal/llm/prices.tsv, which the querier embeds as its default tables.
#
# A model several providers sell is priced at its maker's own API, so a bare model id in a
# log reads the list price; a model no maker sells takes the first reseller's by name. The
# catalog's long-context tiers are not carried.
set -euo pipefail

out=$1
catalog=${2:-}
fetched=
trap 'rm -f $fetched' EXIT
if [[ -z $catalog ]]; then
	fetched=$(mktemp)
	catalog=$fetched
	curl -fsSL https://models.dev/api.json -o "$catalog"
fi

jq -r '
  ["anthropic", "openai", "google", "mistral", "deepseek", "xai", "cohere", "meta", "moonshotai", "alibaba", "zai"] as $makers
  | to_entries
  | sort_by(.key as $p | ($makers | index($p)) // 99, .key)
  | [.[].value.models // {} | to_entries[] | select(.value.cost != null)]
  | reduce .[] as $m ({};
      reduce ("input", "output", "cache_read", "cache_write") as $rate (.;
        if $m.value.cost[$rate] == null or .[$m.key][$rate] != null then .
        else .[$m.key][$rate] = ($m.value.cost[$rate] | tostring) end))
  | ["model", "price_in", "price_out", "price_cache_read", "price_cache_write"],
    (to_entries | sort_by(.key)[]
      | [.key, .value.input // "", .value.output // "", .value.cache_read // "", .value.cache_write // ""])
  | @tsv
' "$catalog" >"$out"
