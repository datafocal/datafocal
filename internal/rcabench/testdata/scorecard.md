# RCA Scorecard

Aggregate over 36 scenarios (27 scored, 4 refusals, 5 known gaps):

| metric | value |
| --- | --- |
| AC@1 | 0.9259 |
| AC@3 | 1.0000 |
| AC@5 | 1.0000 |
| MRR | 0.9630 |
| false_admit_rate | 0.0000 |
| refusal_accuracy | 1.0000 |
| known_gaps | 5 |
| total_gets | 336 |
| total_bytes_read | 98206 |

## By log shape

| logs | scored | AC@1 | AC@3 | MRR | false_admit_rate | refusal_accuracy | known_gaps |
| --- | --- | --- | --- | --- | --- | --- | --- |
| raw | 14 | 0.9286 | 1.0000 | 0.9643 | 0.0000 | 1.0000 | 2 |
| structured | 13 | 0.9231 | 1.0000 | 0.9615 | 0.0000 | 1.0000 | 3 |

## Known gaps

Shapes the tools cannot serve today. Excluded from AC@k; the harness fails if one starts working.

| name | shape | rank | rejected_by | gap |
| --- | --- | --- | --- | --- |
| vocab_inline_duration | quantile | 0 | unknown_field | latency is written 120ms with no space, so masking cannot name the parameter duration_ms and no latency column exists for a quantile |
| vocab_inline_duration_raw | quantile | 0 | unknown_field | latency is written 120ms with no space, so masking cannot name the parameter duration_ms and no latency column exists for a quantile |
| vocab_no_placement | count | 0 | no_attribution | no placement stream, which is the common case without scheduler logs, so the topology and attribution gates lose their declared input |
| vocab_split_outcome | ratio | 0 | unusable_series | the outcome is in the message text, so masking splits the symptom into err/ok patterns and a ratio over either is then constant |
| vocab_split_outcome_raw | ratio | 0 | unusable_series | the outcome is in the message text, so masking splits the symptom into err/ok patterns and a ratio over either is then constant |

## Per-scenario

| name | volume | mechanism | lead | shape | rank | found | admitted_by | rejected_by | false_admits | refusal_expected | refusal_correct | gets | pass |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| baseline | loud | topology | well_separated | count | 1 | true | topology | - | - | false | false | 6 | true |
| baseline_raw | loud | topology | well_separated | count | 1 | true | entity | - | - | false | false | 6 | true |
| lead_featureless_coramp | loud | topology | featureless_coramp | count | 1 | true | topology | - | - | true | true | 6 | true |
| lead_featureless_coramp_raw | loud | topology | featureless_coramp | count | 1 | true | entity | - | - | true | true | 6 | true |
| lead_one_bin | loud | topology | one_bin | count | 1 | true | topology | - | - | false | false | 6 | true |
| lead_one_bin_raw | loud | topology | one_bin | count | 1 | true | entity | - | - | false | false | 6 | true |
| lead_simultaneous | loud | topology | simultaneous | count | 1 | true | topology | - | - | false | false | 6 | true |
| lead_simultaneous_raw | loud | topology | simultaneous | count | 1 | true | entity | - | - | false | false | 6 | true |
| mechanism_attribution | loud | attribution | well_separated | count | 1 | true | entity | - | - | false | false | 6 | true |
| mechanism_attribution_raw | loud | attribution | well_separated | count | 1 | true | entity | - | - | false | false | 6 | true |
| mechanism_entity | loud | entity | well_separated | count | 1 | true | entity | - | - | false | false | 6 | true |
| mechanism_entity_raw | loud | entity | well_separated | count | 1 | true | entity | - | - | false | false | 6 | true |
| mechanism_none | loud | none | well_separated | count | 0 | false | - | no_attribution | - | true | true | 6 | true |
| mechanism_none_raw | loud | none | well_separated | count | 0 | false | - | no_attribution | - | true | true | 6 | true |
| mechanism_shared_field | loud | shared_field | well_separated | count | 1 | true | topology | - | - | false | false | 0 | true |
| mechanism_shared_field_raw | loud | shared_field | well_separated | count | 1 | true | entity | - | - | false | false | 0 | true |
| shape_filtered_count | loud | topology | well_separated | filtered_count | 1 | true | topology | - | - | false | false | 30 | true |
| shape_filtered_count_raw | loud | topology | well_separated | filtered_count | 1 | true | entity | - | - | false | false | 30 | true |
| shape_filtered_quantile | loud | topology | well_separated | filtered_quantile | 1 | true | topology | - | - | false | false | 30 | true |
| shape_filtered_quantile_raw | loud | topology | well_separated | filtered_quantile | 1 | true | entity | - | - | false | false | 30 | true |
| shape_quantile | loud | topology | well_separated | quantile | 1 | true | topology | - | - | false | false | 6 | true |
| shape_quantile_raw | loud | topology | well_separated | quantile | 1 | true | entity | - | - | false | false | 6 | true |
| shape_ratio | loud | topology | well_separated | ratio | 1 | true | topology | - | - | false | false | 18 | true |
| shape_ratio_raw | loud | topology | well_separated | ratio | 1 | true | entity | - | - | false | false | 18 | true |
| shape_sum_ratio | loud | topology | well_separated | sum_ratio | 2 | true | topology | - | - | false | false | 31 | false |
| shape_sum_ratio_raw | loud | topology | well_separated | sum_ratio | 2 | true | entity | - | - | false | false | 31 | false |
| vocab_inline_duration | loud | topology | well_separated | quantile | 0 | false | - | unknown_field | - | false | false | 0 | false |
| vocab_inline_duration_raw | loud | topology | well_separated | quantile | 0 | false | - | unknown_field | - | false | false | 0 | false |
| vocab_no_placement | loud | topology | well_separated | count | 0 | false | - | no_attribution | - | false | false | 0 | false |
| vocab_no_placement_raw | loud | topology | well_separated | count | 1 | true | entity | - | - | false | false | 0 | true |
| vocab_split_outcome | loud | topology | well_separated | ratio | 0 | false | - | unusable_series | - | false | false | 0 | false |
| vocab_split_outcome_raw | loud | topology | well_separated | ratio | 0 | false | - | unusable_series | - | false | false | 0 | false |
| volume_quiet | quiet | topology | well_separated | count | 1 | true | topology | - | - | false | false | 6 | true |
| volume_quiet_raw | quiet | topology | well_separated | count | 1 | true | entity | - | - | false | false | 6 | true |
| volume_silent_then_present | silent_then_present | topology | well_separated | count | 1 | true | topology | - | - | false | false | 5 | true |
| volume_silent_then_present_raw | silent_then_present | topology | well_separated | count | 1 | true | entity | - | - | false | false | 5 | true |
