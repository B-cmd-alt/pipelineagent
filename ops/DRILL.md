# Live drill: prove the automatic repair loop end to end

A controlled failure, injected on the deployed job, that must travel the whole path:
archived bytes → detector → finding → agent triage → replay → model patch → PR → gate →
auto-merge → Cloud Build deploy → verification → `FIXED`. Nothing is simulated; the only
unreal thing is the cause.

## Class 2 (schema drift) on CoinGecko

```bash
# 1. arm: the harness renames the `usd` key in every CoinGecko payload the parser sees
gcloud run jobs update pipelineagent-run --project=<PROJECT> --region=<REGION> \
  --update-env-vars=PIPELINEAGENT_INJECT=coingecko_prices=schema_rename
```

What should happen, hour by hour (the job runs at :23):

| when | what | where to look |
|---|---|---|
| run 1 | fetch fails with `schema: expected key 'usd'`; the injected bytes are archived; `harness_injections` gets its first row (ground truth, MTTD clock starts) | `_runs` (status failed), bucket `raw/coingecko_prices/<sha>` |
| ~3h later | `write_recency` passes its 3h SLA → finding OPENED | `findings`, dashboard card |
| same run | agent: triage `schema_drift → patch`, replay reproduces the breach on the archived bytes, model proposes a parser that accepts `usd` and `usd_price`, replay clears, gate `PASS`, PR opened, **merged**, Cloud Build triggered | `agent_steps`, the PR on GitHub |
| next 1–2 runs | job runs the new image (`GIT_COMMIT` = merge sha), rows land again, finding CLOSES → disposition `FIXED` with the PR's `verdict.json` as evidence | dashboard chain: all green through `verify` |

```bash
# 2. disarm when FIXED is recorded (or after the drill window), then watch one more run stay green
gcloud run jobs update pipelineagent-run --project=<PROJECT> --region=<REGION> --remove-env-vars=PIPELINEAGENT_INJECT
```

The patched parser keeps accepting the real `usd` key (the gate's healthy-payload check
guarantees identical rows on unmodified payloads), so disarming is safe.

## Class 3 (stale source) on any feed — the "correct refusal" drill

`PIPELINEAGENT_INJECT=<feed>=stale_source` makes every fetch time out. Expected: finding after the
SLA, triage `stale_source → defer` once, then `escalate`: one GitHub issue, disposition
`ESCALATED`, **no PR**. Disarm; the finding closes as the source "recovers"; the ESCALATED
disposition stays as the record (a human may dispose it BENIGN with a note).

## Reading the result

```sql
-- MTTD per injection: first OPENED after the first ground-truth row
SELECT i.feed, MIN(i.ts) injected_at, MIN(f.event_at) detected_at,
       TIMESTAMP_DIFF(MIN(f.event_at), MIN(i.ts), MINUTE) mttd_min
FROM pipelineagent.harness_injections i
JOIN pipelineagent.findings f ON f.feed = i.feed AND f.event = 'OPENED' AND f.event_at >= i.ts
GROUP BY i.feed;
```

Rules the drill does not relax: the agent never widens an SLA or disables a detector to make the
finding go away; the gate rejects that, injection or not.
