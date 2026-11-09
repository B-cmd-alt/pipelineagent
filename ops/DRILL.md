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

## Record — class 2 drill, 2026-11-08/09

The first live drill found four defects, every one of them in the repo's own tests or gate,
none in the model's patches. Each was fixed and the finding re-queued:

| attempt | gate verdict | real cause | fix |
|---|---|---|---|
| 1 | `TESTS_FAILED` | a harness test asserted the parser *rejects* `usd_price` | test asserts the injected bytes, not the parser's reaction (#4) |
| 2 | `TESTS_FAILED` | wiring test asserted the injected run ends `failed` | test uses `stale_source` for failure assertions (#5); DESIGN §4 rule |
| 3 | `REPLAY_FAILED` | stale feedback made the model keep the feed raising "to preserve alerting" | prompt rule "fixed means rows land"; human re-queue resets the budget (#7) |
| 1′ | `REPLAY_FAILED,TESTS_FAILED` | replay failed any anomaly containing "schema" — the very string the prompt asks for; a test still pinned "missing key raises" | anomalies are informational; the missing-key behaviour is no longer pinned |

| 2′ | `TESTS_FAILED` | the model's own test hand-converted an epoch to a datetime and got it wrong; the gate fed back a traceback tail with no test name | feedback keeps the `FAILED`/`E` lines; prompt: build expected datetimes with the module's own call (#9) |
| 3′ | **`PASS`** | — | PR #10 opened, gate-merged, Cloud Build deployed; next run landed rows, both findings CLOSED |

Then one more defect, after the fix had already worked: the ledger's auto-close rule stamped the
closed findings `BENIGN "self-healed"` over the agent's `awaiting verification`, so FIXED was never
recorded (#11: self-heal only over an untriaged finding; the agent reclaims its own merged repair).

**Outcome (UTC, 2026-11-08/09):** injected 22:23 → `write_recency` OPENED 01:26 (MTTD 3h02m, the
3h SLA is the floor by design) → repair PR merged 02:18 → rows landing 02:24 → findings CLOSED 02:26
(1h00m after OPENED, of which ~50 min were the four repo-side defects above) → FIXED recorded 02:50
with PR #10 as evidence → disarmed 02:52, next run clean on the real `usd` key, all three detectors OK.
Zero human edits to the fetcher; every human touch was to the harness, the gate, or the tests.

Lessons: the gate must not contradict the prompt; a test that pins the exact failure the agent is
meant to repair turns every correct patch into `TESTS_FAILED`; the model cannot be trusted to do
arithmetic (epoch→datetime) inside a test; and an auto-rule that overrides agent state silently
rewrites who fixed what.
