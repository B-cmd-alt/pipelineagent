# PipelineAgent: Design

PipelineAgent is an autonomous detection-and-repair agent for data pipelines,
paired with an "agentic dashboard" that shows what broke, what the agent did
about it, and whether the thing doing the watching is itself alive.

This document covers the whole system. Section 0 summarizes what v0 already
ships. Everything after it describes the parts that are designed but not built.

---

## 0. v0, shipped

v0 is the observation half: a real fleet, out-of-band detectors, a ledger, a
dead-man's switch, and a static dashboard. No agent yet.

| Component | What exists |
|---|---|
| `fleet.yaml` | Per feed: `id`, `cadence` (`hourly` = state feed, every poll written; `daily` = growing series, identical content skipped), `table`, and SLAs for three axes: `write_recency_max_hours`, `head_lag_max_hours`, `flat_value: {column, group_by, max_consecutive}`. |
| Fleet | 7 public feeds: NWS latest observations, Open-Meteo current conditions, USGS earthquakes `all_hour` GeoJSON, FRED `DGS10` CSV, GitHub Status `summary.json`, CoinGecko `simple/price`, IESO Ontario hourly demand CSV. |
| Fetchers | Append-only rows with `captured_at`, `event_ts`, `source_modified`, `content_sha256`; one `_runs` row per run (`status` ok/no_change/failed/skipped, `rows_written`, `duration_s`, `http_status`, `error`, `code_sha256`). Generic identifying User-Agent, conditional GET and content-hash skip before any write, pacing, benign 404 exits 0, HTML-error-page detection. |
| Detectors | Read data tables, never job status. Result is `OK`, `BREACH`, or `UNKNOWN`. `UNKNOWN` means "could not look" and never renders green. An undeclared axis renders grey ("not declared"). |
| Ledger | Append-only `findings` (transitions only: `OPENED`/`CLOSED`; the hourly state is in `detections`) and `dispositions` (`FIXED`/`BENIGN`/`ESCALATED`/`DEFERRED`/`UNKNOWN`, latest row per finding wins). `HEALTHLOG.md` is generated from the two tables on every render. Auto-rules: new finding gets `UNKNOWN`; a finding that closes with no intervention gets `BENIGN`; a warm-up `UNKNOWN` (insufficient history) is shown but opens no finding. |
| Dead-man's switch | One `heartbeat` row per loop run with the runtime `code_sha256`. Dashboard shows a red banner if the last heartbeat is older than 26h. |
| Dashboard | `render_site.py` writes `site/index.html` + `data.json`: feed x axis health grid, last-24h runs, open findings with disposition, heartbeat and deploy-drift status. Static, no login, zero hosting cost. |
| Scheduling | One Cloud Run Job on an hourly Cloud Scheduler trigger. `deploy.sh` executes the job once after deploying and fails unless the heartbeat's runtime code hash equals the repo hash at the deployed commit. The dashboard also checks its own age client-side, so a dead loop cannot leave a green page behind. |
| Warehouse | One interface, two backends: BigQuery (production) and SQLite (tests, offline demo). |
| ProofGate seam | `schemas/verdict.schema.json`: `run_manifest`, `patch_sha256`, `verdict`, `reason_codes`, `evidence`. |

---

## 1. Goal and non-goals

### Goal

Shorten the time between "a pipeline is silently wrong" and "a reviewed,
evidence-backed fix is waiting for a human or a release gate", without ever
letting the repair side quietly weaken the monitoring side.

Concretely:

1. Detect data failures from the data, not from exit codes.
2. Give every finding an explicit, auditable disposition.
3. Let an agent reproduce a failure from captured inputs and propose a minimal
   fix as a pull request.
4. Hand that pull request to an independent gate that can reject it with a
   named reason.
5. Prove all of the above with a fault-injection benchmark against honest
   baselines.

### Non-goals

- **Not an orchestrator.** It does not replace Airflow, Dagster, or cron. It
  watches whatever those produce.
- **Not a data-quality rules engine.** Three axes, declared per feed. No
  general expectation language.
- **Not auto-merge.** The agent opens PRs. Merging is the gate's or a human's
  decision.
- **Not a correctness oracle.** It cannot tell a plausible wrong number from a
  right one (see Section 7).
- **Not real-time.** Hourly is the finest cadence. Sub-hourly monitoring is
  out of scope.
- **Not multi-tenant.** One fleet, one ledger, one repository.

---

## 2. Architecture overview

```
 +-----------+   append-only rows    +-------------+
 |  fleet    | --------------------> |  warehouse  |  (BigQuery | SQLite)
 | fetchers  |   + raw payload blobs |  data, _runs|
 +-----------+   keyed by sha256     |  findings,  |
      ^                              |  heartbeat  |
      | fleet.yaml (SLAs)            +------+------+
      |                                     |
      |                                     v  read DATA, not job status
      |                              +-------------+
      |                              |  detectors  |  OK | BREACH | UNKNOWN
      |                              +------+------+
      |                                     v
      |                              +-------------+
      |                              |   ledger    |  findings + dispositions
      |                              +------+------+  + HEALTHLOG.md
      |                                     |
      |                     open findings,  |  newest first, depth budget 2
      |                                     v
      |                              +-------------+
      |                              |    agent    |  diagnose, propose
      |                              +------+------+
      |                                     v
      |                              +-------------+
      |                              |   sandbox   |  replay captured bytes,
      |                              |  (replay)   |  run fix + tests
      |                              +------+------+
      |                                     v
      |                              +-------------+
      |                              |  pull req.  |  patch + verdict.json
      |                              +------+------+
      |                                     v
      |                              +-------------+
      +----- merge, redeploy ------- |  ProofGate  |  accept | reject(reason)
                                     +------+------+
                                            v
                                     +-------------+
                                     |  dashboard  |  grid, runs, findings,
                                     |  (static)   |  heartbeat, drift, PRs
                                     +-------------+
```

Two properties hold across the diagram:

- **Observation never depends on the thing observed.** Detectors read the
  warehouse directly. A job that lies about success cannot make a detector
  green.
- **Repair never grades its own homework.** The agent produces evidence. A
  separate gate, with its own rules, decides.

---

## 3. The agent (v1 shipped in dry-run; live once the two secrets are mounted)

Design rule for everything below: **truth lives in two places, the ledger and
GitHub.** The dashboard reads both; nothing else is written anywhere.

### 3.1 One code path for "notified" and "patrols"

The hourly job already runs the detectors and updates the ledger. Right after
that it calls `agent.run()`:

1. Take the findings OPENED this run, newest first, plus any finding whose
   disposition is `DEFERRED` (re-queued by the patrol). Known-open findings
   with any other disposition are not touched and never re-diagnosed.
2. Work at most **2** findings per run (the depth budget). The rest become
   `DEFERRED` with note `depth_budget` and come back next hour.
3. For each: triage → (replay → patch → PR) or escalate → disposition.

So "an agent is notified when a pipeline breaks" and "an agent patrols on a
schedule" are the same loop; there is no queue, no second job, no webhook.
The agent runs inside the job with its own time and token budget.

### 3.2 Triage is deterministic; only the patch needs a model

Inputs, all read-only: the finding, the last few `_runs` rows for the feed,
the archived payload bytes (`raw/<feed>/<content_sha256>`), the fetcher
source at the running `code_sha256`.

| What the evidence says | failure_class | action |
|---|---|---|
| runs are `ok`/`no_change`, write_recency BREACH | `green_job_dead_data` | escalate (the job lies or the source stopped publishing; no code change can fix data that is not there) |
| runs `failed` with transport / 4xx / 5xx / rate-limit | `stale_source` | escalate after the failure has persisted past the SLA; before that, DEFERRED |
| runs `failed` with a parser `schema:` error, or anomalies show non-numeric cells | `schema_drift` | **patch** |
| flat_value BREACH, payloads byte-identical across captures | `flat_value` | escalate (frozen upstream) |
| heartbeat `code_sha256` ≠ repo hash | `deploy_drift` | redeploy (no PR; `deploy.sh` verifies) |
| anything else | `unknown` | escalate, naming exactly what was looked at |

Only `schema_drift` (and later `timezone_shift`) produce a code change, and
only there is an LLM used: it receives the failing payload sample, the
fetcher module and the parser test, and returns a minimal patch confined to
that module plus a new test fixture. There is no `confidence` field: a
missing piece of evidence forces `escalate`, never a guess (seamfulness).

### 3.3 Replay: reproduce first, then clear

Replay runs the fetcher's parse+load path on archived bytes in a plain
subprocess (`python -I`, temp dir, SQLite file, 60 s timeout, sockets
blocked in-process). No Docker: it parses public CSV/JSON from seven known
hosts, and the threat level does not justify a container (Section 6).

A replay **passes** for a patch when, in order:

1. unpatched code on the failing bytes reproduces the breach;
2. patched code on the same bytes clears the same detector axis;
3. patched code on the last **5** healthy payloads of that feed produces
   byte-identical rows to unpatched code (no collateral change);
4. the existing test suite passes unmodified, plus the new fixture test.

### 3.4 Pull request and `verdict.json`

The agent pushes a branch `agent/<finding_id>` and opens a PR carrying
`verdict.json` (`schemas/verdict.schema.json`): `run_manifest` (base commit,
code hashes before/after, replayed payload hashes), `patch_sha256`,
`evidence` (failure_class, diagnosis, detector states before/after), and
`verdict` + `reason_codes` **left empty for the gate**.

### 3.5 The gate: a diff linter plus the replay, nothing smarter

ProofGate, for this project, is a script (`proofgate check`) that runs on the
PR head and writes a commit status. It never calls a model. It re-runs the
replay itself and treats the agent's `evidence` as a claim. Reason codes:

| Code | Rule |
|---|---|
| `REPLAY_FAILED` | any of the four replay conditions fails when the gate re-runs them |
| `TESTS_FAILED` | the suite is red |
| `SLA_WIDENED` | `fleet.yaml` loosened — the same check as `tests/test_policy.py`, run on the PR diff |
| `DETECTOR_DISABLED` | diff touches `detectors.py`, `ledger.py`, `runner.py`, the warehouse, or adds any skip/allowlist |
| `TEST_WEAKENED` | diff deletes or edits an existing test, assertion or fixture |
| `SCOPE_EXCEEDED` | diff touches files outside `feeds/<failing feed>.py` and new files under `tests/` |
| `POLICY_FILE_TOUCHED` | diff touches `fleet.yaml`, `ops/`, `schemas/`, the gate, or CI config |
| `SECRET_IN_DIFF` | key/token patterns in the diff |
| `COLLATERAL_CHANGE` | replay condition 3 differs |

Verdict `PASS` requires zero codes except `REPLAY_PASSED`/`TESTS_PASSED`.
In the public repo the gate is a free GitHub Actions job and a required
status check; in a repo without CI the hourly job runs the same script and
posts the status through the API. Same code, two carriers.

Legitimate SLA, detector or test changes exist. They arrive as human-authored
PRs and are the only PRs the gate does not judge.

### 3.6 Fully automatic: merge on PASS, verify, revert on failure, escalate after 3

The owner's requirement is a loop with **no human in the normal path**.
Autonomy is bounded by evidence, not by confidence:

1. Gate `PASS` → the agent **merges** the PR (squash) and runs `deploy.sh`,
   which fails unless the live heartbeat hash equals the new commit.
2. The next detector run is the verdict that counts. If the finding's axis
   returns to OK within **2 runs** after deploy, disposition `FIXED` with
   `evidence_ref` = the PR's `verdict.json`. The finding then CLOSES normally.
3. If it does not clear, the agent **reverts** the merge (a revert commit,
   never a force-push), redeploys, records attempt N as failed on the
   finding, and goes back to triage with the revert in evidence.
4. After **3 failed attempts** on one finding, or on any `escalate`, the agent
   writes `ESCALATED` and opens exactly one GitHub issue: what was observed,
   what was tried, what is missing. That issue is the only point where a human
   is needed, and the only notification beyond PRs themselves.

What a human can still do at any time: `pipelineagent dispose` overrides any
automatic disposition and stops the agent on that finding; a human PR changes
SLAs or detectors. What the agent can never do, by construction: merge a PR
without a `PASS` status, touch anything outside the failing fetcher, or weaken
a detector, SLA or test (those are gate rejections, not judgement calls).

---

## 4. Fault-injection harness (next)

Four classes first; they map onto existing axes and exercise both outcomes
of the loop (a patch, and a correct refusal to patch):

| # | Class | Injection | Detector axis | Expected end state |
|---|---|---|---|---|
| 1 | green-job-dead-data | fetcher returns 200 with yesterday's bytes; job exits 0 | write_recency | ESCALATED (nothing to patch) |
| 2 | schema drift | rename a CSV column / inject `N/A` into a numeric field in a replayed payload | run `failed`, then write_recency | **FIXED** via patch + replay + auto-merge |
| 3 | stale source | upstream returns 503 / times out past the SLA | write_recency, head_lag | ESCALATED after SLA (DEFERRED before) |
| 7 | deploy drift | deployed image built from an older commit | heartbeat hash | redeploy, no PR |

Class 5 (DST / timezone shift) is the showcase for week 4 if time allows;
IESO's fixed-EST file is the natural fixture. Classes 4 (flat value) and 6
(duplicate / partial load) are deferred: 6 needs a fetcher-side row-count
invariant reported through `_runs`, which is the one open design question.
Each injection writes a ground-truth row (class, injected_at, feed); fixtures
are synthetic, so recorded upstream payloads are never published.

---

## 5. Benchmark (next)

- **Metrics:** detection rate, false-alarm rate, MTTD (injection → OPENED),
  MTTR (OPENED → FIXED or ESCALATED), gate rejections, cost per finding.
- **One baseline:** watchdog-only (v0 as shipped: detectors, no agent). The
  question the benchmark answers is whether the agent lowers MTTR without
  raising false fixes. Status-only monitoring is stated in one line, not
  built: it misses classes 1, 3 and 7 by construction.
- **UNKNOWN is scored honestly:** during an injection an UNKNOWN counts as a
  miss; outside one it is blind time, reported per feed. It is never a
  correct "no breach".
- Results go into the README, not a separate page.

---

## 6. Threat model

| Threat | Mitigation |
|---|---|
| Prompt injection via data (text in a CSV cell or JSON field reaching the model) | the model only ever sees payloads inside the patch step; its output is a diff confined by `SCOPE_EXCEEDED`, judged by a gate that reads no model output |
| A repair that weakens monitoring | `SLA_WIDENED`, `DETECTOR_DISABLED`, `POLICY_FILE_TOUCHED` are hard rejections |
| A repair that weakens tests | `TEST_WEAKENED`; new tests only |
| Runaway cost | depth budget 2/run, per-run token and time caps, request budget 40/run, 1 GiB per BigQuery query |

Not defended: sandbox escape from the replay subprocess. It parses public
CSV/JSON from seven known hosts under `python -I` with sockets blocked; that
is proportionate, and stated here so nobody mistakes it for isolation.
Upstream rate limits, HTML error pages and staging-table leaks are handled in
v0 and are not threats to the agent.

---

## 7. Explicit non-guarantees

- A plausible but wrong value is not detected; only the declared axes are.
- The agent repairs parsers and fetchers. It does not repair upstreams,
  credentials, or infrastructure; those are ESCALATED, correctly.
- An auto-merged patch is verified by replay and by the next live run, not by
  a human reading it. The revert path bounds the damage; it does not make it
  zero.
- During the monitor's own outage every cell is UNKNOWN and the dead-man
  banner is the only signal.
- Third-party feeds may change terms; nothing here is a commitment about them.

---

## 8. Sequencing

Decided, not open: the agent runs inside the hourly job; payloads are kept
with a bucket lifecycle rule and never published; the gate is a script.

- **Week 2 — agent end to end on class 2. DONE (dry-run live).** Triage
  table; replay runner; patch step for `schema_drift`; PR with
  `verdict.json`; `proofgate check` as the linter + replay; auto-merge →
  Cloud Build deploy → verify by the next 2 runs → FIXED/revert; ESCALATED
  with one issue per feed. First live outcome: the FRED incident was triaged
  `stale_source`, deferred one run, then escalated with a single (dry-run)
  issue shared by its three axes — the expected result. Dashboard cards show
  the chain. Remaining to go live: `GITHUB_TOKEN`/`GITHUB_REPO` and
  `OPENROUTER_API_KEY` mounted into the job (the patch
  model is called through OpenRouter; default `deepseek/deepseek-v4-pro`).
- **Week 3 — harness for classes 1, 2, 3, 7** with ground truth; the
  3-attempts-then-escalate path exercised by a deliberately unfixable
  injection; 24-hour summary line on the dashboard.
- **Week 4 — benchmark against watchdog-only, write-up in the README**; class
  5 if time allows.

Prerequisites that are the owner's: a fine-grained GitHub token (contents +
pull_requests on the public repo) and a model API key, both in Secret
Manager, mounted into the job. Without them the agent runs in `--dry-run`:
it triages, replays and writes the PR body to the bucket, but opens nothing.

---

## 9. References borrowed from

- Apple ML Research, *Mapping the Design Space of User Experience for Computer
  Use Agents* (2026): taxonomy of explainability / user control / mental
  models; Wizard-of-Oz finding that interruption alone is insufficient for an
  absent user, that approval should scale with stakes, and that users prefer
  an agent that flags missing knowledge ("seamfulness") over one that guesses.
  Used for Sections 3.0, 3.2 and 3.6.
- *AgentGUI: An Interface for Observing and Steering Long-Running AI Agents*
  (2026): "observability is a precondition for trusting delegated work";
  splitting a trace into reasoning / generation / tool calls made operators
  measurably faster and more accurate in its study; its "manager audit"
  (an independent judge that gathers evidence and rules per criterion) is the
  shape the gate takes here, minus the model: for this project the judge is a
  linter plus a replay. Used for Sections 3.4 and 3.5.
- *AgentOps: Enabling Observability of LLM Agents* (2024): the observability
  vocabulary (traces, artefacts, decisions) the ledger and dashboard follow.
