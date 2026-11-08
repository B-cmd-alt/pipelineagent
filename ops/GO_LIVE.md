# Go live: turning the agent from dry-run into fully automatic

The hourly job already runs the agent. Without credentials it triages and replays for real
but opens nothing. Mounting two secrets and a few variables makes it open PRs, merge on gate
PASS, redeploy and verify. Nothing in the code changes.

## 1. Credentials to create (humans only; never commit them)

| Secret | Where to get it | Minimum scope |
|---|---|---|
| GitHub fine-grained personal access token | GitHub → Settings → Developer settings → Fine-grained tokens | Repository access: only the target repo. Permissions: **Contents: read/write**, **Pull requests: read/write**, **Issues: read/write**, **Commit statuses: read/write**, Metadata: read. Expiry: 90 days or less; rotate. |
| OpenRouter API key | openrouter.ai → Settings → API Keys | a dedicated key with a credit limit (the agent makes at most 2 model calls per hour, only when a patch is attempted; a call costs cents) |

## 2. Put them in the cloud secret store and let the job's runtime service account read them

Create one secret per credential in the job's project (names are free; the examples below are
used in step 3). Grant the job's **runtime service account** the `secretAccessor` role on each
secret, nothing wider. Creating secrets and granting IAM typically needs an admin identity, not
the runtime account itself.

## 3. Mount them on the job

Use `gcloud run jobs update` with `--set-secrets` (secret-backed env vars) and
`--update-env-vars` (plain config). **Never `--set-env-vars`**: it wipes every variable on the
job. `deploy.sh` also uses `--update-env-vars`, so later deploys keep what you set here.

```bash
gcloud run jobs update pipelineagent-run --project=<PROJECT> --region=<REGION> \
  --set-secrets=GITHUB_TOKEN=<github-secret-name>:latest,OPENROUTER_API_KEY=<openrouter-secret-name>:latest \
  --update-env-vars=GITHUB_REPO=<owner>/<repo>,GITHUB_BASE_BRANCH=main,GITHUB_SUBDIR=<subdir-or-empty>,\
PIPELINEAGENT_AUTO_MERGE=<0|1>,CLOUD_BUILD_PROJECT=<PROJECT>,CLOUD_BUILD_REGION=<REGION>,\
CLOUD_BUILD_SA=<runtime-sa-email>,CLOUD_RUN_JOB=pipelineagent-run
```

Variables the agent reads:

| Variable | Meaning |
|---|---|
| `GITHUB_TOKEN`, `GITHUB_REPO` | without both, the GitHub client is a dry run |
| `GITHUB_BASE_BRANCH` | branch PRs target (default `main`) |
| `GITHUB_SUBDIR` | path prefix of this project inside the repo (`pipelineagent` in a monorepo; empty in a standalone repo) |
| `PIPELINEAGENT_AUTO_MERGE` | `1` = merge on gate PASS (fully automatic); `0` = open the PR with the gate status and stop |
| `OPENROUTER_API_KEY` | without it the patch step is a dry run (triage/replay still real) |
| `PIPELINEAGENT_MODEL` | optional OpenRouter model id; default `deepseek/deepseek-v4-pro`. A comma-separated list makes the rest fallbacks (e.g. `deepseek/deepseek-v4-pro,z-ai/glm-5.3`). Only providers that honour strict JSON-schema output are used. |
| `CLOUD_BUILD_PROJECT`, `CLOUD_BUILD_REGION`, `CLOUD_BUILD_SA`, `CLOUD_RUN_JOB` | enable the post-merge build + deploy trigger; the build runs as `CLOUD_BUILD_SA`, which needs permission to push images and update the job |
| `PIPELINEAGENT_AGENT` | `0` turns the agent off entirely (default on) |

## 4. Verify

```bash
gcloud run jobs describe pipelineagent-run --project=<PROJECT> --region=<REGION> \
  --format='yaml(spec.template.spec.template.spec.containers[0].env)'   # secrets show as secretKeyRef, never as values
gcloud run jobs execute pipelineagent-run --project=<PROJECT> --region=<REGION> --wait
```

Then in the warehouse: `SELECT step, outcome, ref FROM pipelineagent.agent_steps ORDER BY ts DESC LIMIT 20`.
Live mode shows `pr ok` with a real GitHub URL instead of `pr dry_run` / `dry-run://` refs, and
escalations open real issues (`escalate ok` with an `https://github.com/.../issues/N` ref).

## 5. Where the automatic merge is appropriate

Auto-merge writes to the repo's default branch with no human. In a repository that has its own
merge discipline (review gates, protected branches, a monorepo shared with other work), run with
`PIPELINEAGENT_AUTO_MERGE=0` until the project lives in its own repository, where the gate is the
only required check. The agent never force-pushes and never touches files outside the failing
fetcher module and new tests; a revert is a new commit.
