#!/usr/bin/env bash
# Deploy the PipelineAgent loop as ONE Cloud Run Job on an hourly Cloud Scheduler
# trigger, then VERIFY the deploy (image digest + the runtime code hash the job
# itself reports) — "committed" and "deployed" are different states.
#
# Required env (no values live in this tree):
#   PROJECT   GCP project id            REGION    e.g. us-central1
#   SA        runtime service account   DATASET   BigQuery dataset (default pipelineagent)
#   SITE_BUCKET  (optional) GCS bucket that receives index.html/data.json/HEALTHLOG.md
# Cost posture: 1 vCPU / 512 MiB, ~1 min/run, hourly => well under $2/month.
set -euo pipefail
: "${PROJECT:?}" "${REGION:?}" "${SA:?}"
DATASET="${DATASET:-pipelineagent}"
JOB="${JOB:-pipelineagent-run}"
SCHED="${SCHED:-pipelineagent-run-hourly}"
CRON="${CRON:-23 * * * *}"   # off the hour on purpose: most schedules fire at :00
HERE="$(cd "$(dirname "$0")" && pwd)"
GIT_COMMIT="$(git -C "$HERE" rev-parse HEAD 2>/dev/null || echo unknown)"
LOCAL_HASH="$(cd "$HERE" && PYTHONPATH=. python3 -m pipelineagent version | python3 -c 'import json,sys; print(json.load(sys.stdin)["code_sha256"])')"

ENV_VARS="PIPELINEAGENT_WAREHOUSE=bigquery://${PROJECT}/${DATASET},GIT_COMMIT=${GIT_COMMIT},LOG_LEVEL=INFO"
[[ -n "${SITE_BUCKET:-}" ]] && ENV_VARS="${ENV_VARS},PIPELINEAGENT_SITE_BUCKET=${SITE_BUCKET}"

echo "[deploy] building + deploying job ${JOB} from source (commit ${GIT_COMMIT:0:10}, code ${LOCAL_HASH:0:12})"
# --task-timeout/--max-retries/--parallelism are mandatory per project policy; 1 vCPU / 512 MiB is far below the ceiling.
gcloud run jobs deploy "$JOB" --source "$HERE" \
  --project="$PROJECT" --region="$REGION" --service-account="$SA" \
  --cpu=1 --memory=512Mi --task-timeout=7200s --max-retries=1 --parallelism=1 --tasks=1 \
  --set-env-vars="$ENV_VARS" --quiet

JOB_URI="https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT}/jobs/${JOB}:run"
if gcloud scheduler jobs describe "$SCHED" --project="$PROJECT" --location="$REGION" >/dev/null 2>&1; then verb=update; else verb=create; fi
gcloud scheduler jobs "$verb" http "$SCHED" --project="$PROJECT" --location="$REGION" \
  --schedule="$CRON" --time-zone="Etc/UTC" --uri="$JOB_URI" --http-method=POST \
  --oauth-service-account-email="$SA" --attempt-deadline=1800s --quiet

echo "[verify] image digest + runtime code hash"
IMAGE="$(gcloud run jobs describe "$JOB" --project="$PROJECT" --region="$REGION" --format='value(spec.template.spec.template.spec.containers[0].image)')"
echo "  image:        $IMAGE"
echo "  repo code:    $LOCAL_HASH  (commit $GIT_COMMIT)"
echo "  Now run the job once and compare the heartbeat row's code_sha256:"
echo "  gcloud run jobs execute $JOB --project=$PROJECT --region=$REGION --wait"
echo "  bq query 'SELECT ts, code_sha256, git_commit FROM ${DATASET}.heartbeat ORDER BY ts DESC LIMIT 1'"
