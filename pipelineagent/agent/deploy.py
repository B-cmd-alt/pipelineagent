"""Trigger a build + Cloud Run job update for a merged commit via the Cloud Build REST API,
with the repository tarball as the source. No gcloud in the container. Dry run when the
GCP env is not configured. Verification is NOT here: the next loop run compares its own
GIT_COMMIT and the finding's state (agent.verify_pending)."""
from __future__ import annotations

import logging
import os
from typing import Callable

log = logging.getLogger("pipelineagent.deploy")


def from_env(github) -> Callable[[str], str | None]:
    project, region, job = os.environ.get("CLOUD_BUILD_PROJECT"), os.environ.get("CLOUD_BUILD_REGION", "us-central1"), os.environ.get("CLOUD_RUN_JOB", "pipelineagent-run")
    sa, bucket, subdir = os.environ.get("CLOUD_BUILD_SA"), os.environ.get("PIPELINEAGENT_SITE_BUCKET"), os.environ.get("GITHUB_SUBDIR", "")
    if not (project and sa and bucket and hasattr(github, "tarball")):
        return lambda sha: None

    def trigger(sha: str) -> str | None:
        try:
            import google.auth
            from google.auth.transport.requests import AuthorizedSession
            from google.cloud import storage

            tar = github.tarball(sha)
            obj = f"build-src/{sha}.tgz"
            storage.Client().bucket(bucket).blob(obj).upload_from_string(tar, content_type="application/gzip")
            image = f"{region}-docker.pkg.dev/{project}/cloud-run-source-deploy/{job}:{sha[:10]}"
            # GitHub tarballs unpack into one top-level dir; the Dockerfile lives in <that>/<subdir>
            build = {
                "source": {"storageSource": {"bucket": bucket, "object": obj}},
                "steps": [
                    {"name": "gcr.io/cloud-builders/docker", "entrypoint": "bash",
                     "args": ["-c", f"cd */{subdir} && docker build -t {image} ."]},
                    {"name": "gcr.io/cloud-builders/docker", "args": ["push", image]},
                    {"name": "gcr.io/google.com/cloudsdktool/cloud-sdk:slim", "entrypoint": "gcloud",
                     "args": ["run", "jobs", "update", job, f"--image={image}", f"--region={region}", f"--project={project}", f"--update-env-vars=GIT_COMMIT={sha}"]},
                ],
                "serviceAccount": f"projects/{project}/serviceAccounts/{sa}",
                "options": {"logging": "CLOUD_LOGGING_ONLY"},
                "timeout": "1200s",
            }
            creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
            r = AuthorizedSession(creds).post(f"https://cloudbuild.googleapis.com/v1/projects/{project}/locations/{region}/builds", json=build, timeout=60)
            r.raise_for_status()
            bid = r.json().get("metadata", {}).get("build", {}).get("id") or r.json().get("name")
            log.info("cloud build %s triggered for %s", bid, sha[:10])
            return str(bid)
        except Exception:  # noqa: BLE001 — the loop records the failure; verify_pending notices the deploy never landed
            log.exception("deploy trigger failed for %s", sha)
            return None

    return trigger
