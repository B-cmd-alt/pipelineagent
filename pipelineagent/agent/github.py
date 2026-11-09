"""Minimal GitHub REST client for the agent: branch, files, PR, status, merge, revert, issue.
No git binary needed. `DryRunGitHub` writes everything it would have sent to a local
directory (and the site bucket, if configured) so the loop can run with no token."""
from __future__ import annotations

import base64
import json
import logging
from dataclasses import dataclass
from pathlib import Path

import requests

log = logging.getLogger("pipelineagent.github")


class GitHubError(RuntimeError):
    pass


@dataclass
class PullRequest:
    number: int
    url: str
    head_sha: str
    branch: str
    dry_run: bool = False


class GitHub:
    dry_run = False

    def __init__(self, token: str, repo: str, base_branch: str = "main", subdir: str = "", api: str = "https://api.github.com"):
        self.repo, self.base_branch, self.subdir, self.api = repo, base_branch, subdir.strip("/"), api
        self.s = requests.Session()
        self.s.headers.update({"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json", "User-Agent": "PipelineAgent/0.1"})

    def _p(self, rel: str) -> str:
        return f"{self.subdir}/{rel}" if self.subdir else rel

    def _req(self, method: str, path: str, **kw):
        r = self.s.request(method, f"{self.api}/repos/{self.repo}{path}", timeout=30, **kw)
        if r.status_code >= 400:
            raise GitHubError(f"{method} {path}: {r.status_code} {r.text[:300]}")
        return r.json() if r.text else {}

    def base_sha(self) -> str:
        return self._req("GET", f"/git/ref/heads/{self.base_branch}")["object"]["sha"]

    def contains(self, ancestor: str, sha: str) -> bool:
        """True when `sha` is `ancestor` or a descendant of it (GitHub compare: identical/ahead)."""
        return self._req("GET", f"/compare/{ancestor}...{sha}").get("status") in ("identical", "ahead")

    def open_pr(self, branch: str, files: dict[str, str], title: str, body: str, verdict: dict) -> PullRequest:
        base = self.base_sha()
        try:
            self._req("POST", "/git/refs", json={"ref": f"refs/heads/{branch}", "sha": base})
        except GitHubError as e:
            if "422" not in str(e):
                raise
        sha = base
        for rel, content in files.items():
            path = self._p(rel)
            payload = {"message": f"agent: {rel}", "content": base64.b64encode(content.encode()).decode(), "branch": branch}
            try:
                existing = self._req("GET", f"/contents/{path}", params={"ref": branch})
                payload["sha"] = existing["sha"]
            except GitHubError:
                pass
            sha = self._req("PUT", f"/contents/{path}", json=payload)["commit"]["sha"]
        vpath = self._p(f"verdicts/{verdict.get('finding_id','x')}.json")
        self._req("PUT", f"/contents/{vpath}", json={"message": "agent: verdict.json", "content": base64.b64encode(json.dumps(verdict, indent=2, default=str).encode()).decode(), "branch": branch})
        pr = self._req("POST", "/pulls", json={"title": title, "body": body, "head": branch, "base": self.base_branch, "draft": False})
        self.set_status(pr["head"]["sha"], verdict)
        return PullRequest(pr["number"], pr["html_url"], pr["head"]["sha"], branch)

    def set_status(self, sha: str, verdict: dict) -> None:
        state = "success" if verdict["verdict"] == "PASS" else "failure"
        self._req("POST", f"/statuses/{sha}", json={"state": state, "context": "proofgate", "description": ",".join(verdict["reason_codes"])[:140]})

    def merge_pr(self, pr: PullRequest) -> str:
        r = self._req("PUT", f"/pulls/{pr.number}/merge", json={"merge_method": "squash", "sha": pr.head_sha})
        return r["sha"]

    def revert(self, merge_sha: str) -> str:
        """Restore the files the merge touched to their pre-merge content via a new commit on base."""
        commit = self._req("GET", f"/commits/{merge_sha}")
        parent = commit["parents"][0]["sha"]
        branch = f"agent/revert-{merge_sha[:10]}"
        self._req("POST", "/git/refs", json={"ref": f"refs/heads/{branch}", "sha": self.base_sha()})
        for f in commit.get("files", []):
            path = f["filename"]
            cur = None
            try:
                cur = self._req("GET", f"/contents/{path}", params={"ref": branch})
            except GitHubError:
                pass
            if f["status"] == "added":
                if cur:
                    self._req("DELETE", f"/contents/{path}", json={"message": f"revert {merge_sha[:10]}: remove {path}", "sha": cur["sha"], "branch": branch})
            else:
                old = self._req("GET", f"/contents/{path}", params={"ref": parent})
                payload = {"message": f"revert {merge_sha[:10]}: restore {path}", "content": old["content"].replace("\n", ""), "branch": branch}
                if cur:
                    payload["sha"] = cur["sha"]
                self._req("PUT", f"/contents/{path}", json=payload)
        pr = self._req("POST", "/pulls", json={"title": f"agent: revert {merge_sha[:10]}", "body": "Automatic revert: the merged repair did not clear its finding on the live detectors.", "head": branch, "base": self.base_branch})
        return self._req("PUT", f"/pulls/{pr['number']}/merge", json={"merge_method": "squash"})["sha"]

    def open_issue(self, finding_id: str, title: str, body: str) -> str:
        r = self._req("POST", "/issues", json={"title": title, "body": body, "labels": ["pipelineagent", "needs-human"]})
        return r["html_url"]

    def close_issue(self, url: str, comment: str) -> bool:
        """Close the issue behind `url` with one comment; False when it was already closed (no second comment)."""
        number = url.rstrip("/").rsplit("/", 1)[-1]
        if self._req("GET", f"/issues/{number}").get("state") == "closed":
            return False
        self._req("POST", f"/issues/{number}/comments", json={"body": comment})
        self._req("PATCH", f"/issues/{number}", json={"state": "closed", "state_reason": "completed"})
        return True

    def publish_pages(self, files: dict[str, bytes], branch: str = "gh-pages") -> str:
        """Replace the `branch` tree with `files` (one commit, no parents needed: the branch is a
        publication, not history). GitHub Pages serves it once the owner points Pages at it."""
        blobs = []
        for path, content in sorted(files.items()):
            b = self._req("POST", "/git/blobs", json={"content": base64.b64encode(content).decode(), "encoding": "base64"})
            blobs.append({"path": path, "mode": "100644", "type": "blob", "sha": b["sha"]})
        tree = self._req("POST", "/git/trees", json={"tree": blobs})["sha"]
        commit = self._req("POST", "/git/commits", json={"message": "PipelineAgent dashboard publish", "tree": tree, "parents": []})["sha"]
        try:
            self._req("PATCH", f"/git/refs/heads/{branch}", json={"sha": commit, "force": True})
        except GitHubError as e:
            if "422" not in str(e) and "404" not in str(e):
                raise
            self._req("POST", "/git/refs", json={"ref": f"refs/heads/{branch}", "sha": commit})
        return commit

    def tarball(self, ref: str) -> bytes:
        r = self.s.get(f"{self.api}/repos/{self.repo}/tarball/{ref}", timeout=60, allow_redirects=True)
        if r.status_code >= 400:
            raise GitHubError(f"tarball {ref}: {r.status_code}")
        return r.content


class DryRunGitHub(GitHub):
    """Same interface, writes artifacts to `out_dir/agent/<branch>/...` instead of calling GitHub."""
    dry_run = True

    def __init__(self, out_dir: Path, base_branch: str = "main"):
        self.out_dir, self.base_branch, self.repo, self.subdir = Path(out_dir), base_branch, "dry-run/local", ""
        self._n = 0

    def base_sha(self) -> str:
        return "0" * 40

    def contains(self, ancestor, sha):
        return True

    def open_pr(self, branch, files, title, body, verdict):
        d = self.out_dir / "agent" / branch.replace("/", "_")
        d.mkdir(parents=True, exist_ok=True)
        for rel, content in files.items():
            p = d / rel
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text(content)
        (d / "PR.md").write_text(f"# {title}\n\n{body}\n")
        (d / "verdict.json").write_text(json.dumps(verdict, indent=2, default=str))
        self._n += 1
        log.info("dry-run PR written to %s", d)
        return PullRequest(self._n, f"dry-run://{d}", "dryrun" + "0" * 34, branch, dry_run=True)

    def set_status(self, sha, verdict):
        return None

    def merge_pr(self, pr):
        raise GitHubError("dry run: nothing to merge")

    def revert(self, merge_sha):
        raise GitHubError("dry run: nothing to revert")

    def publish_pages(self, files, branch="gh-pages"):
        return ""  # dry run: the local site/ directory is the publication

    def close_issue(self, url, comment):
        return False  # dry run: the local issue file is the record

    def open_issue(self, finding_id, title, body):
        d = self.out_dir / "agent" / "issues"
        d.mkdir(parents=True, exist_ok=True)
        (d / f"{finding_id}.md").write_text(f"# {title}\n\n{body}\n")
        return f"dry-run://{d / (finding_id + '.md')}"


def from_env(out_dir: Path) -> GitHub:
    import os

    token, repo = os.environ.get("GITHUB_TOKEN"), os.environ.get("GITHUB_REPO")
    if token and repo:
        return GitHub(token, repo, os.environ.get("GITHUB_BASE_BRANCH", "main"), os.environ.get("GITHUB_SUBDIR", ""))
    return DryRunGitHub(out_dir)
