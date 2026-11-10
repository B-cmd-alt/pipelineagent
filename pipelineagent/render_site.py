"""Static dashboard. Reads the warehouse, writes site/index.html + data.json + HEALTHLOG.md.

Decision-centric by design: the unit of display is a *finding with its
disposition*, not a time series. No JS framework, no hosting cost; the page
is a single self-contained HTML file suitable for GitHub Pages or a bucket.

The page is rendered by the loop itself, so it cannot announce the loop's own
death; a few lines of client-side JS compare `generated_at` with the viewer's
clock and raise the dead-man banner when the page itself has gone stale.
"""
from __future__ import annotations

import html
import json
import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from .config import AXES, Fleet
from .heartbeat import status as heartbeat_status
from .version import describe
from .warehouse.base import Warehouse

log = logging.getLogger("pipelineagent.site")

AXIS_LABEL = {"write_recency": "Write recency", "head_lag": "Head lag", "flat_value": "Flat value"}
STATE_ICON = {"OK": "✓", "BREACH": "✕", "UNKNOWN": "?", "N/A": "–"}
STEP_ICON = {"ok": "✓", "failed": "✕", "skipped": "·", "dry_run": "◌"}
CHAIN = ("triage", "replay", "patch", "pr", "gate", "merge", "deploy", "verify")
RUN_ICON = {"ok": "●", "no_change": "○", "failed": "✕", "skipped": "·"}
PAGE_STALE_AMBER_HOURS = 3.0  # an hourly loop that has not re-rendered in 3h is already suspicious


def _iso(v: Any) -> Any:
    if isinstance(v, datetime):
        return v.astimezone(timezone.utc).isoformat()
    return v


def _hours_ago(ts: datetime | None, now: datetime) -> str:
    if ts is None:
        return "never"
    h = (now - ts).total_seconds() / 3600
    if h < 1:
        return f"{int(h * 60)}m ago"
    if h < 48:
        return f"{h:.1f}h ago"
    return f"{h / 24:.1f}d ago"


def collect(fleet: Fleet, wh: Warehouse, now: datetime | None = None) -> dict[str, Any]:
    """Everything the page needs, as plain JSON-able data. Each read is isolated:
    a failing read becomes an explicit `unavailable` marker, never an empty-looking success."""
    now = now or datetime.now(timezone.utc)
    data: dict[str, Any] = {"generated_at": now.isoformat(), "project": fleet.project, "renderer": describe(str(fleet.path)),
                            "heartbeat_max_age_hours": fleet.heartbeat_max_age_hours, "errors": []}

    def guarded(name, fn, default):
        try:
            return fn()
        except Exception as e:  # noqa: BLE001
            log.exception("site read %s failed", name)
            data["errors"].append(f"{name}: {e.__class__.__name__}: {str(e)[:160]}")
            return default

    hb = guarded("heartbeat", wh.latest_heartbeat, None)
    data["heartbeat"] = heartbeat_status(hb, fleet.heartbeat_max_age_hours, now)

    dets = guarded("detections", wh.latest_detections, None)
    det_by = {(d["feed"], d["axis"]): d for d in (dets or [])}
    runs = guarded("runs", lambda: wh.runs_since(now - timedelta(hours=24)), None)
    runs_by_feed: dict[str, list[dict]] = {}
    for r in runs or []:
        runs_by_feed.setdefault(r["feed"], []).append(r)

    grid = []
    for spec in fleet.feeds:
        cells = {}
        for axis in AXES:
            if not spec.sla.declared(axis):
                cells[axis] = {"state": "N/A", "detail": "not declared in fleet.yaml"}
                continue
            d = det_by.get((spec.id, axis))
            if dets is None:
                cells[axis] = {"state": "UNKNOWN", "detail": "detections table unreadable"}
            elif d is None:
                cells[axis] = {"state": "UNKNOWN", "detail": "no detector result recorded yet"}
            else:
                age_h = (now - d["detected_at"]).total_seconds() / 3600 if d.get("detected_at") else None
                state, detail = d["state"], d["detail"]
                if age_h is not None and age_h > fleet.heartbeat_max_age_hours:
                    state, detail = "UNKNOWN", f"last detector result is {age_h:.0f}h old: {detail}"
                cells[axis] = {"state": state, "detail": detail, "observed": d.get("observed"), "threshold": d.get("threshold"),
                               "detected_at": _iso(d.get("detected_at"))}
        fr = runs_by_feed.get(spec.id, [])
        last = fr[0] if fr else None
        grid.append({
            "feed": spec.id, "description": spec.description, "cadence": spec.cadence, "table": spec.table,
            "sla": {"write_recency_max_hours": spec.sla.write_recency_max_hours, "head_lag_max_hours": spec.sla.head_lag_max_hours,
                    "flat_value": None if not spec.sla.flat_value else spec.sla.flat_value.__dict__},
            "cells": cells,
            "last_run": None if not last else {"status": last["status"], "reason": last.get("reason"), "finished_at": _iso(last["finished_at"]),
                                                 "rows_written": last["rows_written"], "error": last.get("error"), "ago": _hours_ago(last["finished_at"], now)},
            "runs_24h": {"ok": sum(1 for r in fr if r["status"] == "ok"), "no_change": sum(1 for r in fr if r["status"] == "no_change"),
                         "failed": sum(1 for r in fr if r["status"] == "failed"), "skipped": sum(1 for r in fr if r["status"] == "skipped"),
                         "rows": sum(r["rows_written"] for r in fr)},
            "timeline": [{"t": _iso(r["started_at"]), "status": r["status"], "rows": r["rows_written"], "error": r.get("error")} for r in reversed(fr)],
        })
    data["grid"] = grid
    data["runs_unavailable"] = runs is None

    open_f = guarded("open_findings", wh.open_findings, None)
    disp = guarded("dispositions", wh.latest_dispositions, {}) or {}
    findings = []
    for f in open_f or []:
        d = disp.get(f["finding_id"])
        findings.append({**{k: _iso(v) for k, v in f.items()},
                         "disposition": d["disposition"] if d else "UNKNOWN",
                         "disposition_by": d["decided_by"] if d else "none",
                         "disposition_note": d["note"] if d else "no disposition row (should not happen)",
                         "open_for": _hours_ago(f.get("opened_at"), now)})
    steps = guarded("agent_steps", lambda: wh.agent_steps([f["finding_id"] for f in findings]), []) or []
    by_f: dict[str, list] = {}
    for st in steps:
        by_f.setdefault(st["finding_id"], []).append({k: _iso(v) for k, v in st.items()})
    for f in findings:
        f["agent_steps"] = by_f.get(f["finding_id"], [])
    findings.sort(key=lambda f: f.get("opened_at") or "", reverse=True)
    data["open_findings"] = findings
    data["open_findings_unavailable"] = open_f is None
    data["ledger"] = [{k: _iso(v) for k, v in e.items()} for e in (guarded("ledger", lambda: wh.ledger_events(500), []) or [])]
    repo = os.environ.get("GITHUB_REPO", "")
    data["repo_url"] = f"https://github.com/{repo}" if repo.count("/") == 1 else ""

    def _bench():
        from . import benchmark
        res = benchmark.run(wh, now)
        keep = ("feed", "kind", "failure_class", "triaged_as", "mttd_min", "mttr_min", "outcome", "expected", "verdict",
                "gate_rejections", "prs", "reverts", "evidence_ref", "model_cost_usd")
        return {"totals": res["totals"],
                "episodes": [{**{k: getattr(e, k) for k in keep}, "injected": _iso(e.start), "detected": _iso(e.detected_at)} for e in res["episodes"]]}
    data["benchmark"] = guarded("benchmark", _bench, None)

    def _resolved():
        """The agent's track record: recently settled findings with their full step history, newest first."""
        open_ids = {f["finding_id"] for f in findings}
        opened = {e["finding_id"]: e for e in wh.ledger_events(5000) if e.get("kind") == "OPENED"}
        done = sorted(((fid, d) for fid, d in disp.items() if fid not in open_ids and fid in opened and d["disposition"] in ("FIXED", "ESCALATED", "BENIGN")),
                      key=lambda x: x[1]["decided_at"], reverse=True)[:12]
        st = wh.agent_steps([fid for fid, _ in done]) if done else []
        out = []
        for fid, d in done:
            o = opened[fid]
            out.append({"finding_id": fid, "feed": o["feed"], "axis": o["axis"], "opened_at": _iso(o["ts"]), "detail": o.get("detail"),
                        "disposition": d["disposition"], "decided_by": d["decided_by"], "decided_at": _iso(d["decided_at"]), "note": d.get("note"),
                        "evidence_ref": d.get("evidence_ref"),
                        "agent_steps": [{k: _iso(v) for k, v in x.items()} for x in st if x["finding_id"] == fid]})
        return out
    data["resolved"] = guarded("resolved", _resolved, []) or []
    data["disposition_counts"] = {k: sum(1 for d in disp.values() if d["disposition"] == k) for k in ("FIXED", "BENIGN", "ESCALATED", "DEFERRED", "UNKNOWN")}
    day_ago = (now - timedelta(hours=24)).isoformat()
    led24 = [e for e in data["ledger"] if (e.get("ts") or "") >= day_ago]
    def _drills():
        from . import drillwatch
        return drillwatch.status(fleet, wh, now)
    data["drills"] = guarded("drills", _drills, []) or []
    data["last_24h"] = {
        "runs_failed": sum(g["runs_24h"]["failed"] for g in grid),
        "rows": sum(g["runs_24h"]["rows"] for g in grid),
        "opened": sum(1 for e in led24 if e.get("kind") == "OPENED"),
        "closed": sum(1 for e in led24 if e.get("kind") == "CLOSED"),
        "fixed": sum(1 for e in led24 if e.get("kind") == "DISPOSITION" and e.get("value") == "FIXED"),
        "escalated": sum(1 for e in led24 if e.get("kind") == "DISPOSITION" and e.get("value") == "ESCALATED"),
        "benign": sum(1 for e in led24 if e.get("kind") == "DISPOSITION" and e.get("value") == "BENIGN"),
    }
    data["summary"] = {
        "breach": sum(1 for g in grid for c in g["cells"].values() if c["state"] == "BREACH"),
        "unknown": sum(1 for g in grid for c in g["cells"].values() if c["state"] == "UNKNOWN"),
        "ok": sum(1 for g in grid for c in g["cells"].values() if c["state"] == "OK"),
        "not_declared": sum(1 for g in grid for c in g["cells"].values() if c["state"] == "N/A"),
        "open_findings": len(findings),
    }
    return data


# ----------------------------------------------------------------------------- HTML
CSS = """
:root{--bg:#f9f9f7;--surface:#fcfcfb;--ink:#0b0b0b;--ink2:#52514e;--muted:#898781;--grid:#e1e0d9;--ring:rgba(11,11,11,.10);
 --ok:#0ca30c;--breach:#d03b3b;--unknown:#fab219;--unknown-ink:#0b0b0b;--na:#898781;color-scheme:light}
@media (prefers-color-scheme:dark){:root:not([data-theme=light]){--bg:#0d0d0d;--surface:#1a1a19;--ink:#f0efec;--ink2:#c3c2b7;--grid:#2c2c2a;--ring:rgba(255,255,255,.10);color-scheme:dark}}
:root[data-theme=dark]{--bg:#0d0d0d;--surface:#1a1a19;--ink:#f0efec;--ink2:#c3c2b7;--grid:#2c2c2a;--ring:rgba(255,255,255,.10);color-scheme:dark}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--ink);font:15px/1.45 system-ui,-apple-system,Segoe UI,Roboto,sans-serif}
main{max-width:1180px;margin:0 auto;padding:24px 16px 64px}h1{font-size:22px;margin:0 0 4px}h2{font-size:17px;margin:32px 0 10px}
.sub{color:var(--ink2);font-size:13px}.drill.BREACH{color:var(--breach);font-weight:600}.banner{border-radius:8px;padding:12px 16px;margin:16px 0;font-weight:600;display:flex;gap:12px;align-items:center}
.banner.alive{background:color-mix(in srgb,var(--ok) 14%,var(--surface));border:1px solid var(--ok)}
.banner.stale,.banner.missing{background:var(--breach);color:#fff;font-size:17px}
.banner.drift{background:color-mix(in srgb,var(--unknown) 30%,var(--surface));border:1px solid var(--unknown);color:var(--ink)}
.tiles{display:grid;grid-template-columns:repeat(auto-fit,minmax(140px,1fr));gap:10px;margin:14px 0}
.tile{background:var(--surface);border:1px solid var(--ring);border-radius:8px;padding:10px 12px}.tile b{display:block;font-size:26px;line-height:1.1}
.tile span{color:var(--ink2);font-size:12px}table{width:100%;border-collapse:collapse;background:var(--surface);border:1px solid var(--ring);border-radius:8px;overflow:hidden}
th,td{padding:8px 10px;text-align:left;border-bottom:1px solid var(--grid);vertical-align:top;font-size:14px}th{color:var(--ink2);font-weight:600;font-size:12px;text-transform:uppercase;letter-spacing:.03em}
tr:last-child td{border-bottom:0}.feed b{display:block}.feed small{color:var(--muted)}
.cell{display:inline-flex;align-items:center;gap:6px;border-radius:6px;padding:3px 8px;font-weight:600;font-size:13px;white-space:nowrap}
.cell.OK{background:var(--ok);color:#fff}.cell.BREACH{background:var(--breach);color:#fff}.cell.UNKNOWN{background:var(--unknown);color:var(--unknown-ink)}
.cell.NA{border:1px dashed var(--na);color:var(--na);background:transparent}
.detail{display:block;color:var(--ink2);font-size:12px;margin-top:4px;white-space:normal;max-width:260px}
.tl{display:flex;gap:2px}.tl i{display:block;width:9px;height:18px;border-radius:2px;background:var(--grid)}
.tl i.ok{background:var(--ok)}.tl i.no_change{background:color-mix(in srgb,var(--ok) 35%,var(--surface));border:1px solid var(--ok)}.tl i.failed{background:var(--breach)}.tl i.skipped{background:var(--grid);border:1px dashed var(--muted)}
.card{background:var(--surface);border:1px solid var(--ring);border-left:5px solid var(--breach);border-radius:8px;padding:12px 14px;margin:10px 0}
.card.UNKNOWN{border-left-color:var(--unknown)}.card h3{margin:0 0 6px;font-size:15px}.card .meta{color:var(--ink2);font-size:12px}
.badge{display:inline-block;border-radius:999px;padding:1px 9px;font-size:12px;font-weight:700;border:1px solid var(--ring)}
.badge.UNKNOWN{background:var(--unknown);color:var(--unknown-ink)}.badge.BENIGN{background:var(--ok);color:#fff}.badge.FIXED{background:var(--ok);color:#fff}
.badge.ESCALATED{background:var(--breach);color:#fff}.badge.DEFERRED{border-color:var(--muted);color:var(--ink2)}
.chain{display:flex;flex-wrap:wrap;gap:4px;margin-top:8px}.chain span{font-size:12px;border:1px solid var(--grid);border-radius:999px;padding:1px 8px;color:var(--ink2)}
.chain span.ok{border-color:var(--ok);color:var(--ink)}.chain span.failed{border-color:var(--breach);color:var(--ink)}.chain span.dry_run{border-style:dashed}.chain span.todo{color:var(--muted);border-style:dotted}
.chain small{display:block;color:var(--muted);font-size:11px;margin-top:4px}
code{font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:12px;background:color-mix(in srgb,var(--ink) 6%,transparent);padding:1px 4px;border-radius:4px}
.err{background:var(--breach);color:#fff;padding:8px 12px;border-radius:6px;margin:8px 0}footer{margin-top:40px;color:var(--muted);font-size:12px}
#page-stale{display:none}#page-stale.amber{display:flex;background:color-mix(in srgb,var(--unknown) 30%,var(--surface));border:1px solid var(--unknown);color:var(--ink)}#page-stale.red{display:flex;background:var(--breach);color:#fff;font-size:17px}
.hero{margin:6px 0 18px}.hero h1{font-size:28px;margin:0}.tag{font-size:17px;color:var(--ink);margin:4px 0 10px;max-width:780px}
.hero p{max-width:780px;color:var(--ink2);margin:6px 0}.links{display:flex;flex-wrap:wrap;gap:8px;margin-top:10px}
.links a,.btn{display:inline-block;border:1px solid var(--ring);border-radius:999px;padding:4px 12px;font-size:13px;color:var(--ink);text-decoration:none;background:var(--surface)}
.links a:hover,.btn:hover{border-color:var(--ink2)}a{color:inherit}
.live{display:flex;flex-wrap:wrap;gap:6px 16px;align-items:center;font-size:13px;color:var(--ink2);margin:8px 0}.live b{color:var(--ink)}
.dot{display:inline-block;width:8px;height:8px;border-radius:50%;background:var(--ok);margin-right:6px;animation:pulse 2s infinite}
@keyframes pulse{50%{opacity:.35}}@media (prefers-reduced-motion:reduce){.dot{animation:none}}
#fresh{display:none}#fresh.on{display:inline-block;cursor:pointer}
.flow{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:8px;margin:8px 0}
.flow details{background:var(--surface);border:1px solid var(--ring);border-radius:8px;padding:8px 10px}
.flow summary{cursor:pointer;font-weight:600;font-size:14px;list-style:none}.flow summary::-webkit-details-marker{display:none}
.flow summary small{display:block;font-weight:400;color:var(--ink2);font-size:12px}.flow details p{font-size:13px;color:var(--ink2);margin:6px 0 0}
.flow details[open]{border-color:var(--ink2)}.lane{font-size:12px;text-transform:uppercase;letter-spacing:.04em;color:var(--muted);margin:14px 0 2px}
.flow .model{border-style:dashed}.flow .judge{border-width:2px}
.case{background:var(--surface);border:1px solid var(--ring);border-left:5px solid var(--ok);border-radius:8px;padding:12px 14px;margin:10px 0}
.case.ESCALATED{border-left-color:var(--muted)}.case.BENIGN{border-left-color:var(--ok)}
.case h3{margin:0 0 4px;font-size:15px}.tline{list-style:none;margin:8px 0 0;padding:0;border-left:2px solid var(--grid)}
.tline li{position:relative;padding:2px 0 6px 14px;font-size:13px;color:var(--ink2)}.tline li::before{content:"";position:absolute;left:-6px;top:7px;width:10px;height:10px;border-radius:50%;background:var(--grid)}
.tline li.ok::before{background:var(--ok)}.tline li.failed::before{background:var(--breach)}.tline li b{color:var(--ink)}.tline time{font-variant-numeric:tabular-nums;color:var(--muted);margin-right:6px}
details.more>summary{cursor:pointer;font-size:13px;color:var(--ink2);margin-top:6px}
.verdict{font-weight:700}.verdict.CORRECT{color:var(--ok)}.verdict.WRONG,.verdict.MISSED{color:var(--breach)}
.wrap{overflow-x:auto}.kpi{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:10px;margin:10px 0}
.feedgroup>summary{cursor:pointer;font-weight:600;margin:10px 0 4px}
@media (max-width:720px){.grid thead{display:none}.grid tr{display:block;border-bottom:1px solid var(--grid);padding:6px 0}.grid td{display:block;border:0;padding:4px 10px}
 .grid td[data-label]::before{content:attr(data-label);display:block;font-size:11px;text-transform:uppercase;letter-spacing:.03em;color:var(--muted)}
 .detail{max-width:none}.banner{flex-wrap:wrap;font-size:14px}.banner code{word-break:break-all}.hero h1{font-size:24px}}
"""


def _e(s: Any) -> str:
    return html.escape("" if s is None else str(s))


def _cell(c: dict) -> str:
    st = c["state"]
    cls = "NA" if st == "N/A" else st
    label = "not declared" if st == "N/A" else st
    obs = ""
    if c.get("observed") is not None and c.get("threshold") is not None:
        obs = f" <small>{c['observed']:g} / {c['threshold']:g}</small>"
    return f'<span class="cell {cls}" title="{_e(c.get("detail"))}">{STATE_ICON[st]} {label}{obs}</span><span class="detail">{_e(c.get("detail"))}</span>'


# Client-side dead-man's switch: the page cannot know the loop died, but the viewer's clock can.
STALE_JS = """
(function(){var g=Date.parse(document.body.dataset.generatedAt);var max=parseFloat(document.body.dataset.maxAgeHours);
var h=(Date.now()-g)/36e5;var el=document.getElementById("page-stale");if(!el||isNaN(h))return;
if(h>max){el.className="banner red";el.textContent="\u26a0 DEAD-MAN SWITCH: this page was rendered "+h.toFixed(1)+"h ago, over the "+max+"h limit. The loop that renders it has stopped; every cell below is stale.";}
else if(h>%s){el.className="banner amber";el.textContent="\u26a0 Page rendered "+h.toFixed(1)+"h ago; the loop is hourly, so it may be falling behind.";}})();
""" % PAGE_STALE_AMBER_HOURS


# Live strip: the age of this render, the next scheduled run (hourly at :23 UTC), and a refresh prompt
# when data.json is newer than the page. No reload without asking, so an expanded timeline stays open.
LIVE_JS = """
(function(){var g=Date.parse(document.body.dataset.generatedAt);function tick(){var m=Math.round((Date.now()-g)/6e4);
var a=document.getElementById("age");if(a&&!isNaN(m))a.textContent="rendered "+(m<60?m+" min":(m/60).toFixed(1)+" h")+" ago";
var d=new Date(),n=new Date(Date.UTC(d.getUTCFullYear(),d.getUTCMonth(),d.getUTCDate(),d.getUTCHours(),23));if(n<=d)n=new Date(n.getTime()+36e5);
var x=document.getElementById("next");if(x)x.textContent="next run in "+Math.round((n-d)/6e4)+" min (hourly at :23 UTC)";}
function poll(){if(!window.fetch)return;fetch("data.json",{cache:"no-store"}).then(function(r){return r.json()}).then(function(j){
if(Date.parse(j.generated_at)>g){var f=document.getElementById("fresh");if(f)f.className="btn on";}}).catch(function(){});}
tick();setInterval(tick,30000);setInterval(poll,60000);})();
"""


def _chain(steps: list[dict]) -> str:
    """The agent's decision chain for one finding: latest outcome per step, in order, plus
    the last attempt's detail. Steps not reached yet are dotted, never green."""
    if not steps:
        return '<div class="chain"><span class="todo">agent: not yet run on this finding</span></div>'
    last_attempt = max(int(s.get("attempt") or 0) for s in steps)
    latest: dict[str, dict] = {}
    for st in steps:
        if int(st.get("attempt") or 0) == last_attempt or st["step"] in ("escalate", "defer", "revert", "join", "issue_closed", "scenario_draft"):
            latest[st["step"]] = st
    pills = []
    for step in CHAIN:
        st = latest.get(step)
        if st:
            pills.append(f'<span class="{_e(st["outcome"])}" title="{_e(st.get("detail"))}">{STEP_ICON.get(st["outcome"], "?")} {step}</span>')
        else:
            pills.append(f'<span class="todo">{step}</span>')
    for extra in ("join", "revert", "escalate", "defer", "issue_closed", "scenario_draft"):
        if extra in latest:
            st = latest[extra]
            pills.append(f'<span class="{_e(st["outcome"])}" title="{_e(st.get("detail"))}">{STEP_ICON.get(st["outcome"], "?")} {extra}</span>')
    tail = sorted(steps, key=lambda s: s.get("ts") or "")[-1]
    ref = f' · <a href="{_e(tail["ref"])}">{_e(tail["ref"][:60])}</a>' if tail.get("ref") and str(tail["ref"]).startswith("http") else ""
    return (f'<div class="chain">{"".join(pills)}<small>attempt {last_attempt} · last: {_e(tail["step"])} {_e(tail["outcome"])} — {_e((tail.get("detail") or "")[:160])}{ref}</small></div>')


def _link(ref: Any, label: str | None = None) -> str:
    ref = str(ref or "")
    if not ref.startswith("http"):
        return ""
    if label is None:
        label = ("PR #" if "/pull/" in ref else "issue #" if "/issues/" in ref else "") + ref.rstrip("/").rsplit("/", 1)[-1]
    return f'<a href="{_e(ref)}">{_e(label)}</a>'


# Steps written in one run share a timestamp; this is the order the agent takes them in.
STEP_ORDER = {k: i for i, k in enumerate(("verify", "revert", "triage", "join", "defer", "patch", "replay", "gate", "pr", "merge", "deploy",
                                          "escalate", "issue_closed", "scenario_draft", "drill_alarm"))}


def _ordered(steps: list[dict]) -> list[dict]:
    return sorted(steps, key=lambda x: (x.get("ts") or "", STEP_ORDER.get(x["step"], 99)))


def _final_attempt(steps: list[dict]) -> list[dict]:
    """The run of steps that produced the outcome: from the last triage onward."""
    st = _ordered(steps)
    starts = [i for i, x in enumerate(st) if x["step"] == "triage"]
    return st[starts[-1]:] if starts else st


def _timeline(steps: list[dict]) -> str:
    """Every step the agent took on a finding, in time order: what it decided and why."""
    if not steps:
        return '<p class="sub">No agent steps recorded.</p>'
    items = []
    for st in _ordered(steps):
        ref = _link(st.get("ref"))
        att = f" · attempt {st['attempt']}" if int(st.get("attempt") or 0) > 0 else ""
        items.append(f'<li class="{_e(st["outcome"])}"><time>{_e((st.get("ts") or "")[5:16].replace("T", " "))}Z</time><b>{_e(st["step"])}</b> {_e(st["outcome"])}{att}'
                     f' — {_e(str(st.get("detail") or "")[:260])}' + (f" · {ref}" if ref else "") + "</li>")
    return f'<ol class="tline">{"".join(items)}</ol>'


def _case(c: dict) -> str:
    who = "the agent" if c["decided_by"] == "agent" else ("a person" if str(c["decided_by"]).startswith("human:") else "the ledger rule")
    ev = _link(c.get("evidence_ref"))
    return f"""<div class="case {_e(c['disposition'])}">
 <h3>{_e(c['feed'])} / {_e(AXIS_LABEL.get(c['axis'], c['axis']))} <span class="badge {_e(c['disposition'])}">{_e(c['disposition'])}</span></h3>
 <div class="meta">opened {_e((c.get('opened_at') or '')[:16])}Z · settled {_e((c.get('decided_at') or '')[:16])}Z by {who}{(' · ' + ev) if ev else ''}</div>
 <div class="sub" style="margin-top:4px">{_e(str(c.get('note') or '')[:240])}</div>
 <details class="more"><summary>Every step ({len(c.get('agent_steps') or [])})</summary>{_timeline(c.get('agent_steps') or [])}</details>
</div>"""


FLOW = (
    ("Watch", (
        ("Fetch", "7 public feeds + a canary", "Hourly. Every raw response is archived by content hash, so any failure can be replayed byte for byte later.", ""),
        ("Store", "append-only, vintaged", "Rows are never overwritten; each carries captured_at. The warehouse is the only source the detectors read.", ""),
        ("Detect", "data, not exit codes", "Three axes per feed: how long since we wrote, how old the data's own clock is, whether values froze. UNKNOWN is never green.", ""),
        ("Ledger", "findings + dispositions", "A finding is one not-OK stretch. It carries exactly one current disposition, and FIXED requires evidence.", ""),
    )),
    ("Act", (
        ("Triage", "a rule table, no model", "Schema drift is patchable; a timed-out source, a green job with dead data, a frozen value are not, and are escalated with one issue.", ""),
        ("Replay", "reproduce before fixing", "The archived failing bytes are fed to the current code in a sandbox with the network off. If the failure does not reproduce, nothing is patched.", ""),
        ("Patch", "the only model call", "A model proposes a new fetcher module and one test. It only proposes; it never decides.", "model"),
        ("Gate", "a script is the judge", "Diff linter (no SLA, detector, test or policy edits), replay (failing bytes must land rows, healthy bytes must not change) and the whole test suite.", "judge"),
        ("Merge + deploy", "automatic on PASS", "Squash merge, Cloud Build, and a check that the running job's code hash matches the commit.", ""),
        ("Verify", "the next live runs decide", "Cleared within two runs: FIXED with the PR as evidence. Still open: automatic revert. Three failures: ESCALATED.", "judge"),
    )),
    ("Guard the guard", (
        ("Scenario evals", "every change, offline", "Recorded drills and incidents replayed through the real code; guarded so answers cannot drift silently.", ""),
        ("Weekly drills", "the live path", "A canary is broken on purpose every Wednesday; a wrong or missed verdict opens an issue.", ""),
        ("Dead-man switch", "the monitor's own pulse", "No heartbeat for 26 hours turns this whole page red, computed in your browser.", ""),
        ("Learning", "bounded", "Findings a person settles become replayable test drafts. The system grows its tests, never edits its own judge.", ""),
    )),
)


def _flow() -> str:
    out = []
    for lane, nodes in FLOW:
        cards = "".join(f'<details class="{cls}"><summary>{_e(t)}<small>{_e(sub)}</small></summary><p>{_e(txt)}</p></details>' for t, sub, txt, cls in nodes)
        out.append(f'<div class="lane">{_e(lane)}</div><div class="flow">{cards}</div>')
    return "".join(out) + '<p class="sub">Click a step for what it does. Dashed: the model. Bold: a deterministic judge.</p>'


def _bench_html(b: dict | None) -> str:
    if not b:
        return '<p class="sub">Benchmark unavailable.</p>'
    t = b["totals"]
    rows = []
    for e in b["episodes"]:
        rows.append(f"<tr><td>{_e(e['feed'])}</td><td>{_e(e['failure_class'])}<br><small class='sub'>{_e(e['kind'])}</small></td>"
                    f"<td>{_e(e.get('triaged_as') or '—')}</td><td>{_e((e.get('injected') or '')[:16])}Z</td>"
                    f"<td>{'' if e['mttd_min'] is None else _e(e['mttd_min'])}</td><td>{_e(e.get('outcome') or '—')}</td><td>{_e(e['expected'])}</td>"
                    f"<td>{'' if e['mttr_min'] is None else _e(e['mttr_min'])}</td><td>{_e(e['gate_rejections'])}</td>"
                    f"<td class='verdict {_e(str(e['verdict']).split(' ')[0])}'>{_e(e['verdict'])}</td><td>{_link(e.get('evidence_ref'))}</td></tr>")
    kpi = f"""<div class="kpi">
 <div class="tile"><b>{t['correct']}/{t['episodes']}</b><span>injected failures ended as they must</span></div>
 <div class="tile"><b>{t.get('triage_exact', 0)}/{t.get('triaged', 0)}</b><span>classified as the injected class</span></div>
 <div class="tile"><b>{t['median_mttd_min'] if t['median_mttd_min'] is not None else '—'}</b><span>median minutes to detect (the SLA is the floor)</span></div>
 <div class="tile"><b>{t['median_mttr_min'] if t['median_mttr_min'] is not None else '—'}</b><span>median minutes from detection to the agent's answer</span></div>
</div>"""
    return kpi + f"""<div class="wrap"><table><thead><tr><th>Feed</th><th>Injected</th><th>Triaged as</th><th>At (UTC)</th><th>MTTD min</th><th>Outcome</th><th>Expected</th><th>MTTR min</th><th>Gate rejects</th><th>Verdict</th><th>Evidence</th></tr></thead>
<tbody>{''.join(rows) or '<tr><td colspan="11">No drills yet.</td></tr>'}</tbody></table></div>
<p class="sub">Faults are injected between the HTTP client and the parser; everything downstream reacts as to a real upstream change. Baseline: detectors without the agent find the same failures at the same time, then wait for a person. Detection time is bounded below by each feed's SLA: 3 hours for the hourly feeds, 30 hours for IESO's once-a-day file.</p>"""


def render_html(data: dict[str, Any]) -> str:
    hb = data["heartbeat"]
    s = data["summary"]
    if hb["state"] == "ALIVE":
        banner = f'<div class="banner alive">♥ Loop alive — last heartbeat {hb["age_hours"]}h ago at render time (threshold {hb["max_age_hours"]:g}h). Runtime code <code>{_e((hb.get("code_sha256") or "")[:12])}</code>' + (f' @ <code>{_e(hb["git_commit"][:10])}</code>' if hb.get("git_commit") else "") + "</div>"
    elif hb["state"] == "STALE":
        banner = f'<div class="banner stale">⚠ DEAD-MAN\'S SWITCH: last heartbeat {hb["age_hours"]}h ago, over the {hb["max_age_hours"]:g}h limit. The monitor itself is down; every cell below is stale.</div>'
    else:
        banner = '<div class="banner missing">⚠ DEAD-MAN\'S SWITCH: no heartbeat has ever been recorded (or the heartbeat table is unreadable). Nothing below can be trusted.</div>'
    banner += '<div id="page-stale" class="banner"></div>'
    errs = "".join(f'<div class="err">Dashboard read failed — {_e(x)}</div>' for x in data["errors"])

    tiles = f"""<div class="tiles">
 <div class="tile"><b style="color:var(--breach)">{s['breach']}</b><span>✕ BREACH cells</span></div>
 <div class="tile"><b style="color:#b8800a">{s['unknown']}</b><span>? UNKNOWN cells (we could not look)</span></div>
 <div class="tile"><b style="color:var(--ok)">{s['ok']}</b><span>✓ OK cells</span></div>
 <div class="tile"><b>{s['not_declared']}</b><span>– not declared</span></div>
 <div class="tile"><b>{s['open_findings']}</b><span>open findings</span></div>
</div>"""

    rows = []
    for g in data["grid"]:
        lr = g["last_run"]
        if lr:
            lr_html = f'<span class="cell" style="background:transparent;border:1px solid var(--ring);color:var(--ink)">{RUN_ICON.get(lr["status"], "?")} {_e(lr["status"])}</span><span class="detail">{_e(lr["ago"])}, {lr["rows_written"]} rows' + (f' — {_e(lr["error"][:120])}' if lr.get("error") else "") + "</span>"
        else:
            lr_html = '<span class="cell UNKNOWN">? no run in 24h</span>'
        tl = "".join(f'<i class="{_e(t["status"])}" title="{_e(t["t"])} {_e(t["status"])} rows={t["rows"]} {_e((t.get("error") or "")[:80])}"></i>' for t in g["timeline"][-24:])
        r24 = g["runs_24h"]
        rows.append(f"""<tr>
 <td class="feed"><b>{_e(g['feed'])}</b><small>{_e(g['cadence'])} · {_e(g['description'])}</small></td>
 <td data-label="Write recency">{_cell(g['cells']['write_recency'])}</td><td data-label="Head lag">{_cell(g['cells']['head_lag'])}</td><td data-label="Flat value">{_cell(g['cells']['flat_value'])}</td>
 <td data-label="Last run">{lr_html}</td>
 <td data-label="Last 24h runs"><div class="tl">{tl}</div><span class="detail">{r24['ok']} ok · {r24['no_change']} unchanged · {r24['failed']} failed · {r24['skipped']} skipped · {r24['rows']} rows</span></td>
</tr>""")
    grid_html = f"""<table class="grid"><thead><tr><th>Feed</th><th>Write recency<br><small>last captured_at vs now</small></th><th>Head lag<br><small>newest event_ts vs now</small></th><th>Flat value<br><small>N identical in a row</small></th><th>Last run</th><th>Last 24h runs</th></tr></thead>
<tbody>{''.join(rows)}</tbody></table>
<p class="sub">Cells are computed from the <em>data tables</em>, never from job exit codes. ✓ OK · ✕ BREACH · ? UNKNOWN = the detector could not look (table missing, read failed, not enough history) and is never shown as healthy · – not declared in fleet.yaml. Hover a cell for the detector's reasoning.</p>"""

    cards = []
    for f in data["open_findings"]:
        f["chain_html"] = _chain(f.get("agent_steps") or [])
        cards.append(f"""<div class="card {_e(f['state'])}">
 <h3>{STATE_ICON.get(f['state'], '?')} {_e(f['feed'])} / {_e(AXIS_LABEL.get(f['axis'], f['axis']))} — {_e(f['state'])} <span class="badge {_e(f['disposition'])}">{_e(f['disposition'])}</span></h3>
 <div class="meta">finding <code>{_e(f['finding_id'])}</code> · opened {_e((f.get('opened_at') or '?')[:16])}Z ({_e(f['open_for'])})</div>
 <div style="margin-top:6px"><b>Evidence:</b> {_e(f['detail'])} <span class="sub">(observed {_e(f.get('observed'))} vs threshold {_e(f.get('threshold'))})</span></div>
 <div class="meta" style="margin-top:4px"><b>Disposition</b> by {_e(f['disposition_by'])}: {_e(f['disposition_note'])}</div>
 {f['chain_html']}
 <details class="more"><summary>Every step ({len(f.get('agent_steps') or [])})</summary>{_timeline(f.get('agent_steps') or [])}</details>
</div>""")
    if data["open_findings_unavailable"]:
        findings_html = '<div class="err">Open findings could not be read — treat as UNKNOWN, not as "no findings".</div>'
    elif not cards:
        findings_html = '<p class="sub">No open findings. (Verified by reading the ledger, not by absence of alerts.)</p>'
    else:
        by_feed: dict[str, list[str]] = {}
        for f, card in zip(data["open_findings"], cards):
            by_feed.setdefault(f["feed"], []).append(card)
        findings_html = "".join(f'<details class="feedgroup" open><summary>{_e(feed)} ({len(cs)})</summary>{"".join(cs)}</details>' for feed, cs in by_feed.items())

    drills_html = ""
    for dr in data.get("drills") or []:
        last = dr["last"]
        if dr["alarms"]:
            txt = "⚠ SELF-TEST FAILED: " + "; ".join(_e(a["why"]) for a in dr["alarms"])
        elif last:
            ev = f' · <a href="{_e(last["evidence"])}">evidence</a>' if last.get("evidence") else ""
            txt = (f'Self-test on <code>{_e(dr["feed"])}</code>: {dr["correct"]}/{dr["episodes"]} drills correct · last: {_e(last["class"])} injected {_e(last["injected"][:16])}Z → '
                   f'<b>{_e(last["verdict"])}</b>' + (f' ({_e(last["outcome"])}, detected in {last["mttd_min"]} min)' if last.get("outcome") else "") + ev)
        else:
            txt = f'Self-test on <code>{_e(dr["feed"])}</code>: no drill has run yet (scheduled weekly).'
        drills_html += f'<p class="sub drill {_e(dr["state"])}">{txt}</p>'
    repo = data.get("repo_url") or ""
    links = [("#benchmark", "Results"), ("#how", "How it works"), ("#fleet", "Live fleet")]
    if repo:
        links += [(repo, "Source on GitHub"), (f"{repo}/blob/main/DESIGN.md", "Design"), (f"{repo}/blob/main/ops/DRILL.md", "Drill records"),
                  (f"{repo}/tree/main/evals", "Evals")]
    links_html = "".join(f'<a href="{_e(h)}">{_e(t)}</a>' for h, t in links)
    resolved = data.get("resolved") or []
    fixed = [c for c in resolved if c["disposition"] == "FIXED" and c["decided_by"] == "agent"]
    if fixed:
        h = fixed[0]
        allst = h.get("agent_steps") or []
        final = _final_attempt(allst)
        rejected = sum(1 for x in allst if x["step"] == "gate" and x["outcome"] == "failed")
        hero_html = (f'<p class="sub">Latest repair, start to finish, with no person touching the code: the source changed shape, the agent reproduced the failure '
                     f'from archived bytes, a model proposed a patch, a script approved it, it merged and deployed itself, and the next live run confirmed it.</p>'
                     + _case({**h, "agent_steps": final}).replace('<details class="more">', '<details class="more" open>', 1).replace("Every step (", "The successful attempt (", 1)
                     + (f'<details class="more"><summary>Full history: {len(allst)} steps, {rejected} gate rejections before this attempt (why: <a href="{_e(repo)}/blob/main/ops/DRILL.md">drill record</a>)</summary>{_timeline(allst)}</details>'
                        if len(allst) > len(final) else ""))
    else:
        hero_html = '<p class="sub">No repair recorded yet.</p>'
    others = [c for c in resolved if not fixed or c is not fixed[0]]
    settled_html = "".join(_case(c) for c in others) or '<p class="sub">Nothing settled yet.</p>'
    bench_html = _bench_html(data.get("benchmark"))
    flow_html = _flow()
    d24 = data.get("last_24h") or {k: 0 for k in ("runs_failed", "rows", "opened", "closed", "fixed", "escalated", "benign")}
    dc = data["disposition_counts"]
    disp_html = " · ".join(f'<span class="badge {k}">{k}</span> {v}' for k, v in dc.items())

    return f"""<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>PipelineAgent Fleet</title><style>{CSS}</style></head>
<body data-generated-at="{_e(data['generated_at'])}" data-max-age-hours="{data['heartbeat_max_age_hours']:g}"><main>
<header class="hero">
 <h1>PipelineAgent</h1>
 <p class="tag">An agent that watches data pipelines, repairs the breaks it can, opens a pull request and merges it after a scripted gate passes, and hands the rest to a person with one issue.</p>
 <p>Most monitoring asks whether a job exited 0. This asks whether fresh data landed. A job that succeeds while writing nothing is the failure it is built around.</p>
 <p>Everything below is live: {len(data['grid'])} public feeds, checked every hour, rendered by the loop itself.</p>
 <div class="links">{links_html}</div>
</header>
{banner}{errs}
<div class="live"><span><span class="dot"></span><b id="age">rendered {_e(data['generated_at'][11:16])}Z</b></span><span id="next"></span><span>code <code>{_e(data['renderer']['code_sha256'][:12])}</code></span><a id="fresh" class="btn" onclick="location.reload()">New data: refresh</a></div>
{drills_html}
<h2 id="record">What the agent has done</h2>
{hero_html}
<h2 id="benchmark">Does it work? Injected failures, scored</h2>
{bench_html}
<h2 id="how">How it works</h2>
{flow_html}
<h2 id="fleet">The fleet, right now</h2>
{tiles}
<p class="sub day">Last 24h: {d24["runs_failed"]} failed runs · {d24["rows"]} rows landed · findings {d24["opened"]} opened / {d24["closed"]} closed · dispositions {d24["fixed"]} FIXED · {d24["escalated"]} ESCALATED · {d24["benign"]} BENIGN · {s["unknown"]} UNKNOWN cells right now (blind, never green)</p>
{grid_html}
<h2 id="open">Open findings ({len(data['open_findings'])})</h2>
<p class="sub">Each finding is one continuous not-OK stretch of a feed × axis. It carries exactly one current disposition: {disp_html} (all-time, latest row per finding).</p>
{findings_html}
<h2 id="settled">Recently settled</h2>
{settled_html}
<footer>PipelineAgent · static page, regenerated after every hourly run · <a href="data.json">data.json</a> · <a href="HEALTHLOG.md">HEALTHLOG.md</a> (every ledger event, generated from the tables)</footer>
</main><script>{STALE_JS}</script><script>{LIVE_JS}</script></body></html>"""


def render_healthlog(data: dict[str, Any]) -> str:
    lines = ["# PipelineAgent HEALTHLOG", "",
             f"Generated {data['generated_at'][:19]}Z from the append-only `findings` and `dispositions` tables (oldest first). "
             "OPENED/CLOSED are detector transitions; DISPOSITION rows are automatic or human decisions.", ""]
    for e in data["ledger"]:
        who = f" ({e['actor']})" if e.get("actor") and e["actor"] != "detector" else ""
        where = f" {e['feed']}/{e['axis']}" if e.get("feed") else ""
        lines.append(f"- {(e['ts'] or '')[:16]}Z {e['kind']} `{e['finding_id']}`{where} {e.get('value') or ''}{who} — {e.get('detail') or ''}")
    if not data["ledger"]:
        lines.append("- (no ledger events yet)")
    return "\n".join(lines) + "\n"


def render(fleet: Fleet, wh: Warehouse, site_dir: Path, bucket: str | None = None, now: datetime | None = None, github=None) -> Path:
    data = collect(fleet, wh, now)
    site_dir.mkdir(parents=True, exist_ok=True)
    (site_dir / "data.json").write_text(json.dumps(data, indent=1, default=str), encoding="utf-8")
    (site_dir / "index.html").write_text(render_html(data), encoding="utf-8")
    (site_dir / "HEALTHLOG.md").write_text(render_healthlog(data), encoding="utf-8")
    if github is not None and os.environ.get("PIPELINEAGENT_PAGES", "1") == "1":
        try:
            sha = github.publish_pages({"index.html": (site_dir / "index.html").read_bytes(), "data.json": (site_dir / "data.json").read_bytes(),
                                        "HEALTHLOG.md": (site_dir / "HEALTHLOG.md").read_bytes(), ".nojekyll": b""})
            if sha:
                log.info("dashboard published to gh-pages (%s)", sha[:10])
        except Exception:  # noqa: BLE001
            log.exception("gh-pages publish failed (bucket/local copies still written)")
    if bucket:
        files = {"index.html": site_dir / "index.html", "data.json": site_dir / "data.json", "HEALTHLOG.md": site_dir / "HEALTHLOG.md"}
        # the agent's dry-run artifacts (PR drafts, issues it would have opened) live under site/agent/
        for p in (site_dir / "agent").rglob("*"):
            if p.is_file():
                files[p.relative_to(site_dir).as_posix()] = p
        _upload(bucket, files)
    return site_dir / "index.html"


def _upload(bucket: str, files: dict[str, Path]) -> None:
    try:
        from google.cloud import storage

        b = storage.Client().bucket(bucket)
        types = {".html": "text/html; charset=utf-8", ".json": "application/json", ".md": "text/markdown; charset=utf-8"}
        for name, path in files.items():
            blob = b.blob(name)
            blob.cache_control = "no-cache, max-age=60"
            blob.upload_from_filename(str(path), content_type=types.get(path.suffix, "application/octet-stream"))
        log.info("site uploaded to gs://%s/", bucket)
    except Exception:  # noqa: BLE001
        log.exception("site upload failed (local copy still written)")
