"""Static dashboard. Reads the warehouse, writes site/index.html + site/data.json.

Decision-centric by design: the unit of display is a *finding with its
disposition*, not a time series. No JS framework, no hosting cost; the page
is a single self-contained HTML file suitable for GitHub Pages or a bucket.
"""
from __future__ import annotations

import html
import json
import logging
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
RUN_ICON = {"ok": "●", "no_change": "○", "failed": "✕", "skipped": "·"}


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
    data: dict[str, Any] = {"generated_at": now.isoformat(), "project": fleet.project, "renderer": describe(str(fleet.path)), "errors": []}

    def guarded(name, fn, default):
        try:
            return fn()
        except Exception as e:  # noqa: BLE001
            log.exception("site read %s failed", name)
            data["errors"].append(f"{name}: {e.__class__.__name__}: {str(e)[:160]}")
            return default

    hb = guarded("heartbeat", wh.latest_heartbeat, None)
    data["heartbeat"] = heartbeat_status(hb, fleet.heartbeat_max_age_hours, now)
    data["deploy_drift"] = None
    if hb and hb.get("code_sha256"):
        data["deploy_drift"] = {"runtime_code_sha256": hb["code_sha256"], "renderer_code_sha256": data["renderer"]["code_sha256"],
                                "match": hb["code_sha256"] == data["renderer"]["code_sha256"]}

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
                state = d["state"]
                detail = d["detail"]
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
                         "disposition_note": d["note"] if d else "no disposition row (should not happen)"})
    # opened_at for each open finding = first event; approximate from recent events
    events = guarded("events", lambda: wh.recent_finding_events(200), []) or []
    opened_at = {e["finding_id"]: e["event_at"] for e in events if e["event"] == "OPENED"}
    for f in findings:
        oa = opened_at.get(f["finding_id"])
        f["opened_at"] = _iso(oa)
        f["open_for"] = _hours_ago(oa, now) if oa else "unknown"
    findings.sort(key=lambda f: f.get("opened_at") or "", reverse=True)
    data["open_findings"] = findings
    data["open_findings_unavailable"] = open_f is None
    data["recent_events"] = [{k: _iso(v) for k, v in e.items()} for e in events[:40]]
    data["disposition_counts"] = {k: sum(1 for d in disp.values() if d["disposition"] == k) for k in ("FIXED", "BENIGN", "ESCALATED", "DEFERRED", "UNKNOWN")}
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
.sub{color:var(--ink2);font-size:13px}.banner{border-radius:8px;padding:12px 16px;margin:16px 0;font-weight:600;display:flex;gap:12px;align-items:center}
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
.steps{display:grid;grid-template-columns:repeat(5,1fr);gap:6px;margin-top:10px}.steps div{border:1px solid var(--grid);border-radius:6px;padding:6px 8px;font-size:12px}
.steps div.done{border-color:var(--ok)}.steps div b{display:block;font-size:11px;color:var(--ink2);text-transform:uppercase}.steps div.todo{color:var(--muted);border-style:dashed}
code{font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:12px;background:color-mix(in srgb,var(--ink) 6%,transparent);padding:1px 4px;border-radius:4px}
.err{background:var(--breach);color:#fff;padding:8px 12px;border-radius:6px;margin:8px 0}footer{margin-top:40px;color:var(--muted);font-size:12px}
ul.log{list-style:none;padding:0;margin:0}ul.log li{padding:6px 0;border-bottom:1px solid var(--grid);font-size:13px}
@media (max-width:720px){.steps{grid-template-columns:repeat(2,1fr)}th:nth-child(5),td:nth-child(5){display:none}}
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


def render_html(data: dict[str, Any]) -> str:
    hb = data["heartbeat"]
    s = data["summary"]
    if hb["state"] == "ALIVE":
        banner = f'<div class="banner alive">♥ Loop alive — last heartbeat {hb["age_hours"]}h ago (threshold {hb["max_age_hours"]:g}h). Runtime code <code>{_e((hb.get("code_sha256") or "")[:12])}</code>' + (f' @ <code>{_e(hb["git_commit"][:10])}</code>' if hb.get("git_commit") else "") + "</div>"
    elif hb["state"] == "STALE":
        banner = f'<div class="banner stale">⚠ DEAD-MAN\'S SWITCH: last heartbeat {hb["age_hours"]}h ago, over the {hb["max_age_hours"]:g}h limit. The monitor itself is down; every cell below is stale.</div>'
    else:
        banner = '<div class="banner missing">⚠ DEAD-MAN\'S SWITCH: no heartbeat has ever been recorded (or the heartbeat table is unreadable). Nothing below can be trusted.</div>'
    drift = data.get("deploy_drift")
    if drift and not drift["match"]:
        banner += f'<div class="banner drift">⚠ Deploy drift: the running loop reports code <code>{_e(drift["runtime_code_sha256"][:12])}</code> but this page was rendered by <code>{_e(drift["renderer_code_sha256"][:12])}</code>. Committed ≠ deployed until these match.</div>'
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
 <td>{_cell(g['cells']['write_recency'])}</td><td>{_cell(g['cells']['head_lag'])}</td><td>{_cell(g['cells']['flat_value'])}</td>
 <td>{lr_html}</td>
 <td><div class="tl">{tl}</div><span class="detail">{r24['ok']} ok · {r24['no_change']} unchanged · {r24['failed']} failed · {r24['skipped']} skipped · {r24['rows']} rows</span></td>
</tr>""")
    grid_html = f"""<table><thead><tr><th>Feed</th><th>Write recency<br><small>last captured_at vs now</small></th><th>Head lag<br><small>newest event_ts vs now</small></th><th>Flat value<br><small>N identical in a row</small></th><th>Last run</th><th>Last 24h runs</th></tr></thead>
<tbody>{''.join(rows)}</tbody></table>
<p class="sub">Cells are computed from the <em>data tables</em>, never from job exit codes. ✓ OK · ✕ BREACH · ? UNKNOWN = the detector could not look (table missing, read failed, not enough history) and is never shown as healthy · – not declared in fleet.yaml. Hover a cell for the detector's reasoning.</p>"""

    cards = []
    for f in data["open_findings"]:
        cards.append(f"""<div class="card {_e(f['state'])}">
 <h3>{STATE_ICON.get(f['state'], '?')} {_e(f['feed'])} / {_e(AXIS_LABEL.get(f['axis'], f['axis']))} — {_e(f['state'])} <span class="badge {_e(f['disposition'])}">{_e(f['disposition'])}</span></h3>
 <div class="meta">finding <code>{_e(f['finding_id'])}</code> · opened {_e(f.get('opened_at') or '?')} ({_e(f['open_for'])}) · last seen {_e(f['event'])} at {_e(f['event_at'])}</div>
 <div style="margin-top:6px">{_e(f['detail'])}</div>
 <div class="meta" style="margin-top:4px">disposition by <b>{_e(f['disposition_by'])}</b>: {_e(f['disposition_note'])}</div>
 <div class="steps">
  <div class="done"><b>1 · Evidence</b>detector {_e(f['axis'])}: observed {_e(f.get('observed'))} vs threshold {_e(f.get('threshold'))}</div>
  <div class="todo"><b>2 · Diagnosis</b>repair agent not built (v0)</div>
  <div class="todo"><b>3 · Patch</b>—</div>
  <div class="todo"><b>4 · Gate verdict</b>—</div>
  <div class="done"><b>5 · Disposition</b>{_e(f['disposition'])}</div>
 </div>
</div>""")
    if data["open_findings_unavailable"]:
        findings_html = '<div class="err">Open findings could not be read — treat as UNKNOWN, not as "no findings".</div>'
    elif not cards:
        findings_html = '<p class="sub">No open findings. (Verified by reading the ledger, not by absence of alerts.)</p>'
    else:
        findings_html = "".join(cards)

    dc = data["disposition_counts"]
    disp_html = " · ".join(f'<span class="badge {k}">{k}</span> {v}' for k, v in dc.items())
    log_items = "".join(
        f'<li><code>{_e(e["event_at"][:16])}Z</code> <b>{_e(e["event"])}</b> {_e(e["feed"])}/{_e(e["axis"])} {_e(e["state"])} <code>{_e(e["finding_id"])}</code> <span class="sub">{_e((e.get("detail") or "")[:140])}</span></li>'
        for e in data["recent_events"])

    return f"""<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>PipelineAgent Fleet</title><style>{CSS}</style></head><body><main>
<h1>PipelineAgent — fleet health</h1>
<div class="sub">Generated {_e(data['generated_at'][:19])}Z · {len(data['grid'])} feeds · detectors read data, not exit codes · renderer code <code>{_e(data['renderer']['code_sha256'][:12])}</code></div>
{banner}{errs}{tiles}
<h2>Fleet health grid</h2>{grid_html}
<h2>Open findings ({len(data['open_findings'])})</h2>
<p class="sub">Each finding is one continuous not-OK stretch of a feed × axis. It carries exactly one current disposition: {disp_html} (all-time, latest row per finding).</p>
{findings_html}
<h2>Recent ledger events</h2><ul class="log">{log_items or '<li class="sub">none</li>'}</ul>
<footer>PipelineAgent v0 · static page, regenerated after every detector run · <a href="data.json">data.json</a> · HEALTHLOG at <a href="HEALTHLOG.md">ops/HEALTHLOG.md</a></footer>
</main></body></html>"""


def render(fleet: Fleet, wh: Warehouse, site_dir: Path, bucket: str | None = None, now: datetime | None = None) -> Path:
    data = collect(fleet, wh, now)
    site_dir.mkdir(parents=True, exist_ok=True)
    (site_dir / "data.json").write_text(json.dumps(data, indent=1, default=str), encoding="utf-8")
    (site_dir / "index.html").write_text(render_html(data), encoding="utf-8")
    if bucket:
        _upload(bucket, {"index.html": site_dir / "index.html", "data.json": site_dir / "data.json"})
    return site_dir / "index.html"


def healthlog_path(site_dir: str | Path) -> Path:
    return Path(site_dir).resolve().parent / "ops" / "HEALTHLOG.md"


HEALTHLOG_HEADER = "# PipelineAgent HEALTHLOG\n\nAppend-only. One line per ledger event (finding OPENED / CLOSED, disposition changes). Newest at the bottom. Generated by the loop; humans append via `pipelineagent dispose`.\n\n"


def append_healthlog(lines: list[str], site_dir: str | Path, bucket: str | None = None) -> None:
    if not lines:
        return
    text = "\n".join(lines) + "\n"
    p = healthlog_path(site_dir)
    p.parent.mkdir(parents=True, exist_ok=True)
    if bucket:
        # the bucket copy is the durable one (container filesystems are ephemeral): read, append, write back
        try:
            from google.cloud import storage

            blob = storage.Client().bucket(bucket).blob("HEALTHLOG.md")
            existing = blob.download_as_text() if blob.exists() else HEALTHLOG_HEADER
            blob.upload_from_string(existing + text, content_type="text/markdown; charset=utf-8")
        except Exception:  # noqa: BLE001
            log.exception("HEALTHLOG upload failed (local copy still written)")
    if not p.exists():
        p.write_text(HEALTHLOG_HEADER, encoding="utf-8")
    with p.open("a", encoding="utf-8") as fh:
        fh.write(text)


def _upload(bucket: str, files: dict[str, Path]) -> None:
    try:
        from google.cloud import storage

        b = storage.Client().bucket(bucket)
        for name, path in files.items():
            ct = "text/html; charset=utf-8" if name.endswith(".html") else "application/json"
            blob = b.blob(name)
            blob.cache_control = "no-cache, max-age=60"
            blob.upload_from_filename(str(path), content_type=ct)
        log.info("site uploaded to gs://%s/", bucket)
    except Exception:  # noqa: BLE001
        log.exception("site upload failed (local copy still written)")
