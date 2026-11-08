"""`python -m pipelineagent <command>`"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from .config import load_fleet
from .warehouse import open_warehouse


def _wh(args):
    return open_warehouse(args.warehouse)


def cmd_version(args) -> int:
    from .version import describe

    print(json.dumps(describe(args.fleet), indent=2))
    return 0


def cmd_init(args) -> int:
    from .runner import ensure_warehouse

    fleet = load_fleet(args.fleet)
    wh = _wh(args)
    if hasattr(wh, "ensure_dataset"):
        wh.ensure_dataset("PipelineAgent: public-feed fleet, detector results, findings/dispositions ledger, heartbeat. Append-only.")
    ensure_warehouse(fleet, wh)
    print("warehouse ready")
    return 0


def cmd_fetch(args) -> int:
    """Pre-flight one feed: fetch and print rows WITHOUT writing anything."""
    from . import feeds as reg
    from .feeds.base import FetchContext
    from .http import Client

    fleet = load_fleet(args.fleet)
    spec = fleet.feed(args.feed)
    wh = _wh(args)
    http = Client(fleet.user_agent, fleet.request_budget_per_run, fleet.min_seconds_between_requests)
    res = reg.get(spec.id).fetch(FetchContext(http, spec.params, None, wh, spec.table, datetime.now(timezone.utc)))
    print(json.dumps({"rows": len(res.rows), "http_status": res.http_status, "content_sha256": res.content_sha256,
                      "source_modified": res.source_modified.isoformat() if res.source_modified else None,
                      "anomalies": res.anomalies, "note": res.note,
                      "sample": [{k: (v.isoformat() if hasattr(v, "isoformat") else v) for k, v in r.items()} for r in res.rows[:3]]},
                     indent=2, default=str))
    return 0


def cmd_run(args) -> int:
    from .heartbeat import beat
    from .runner import run

    fleet = load_fleet(args.fleet)
    wh = _wh(args)
    feeds = args.feeds.split(",") if args.feeds else None
    report = run(fleet, wh, feeds=feeds, fetch=not args.no_fetch, detect=not args.no_detect)
    if not args.no_detect and report.ledger is not None:
        report.heartbeat = beat(wh, report.run_id, feeds_attempted=len(report.runs), feeds_ok=report.feeds_ok,
                                breaches=report.breaches, unknowns=report.unknowns, duration_s=report.duration_s, fleet_path=args.fleet)
        if report.ledger.healthlog_lines():
            from .render_site import append_healthlog

            append_healthlog(report.ledger.healthlog_lines(), args.site_dir, os.environ.get("PIPELINEAGENT_SITE_BUCKET"))
    if not args.no_render:
        from .render_site import render

        out = render(fleet, wh, Path(args.site_dir), bucket=os.environ.get("PIPELINEAGENT_SITE_BUCKET"))
        print(f"site rendered -> {out}")
    print(json.dumps({"run_id": report.run_id, "feeds": {r['feed']: r['status'] for r in report.runs},
                      "breaches": report.breaches, "unknowns": report.unknowns,
                      "opened": len(report.ledger.opened) if report.ledger else None,
                      "closed": len(report.ledger.closed) if report.ledger else None,
                      "duration_s": round(report.duration_s, 1)}, indent=2))
    # exit 0 even with breaches: the exit code is a *process* signal; breaches live in the data + dashboard.
    return 0


def cmd_render(args) -> int:
    from .render_site import render

    fleet = load_fleet(args.fleet)
    out = render(fleet, _wh(args), Path(args.site_dir), bucket=os.environ.get("PIPELINEAGENT_SITE_BUCKET"))
    print(f"site rendered -> {out}")
    return 0


def cmd_dispose(args) -> int:
    from .ledger import dispose
    from .render_site import append_healthlog

    who = f"human:{args.who}" if not args.who.startswith(("human:", "agent")) else args.who
    row = dispose(_wh(args), args.finding_id, args.disposition, who, args.note, datetime.now(timezone.utc), args.evidence_ref)
    append_healthlog([f"- {row['decided_at'].strftime('%Y-%m-%d %H:%M')}Z DISPOSITION `{row['finding_id']}` → {row['disposition']} ({who}: {args.note})"],
                     args.site_dir, os.environ.get("PIPELINEAGENT_SITE_BUCKET"))
    print(json.dumps(row, default=str, indent=2))
    return 0


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s", stream=sys.stdout)
    p = argparse.ArgumentParser(prog="pipelineagent")
    p.add_argument("--fleet", default=os.environ.get("PIPELINEAGENT_FLEET"), help="path to fleet.yaml")
    p.add_argument("--warehouse", default=os.environ.get("PIPELINEAGENT_WAREHOUSE"), help="sqlite:///file.db | bigquery://project/dataset")
    p.add_argument("--site-dir", default=os.environ.get("PIPELINEAGENT_SITE_DIR", "site"))
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("version").set_defaults(fn=cmd_version)
    sub.add_parser("init-warehouse").set_defaults(fn=cmd_init)
    f = sub.add_parser("fetch", help="pre-flight one feed; prints rows, writes nothing")
    f.add_argument("--feed", required=True)
    f.set_defaults(fn=cmd_fetch)
    r = sub.add_parser("run", help="fetch -> detect -> ledger -> heartbeat -> render")
    r.add_argument("--feeds", help="comma-separated subset")
    r.add_argument("--no-fetch", action="store_true")
    r.add_argument("--no-detect", action="store_true")
    r.add_argument("--no-render", action="store_true")
    r.set_defaults(fn=cmd_run)
    sub.add_parser("render").set_defaults(fn=cmd_render)
    d = sub.add_parser("dispose", help="record a human disposition for a finding")
    d.add_argument("finding_id")
    d.add_argument("disposition", choices=["FIXED", "BENIGN", "ESCALATED", "DEFERRED", "UNKNOWN"])
    d.add_argument("--who", required=True)
    d.add_argument("--note", required=True)
    d.add_argument("--evidence-ref", help="required for FIXED: path/URL of the gate verdict JSON")
    d.set_defaults(fn=cmd_dispose)
    args = p.parse_args(argv)
    return args.fn(args)
