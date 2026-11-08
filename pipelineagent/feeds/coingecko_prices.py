"""CoinGecko simple/price. Free tier rate-limits hard (HTTP 429), which the
client surfaces as RateLimited -> a `failed` run, not a crash."""
from __future__ import annotations

import json
from datetime import datetime, timezone

from ..schema import Column
from .base import Feed, FetchContext, FetchResult, to_float

COLUMNS = (
    Column("coin", "STRING", True),
    Column("price_usd", "FLOAT64"),
)


def parse(payload: dict, coins: list[str]) -> tuple[list[dict], list[str]]:
    anomalies: list[str] = []
    rows = []
    for c in coins:
        d = payload.get(c)
        if not isinstance(d, dict):
            anomalies.append(f"{c}: missing from response")
            continue
        if "usd" not in d:
            # a renamed/missing key is schema drift and must FAIL the run (the repair agent's trigger);
            # a null value under the right key is data and stays a NULL with an anomaly
            raise ValueError(f"schema: expected key 'usd' under {c}; got {sorted(d)}")
        lu = d.get("last_updated_at")
        rows.append({
            "coin": c,
            "event_ts": datetime.fromtimestamp(lu, tz=timezone.utc) if lu else None,
            "price_usd": to_float(d.get("usd"), anomalies, f"{c}.usd"),
        })
    return rows, anomalies


def fetch(ctx: FetchContext) -> FetchResult:
    coins = list(ctx.params.get("coins") or ["bitcoin"])
    r = ctx.http.get("https://api.coingecko.com/api/v3/simple/price",
                     params={"ids": ",".join(coins), "vs_currencies": "usd", "include_last_updated_at": "true"},
                     accept="application/json", expect="json")
    rows, anomalies = parse(json.loads(r.text()), coins)
    return FetchResult(rows, r.sha256, r.last_modified, r.status, anomalies=anomalies, raw=r.body)


FEED = Feed("coingecko_prices", COLUMNS, fetch)
