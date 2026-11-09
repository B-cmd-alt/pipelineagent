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
        price_value = None
        price_key = None
        if "usd" in d:
            price_key = "usd"
            price_value = d.get("usd")
        elif "usd_price" in d:
            price_key = "usd_price"
            price_value = d.get("usd_price")
            anomalies.append(f"{c}: schema drift: using 'usd_price' instead of 'usd'")
        else:
            anomalies.append(f"{c}: missing price fields (expected 'usd' or 'usd_price')")
        lu = d.get("last_updated_at")
        price_usd = to_float(price_value, anomalies, f"{c}.{price_key}") if price_key else None
        rows.append({
            "coin": c,
            "event_ts": datetime.fromtimestamp(lu, tz=timezone.utc) if lu else None,
            "price_usd": price_usd,
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
