"""NWS latest observation per station. JSON-LD GeoJSON; values may be null when
a sensor is down (that is real data, not a parse failure)."""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone

from ..http import SourceError
from ..schema import Column
from .base import Feed, FetchContext, FetchResult, to_float

COLUMNS = (
    Column("station", "STRING", True),
    Column("temperature_c", "FLOAT64"),
    Column("wind_speed_kmh", "FLOAT64"),
    Column("barometric_pressure_pa", "FLOAT64"),
    Column("text_description", "STRING"),
    Column("raw_timestamp", "STRING"),
)


def _ts(s: str | None) -> datetime | None:
    if not s:
        return None
    return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)


def fetch(ctx: FetchContext) -> FetchResult:
    stations = list(ctx.params.get("stations") or ["KOKC"])
    rows, anomalies, digest, status, lm, raw = [], [], hashlib.sha256(), None, None, []
    for st in stations:
        try:
            r = ctx.http.get(f"https://api.weather.gov/stations/{st}/observations/latest", accept="application/geo+json", expect="json")
        except SourceError as e:  # one dark station must not discard the others
            anomalies.append(f"{st}: {e}")
            continue
        status = r.status
        digest.update(r.body)
        raw.append(r.body)
        lm = r.last_modified or lm
        p = json.loads(r.text()).get("properties", {})
        rows.append({
            "station": st,
            "event_ts": _ts(p.get("timestamp")),
            "temperature_c": to_float((p.get("temperature") or {}).get("value"), anomalies, f"{st}.temperature"),
            "wind_speed_kmh": to_float((p.get("windSpeed") or {}).get("value"), anomalies, f"{st}.windSpeed"),
            "barometric_pressure_pa": to_float((p.get("barometricPressure") or {}).get("value"), anomalies, f"{st}.pressure"),
            "text_description": p.get("textDescription"),
            "raw_timestamp": p.get("timestamp"),
        })
    if not rows:
        raise SourceError("all stations failed: " + "; ".join(anomalies))
    # archive as a JSON array of the per-station bodies so a replay can split them again
    archive = json.dumps([b.decode("utf-8", errors="replace") for b in raw]).encode()
    return FetchResult(rows, digest.hexdigest(), lm, status, anomalies=anomalies, raw=archive)


FEED = Feed("nws_observations", COLUMNS, fetch)
