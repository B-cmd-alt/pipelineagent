"""Feed registry: feed id -> Feed. Adding a feed = one module + one fleet.yaml entry."""
from __future__ import annotations

from .base import Feed
from . import (
    canary_selfcheck,
    coingecko_prices,
    fred_dgs10,
    github_status,
    ieso_ontario_demand,
    nws_observations,
    open_meteo_current,
    usgs_earthquakes_hour,
)

REGISTRY: dict[str, Feed] = {
    m.FEED.id: m.FEED
    for m in (nws_observations, open_meteo_current, usgs_earthquakes_hour, fred_dgs10, github_status, coingecko_prices, ieso_ontario_demand, canary_selfcheck)
}


def get(feed_id: str) -> Feed:
    try:
        return REGISTRY[feed_id]
    except KeyError:
        raise KeyError(f"no fetcher registered for feed {feed_id!r}; registry has {sorted(REGISTRY)}") from None
