import json
from datetime import datetime, timezone

from pipelineagent.feeds import coingecko_prices


def test_coingecko_prices_parses_usd_price_drift():
    payload = json.loads(
        '{"bitcoin":{"usd_price":75136,"last_updated_at":1794190522},'
        '"ethereum":{"usd_price":2142.79,"last_updated_at":1794190513}}'
    )
    rows, anomalies = coingecko_prices.parse(payload, ["bitcoin", "ethereum"])

    assert len(rows) == 2

    assert rows[0]["coin"] == "bitcoin"
    assert rows[0]["price_usd"] == 75136
    assert rows[0]["event_ts"] == datetime.fromtimestamp(1794190522, tz=timezone.utc)

    assert rows[1]["coin"] == "ethereum"
    assert rows[1]["price_usd"] == 2142.79
    assert rows[1]["event_ts"] == datetime.fromtimestamp(1794190513, tz=timezone.utc)

    assert any("usd_price" in a for a in anomalies)
