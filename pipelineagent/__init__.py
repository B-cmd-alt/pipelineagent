"""PipelineAgent — autonomous detection-and-repair for data pipelines.

Package layout:
  config      fleet.yaml loader + typed manifest
  http        paced, identifying, conditional HTTP client with budget
  feeds/      one small fetcher per public source (append-only, vintage-stamped)
  warehouse/  storage backends (BigQuery for production, SQLite for tests/offline)
  detectors   out-of-band data detectors -> OK / BREACH / UNKNOWN
  ledger      findings + dispositions (append-only) + HEALTHLOG
  heartbeat   dead-man's switch row (carries the runtime code hash)
  render_site static dashboard generator
  cli         `python -m pipelineagent ...`
"""
__version__ = "0.1.0"
