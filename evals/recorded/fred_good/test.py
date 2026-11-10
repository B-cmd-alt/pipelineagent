from pipelineagent.feeds import fred_dgs10


def test_renamed_date_column_parses():
    rows, anomalies = fred_dgs10.parse('DATE,DGS10\n2026-11-05,4.1\n2026-11-06,4.2\n', 'DGS10', 5)
    assert len(rows) == 2 and any('renamed' in a for a in anomalies)
