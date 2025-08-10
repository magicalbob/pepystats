import pandas as pd
import pytest
import pepystats.api as api


def test_get_overall_happy_path(monkeypatch):
    # Arrange: daily per-version data for 3 days
    payload = {
        "id": "chunkwrap",
        "downloads": {
            "2025-08-08": {"2.3.0": 10, "2.2.0": 5},
            "2025-08-09": {"2.3.0": 0, "2.2.0": 4},
            "2025-08-10": {"2.3.0": 7},
        },
    }
    monkeypatch.setattr(api.requests, "get", lambda *a, **k: __import__("tests.conftest", fromlist=[""]).make_response(payload))

    # Act
    df = api.get_overall("chunkwrap", months=0)  # skip trimming for deterministic test

    # Assert
    assert list(df.columns) == ["date", "downloads", "label"]
    assert set(df["label"]) == {"total"}
    # 10+5, 0+4, 7
    assert df.loc[df["date"] == "2025-08-08", "downloads"].item() == 15
    assert df.loc[df["date"] == "2025-08-09", "downloads"].item() == 4
    assert df.loc[df["date"] == "2025-08-10", "downloads"].item() == 7


def test_get_overall_401_raises_runtimeerror(monkeypatch):
    monkeypatch.setattr(
        api.requests, "get",
        lambda *a, **k: __import__("tests.conftest", fromlist=[""]).make_response({}, status=401)
    )
    with pytest.raises(RuntimeError) as ei:
        api.get_overall("chunkwrap", months=0)
    assert "Unauthorized" in str(ei.value)


def test_get_versions_filters_and_shapes(monkeypatch):
    payload = {
        "id": "pkg",
        "downloads": {
            "2025-07-01": {"1.0": 3, "2.0": 9},
            "2025-07-02": {"1.0": 4},
        },
    }
    monkeypatch.setattr(api.requests, "get", lambda *a, **k: __import__("tests.conftest", fromlist=[""]).make_response(payload))

    df = api.get_versions("pkg", versions=["1.0"], months=0)
    assert set(df["label"]) == {"1.0"}
    assert df.loc[df["date"] == "2025-07-01", "downloads"].item() == 3
    assert df.loc[df["date"] == "2025-07-02", "downloads"].item() == 4


def test_trim_months_uses_utc_and_is_tz_safe(monkeypatch):
    # Make now = 2025-08-10 UTC
    cf = __import__("tests.conftest", fromlist=[""])
    monkeypatch.setattr(api.pd.Timestamp, "now", staticmethod(lambda tz=None: cf.fixed_now()))

    # Data spans two months
    payload = {
        "downloads": {
            "2025-06-30": {"1.0": 1},
            "2025-07-10": {"1.0": 2},
            "2025-08-05": {"1.0": 3},
        }
    }
    monkeypatch.setattr(api.requests, "get", lambda *a, **k: cf.make_response(payload))

    # months=1 → keep rows on/after 2025-07-10 (cutoff ~ 2025-07-10 since we normalize and subtract 1 month)
    df = api.get_overall("pkg", months=1)
    assert set(df["date"]) == {"2025-07-10", "2025-08-05"}


def test_granularity_weekly_resamples(monkeypatch):
    # 7 days → one weekly bucket (Sun-Sat end), pandas "W" sums
    payload = {
        "downloads": {
            "2025-08-03": {"1.0": 1},
            "2025-08-04": {"1.0": 2},
            "2025-08-05": {"1.0": 3},
            "2025-08-06": {"1.0": 4},
            "2025-08-07": {"1.0": 5},
            "2025-08-08": {"1.0": 6},
            "2025-08-09": {"1.0": 7},
        }
    }
    cf = __import__("tests.conftest", fromlist=[""])
    monkeypatch.setattr(api.requests, "get", lambda *a, **k: cf.make_response(payload))

    df_daily = api.get_overall("pkg", months=0, granularity="daily")
    assert len(df_daily) == 7

    df_week = api.get_overall("pkg", months=0, granularity="weekly")
    # Expect exactly one row after weekly sum
    assert len(df_week) == 1
    assert df_week["downloads"].item() == sum(range(1, 8))


def test_to_markdown_and_csv_are_pivoted(monkeypatch):
    # Build a tidy df and check formatted outputs contain expected labels/headers
    df = pd.DataFrame(
        [
            {"date": "2025-08-08", "downloads": 1, "label": "A"},
            {"date": "2025-08-08", "downloads": 2, "label": "B"},
            {"date": "2025-08-09", "downloads": 3, "label": "A"},
        ]
    )
    md = api.to_markdown(df)
    csv = api.to_csv(df)
    # Markdown header should include both labels as columns
    assert "A" in md and "B" in md and "| date " in md
    # CSV header should include date and both labels (order can vary by pandas, so be forgiving)
    assert "date" in csv and "A" in csv and "B" in csv
