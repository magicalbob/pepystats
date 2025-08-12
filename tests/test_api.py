import pandas as pd
import pytest
import pepystats.api as api


def test_get_overall_returns_sum(monkeypatch):
    payload = {
        "id": "chunkwrap",
        "downloads": {
            "2025-08-08": {"2.3.0": 10, "2.2.0": 5},
            "2025-08-09": {"2.3.0": 0, "2.2.0": 4},
            "2025-08-10": {"2.3.0": 7},
        },
    }
    monkeypatch.setattr(api.requests, "get", lambda *a, **k: __import__("tests.conftest", fromlist=[""]).make_response(payload))
    total = api.get_overall("chunkwrap", months=0)
    assert total == 10 + 5 + 0 + 4 + 7  # 26


def test_get_overall_401_raises_runtimeerror(monkeypatch):
    monkeypatch.setattr(
        api.requests, "get",
        lambda *a, **k: __import__("tests.conftest", fromlist=[""]).make_response({}, status=401)
    )
    with pytest.raises(RuntimeError) as ei:
        api.get_overall("chunkwrap", months=0)
    assert "Unauthorized" in str(ei.value)


def test_get_detailed_happy_path(monkeypatch):
    payload = {
        "id": "pkg",
        "downloads": {
            "2025-07-01": {"1.0": 3, "2.0": 9},
            "2025-07-02": {"1.0": 4},
        },
    }
    monkeypatch.setattr(api.requests, "get", lambda *a, **k: __import__("tests.conftest", fromlist=[""]).make_response(payload))
    df = api.get_detailed("pkg", months=0)
    assert set(df["label"]) == {"total"}
    assert df.loc[df["date"] == "2025-07-01", "downloads"].item() == 12
    assert df.loc[df["date"] == "2025-07-02", "downloads"].item() == 4


def test_trim_months_uses_utc_and_is_tz_safe(monkeypatch):
    cf = __import__("tests.conftest", fromlist=[""])
    monkeypatch.setattr(api.pd.Timestamp, "now", staticmethod(lambda tz=None: cf.fixed_now()))
    payload = {"downloads": {"2025-06-30": {"1.0": 1}, "2025-07-10": {"1.0": 2}, "2025-08-05": {"1.0": 3}}}
    monkeypatch.setattr(api.requests, "get", lambda *a, **k: cf.make_response(payload))
    df = api.get_detailed("pkg", months=1)
    # With zero-fill, we expect a continuous daily range between min and max
    assert df["date"].min() == "2025-07-10"
    assert df["date"].max() == "2025-08-05"
    # original points preserved
    assert df.loc[df["date"] == "2025-07-10", "downloads"].item() == 2
    assert df.loc[df["date"] == "2025-08-05", "downloads"].item() == 3
    # and a gap day is present with zero
    assert df.loc[df["date"] == "2025-07-11", "downloads"].item() == 0


def test_granularity_weekly_resamples(monkeypatch):
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

    df_daily = api.get_detailed("pkg", months=0, granularity="daily")
    assert len(df_daily) == 7

    df_week = api.get_detailed("pkg", months=0, granularity="weekly")
    assert len(df_week) == 1
    assert df_week["downloads"].item() == sum(range(1, 8))


def test_to_markdown_and_csv_are_pivoted():
    df = pd.DataFrame(
        [
            {"date": "2025-08-08", "downloads": 1, "label": "A"},
            {"date": "2025-08-08", "downloads": 2, "label": "B"},
            {"date": "2025-08-09", "downloads": 3, "label": "A"},
        ]
    )
    md = api.to_markdown(df)
    csv = api.to_csv(df)
    assert "A" in md and "B" in md and "| date " in md
    assert "date" in csv and "A" in csv and "B" in csv


def test_detailed_fills_missing_days(monkeypatch):
    payload = {"downloads": {
        "2025-08-01": {"1.0": 2},
        # 2025-08-02 missing
        "2025-08-03": {"1.0": 3},
    }}
    import pepystats.api as api
    cf = __import__("tests.conftest", fromlist=[""])
    monkeypatch.setattr(api.requests, "get", lambda *a, **k: cf.make_response(payload))

    df = api.get_detailed("pkg", months=0, granularity="daily")
    assert list(df["date"]) == ["2025-08-01", "2025-08-02", "2025-08-03"]
    assert list(df["downloads"]) == [2, 0, 3]


def test_weekly_includes_empty_weeks(monkeypatch):
    payload = {"downloads": {
        "2025-08-01": {"1.0": 2},  # falls in week ending Sat 2025-08-02
        "2025-08-15": {"1.0": 1},  # week ending Sat 2025-08-16
        # Week ending 2025-08-09 has no data → should appear as 0
    }}
    import pepystats.api as api
    cf = __import__("tests.conftest", fromlist=[""])
    monkeypatch.setattr(api.requests, "get", lambda *a, **k: cf.make_response(payload))

    df = api.get_detailed("pkg", months=0, granularity="weekly").sort_values("date")
    # Expect three consecutive Saturdays
    assert len(df) == 3
    assert 0 in df["downloads"].tolist()
