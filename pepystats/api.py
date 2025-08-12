from __future__ import annotations

import os
from typing import Iterable, Optional, Dict, Any

import pandas as pd
import requests

BASE = "https://api.pepy.tech"


def _headers(api_key: Optional[str]) -> Dict[str, str]:
    key = api_key or os.getenv("PEPY_API_KEY")
    return {"X-API-Key": key} if key else {}


def _parse_v2_downloads(data: Dict[str, Any]) -> Dict[str, Any]:
    return data.get("downloads") or {}


def _to_naive_utc(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, utc=True).dt.tz_localize(None)


def _trim_months(df: pd.DataFrame, months: Optional[int]) -> pd.DataFrame:
    if df.empty or not months or months <= 0:
        return df
    out = df.copy()
    out["date"] = _to_naive_utc(out["date"])
    now_naive = pd.Timestamp.now(tz="UTC").normalize().tz_localize(None)
    cutoff = now_naive - pd.DateOffset(months=months)
    out = out[out["date"] >= cutoff]
    out["date"] = out["date"].dt.strftime("%Y-%m-%d")
    return out


def _complete_range(df: pd.DataFrame, freq: str) -> pd.DataFrame:
    """For each label, reindex to a complete date range and fill zeros."""
    if df.empty:
        return df

    frames = []
    for label, grp in df.groupby("label"):
        g = grp.copy()
        g["date"] = _to_naive_utc(g["date"])
        g = g.set_index("date").sort_index()

        # Build a complete index from min..max for this label
        start = g.index.min().normalize()
        end = g.index.max().normalize()
        full_idx = pd.date_range(start, end, freq=freq)

        s = g["downloads"].astype("int64").reindex(full_idx, fill_value=0)
        out = s.reset_index().rename(columns={"index": "date"})
        out["label"] = label
        frames.append(out)

    out = pd.concat(frames, ignore_index=True)
    out["date"] = out["date"].dt.strftime("%Y-%m-%d")
    return out[["date", "downloads", "label"]]



def _apply_granularity(df: pd.DataFrame, granularity: str) -> pd.DataFrame:
    if df.empty:
        return df

    if granularity == "daily":
        return _complete_range(df, "D")

    freq = None
    if granularity == "weekly":
        freq = "W-SAT"
    elif granularity == "monthly":
        freq = "MS"
    elif granularity == "yearly":
        freq = "YS"
    else:
        return df  # unknown → leave as-is

    frames = []
    for label, grp in df.groupby("label"):
        g = grp.copy()
        g["date"] = _to_naive_utc(g["date"])
        g = g.set_index("date").sort_index()

        res = g["downloads"].resample(freq).sum()

        # Ensure empty periods are present with zero
        full_idx = pd.date_range(res.index.min(), res.index.max(), freq=freq)
        res = res.reindex(full_idx, fill_value=0)

        out = res.reset_index().rename(columns={"index": "date"})
        out["label"] = label
        frames.append(out)

    out = pd.concat(frames, ignore_index=True)
    out["date"] = out["date"].dt.strftime("%Y-%m-%d")
    return out[["date", "downloads", "label"]]


def get_detailed(
    project: str,
    *,
    months: int = 3,
    granularity: str = "daily",
    include_ci: bool = True,  # placeholder for parity; not used by public v2
    api_key: Optional[str] = None,
) -> pd.DataFrame:
    """
    Per-day (optionally resampled) totals across all versions.
    Returns tidy DataFrame: columns [date, downloads, label='total'].
    """
    url = f"{BASE}/api/v2/projects/{project}"
    r = requests.get(url, headers=_headers(api_key), timeout=30)
    if r.status_code == 401:
        raise RuntimeError("Unauthorized (401) from pepy.tech. Set PEPY_API_KEY or pass api_key.")
    r.raise_for_status()

    data = r.json()
    rows = []
    for date, ver_map in _parse_v2_downloads(data).items():
        if isinstance(ver_map, dict):
            total = sum(int(v or 0) for v in ver_map.values())
        else:
            total = int(ver_map or 0)
        rows.append({"date": date, "downloads": total, "label": "total"})

    df = pd.DataFrame(rows, columns=["date", "downloads", "label"])
    df = _trim_months(df, months)
    df = _apply_granularity(df, granularity)
    return df


def get_overall(
    project: str,
    *,
    months: int = 3,
    include_ci: bool = True,   # placeholder for parity; not used by public v2
    api_key: Optional[str] = None,
) -> int:
    """
    Sum of downloads over the selected window across all versions.
    Returns a single integer.
    """
    df = get_detailed(project, months=months, granularity="daily", include_ci=include_ci, api_key=api_key)
    return int(df["downloads"].sum()) if not df.empty else 0


def get_versions(
    project: str,
    *,
    versions: Iterable[str],
    months: int = 3,
    granularity: str = "daily",
    include_ci: bool = True,
    api_key: Optional[str] = None,
) -> pd.DataFrame:
    url = f"{BASE}/api/v2/projects/{project}"
    r = requests.get(url, headers=_headers(api_key), timeout=30)
    if r.status_code == 401:
        raise RuntimeError("Unauthorized (401) from pepy.tech. Set PEPY_API_KEY or pass api_key.")
    r.raise_for_status()

    data = r.json()
    want = set(versions or [])
    rows = []
    for date, ver_map in _parse_v2_downloads(data).items():
        if not isinstance(ver_map, dict):
            continue
        for ver, count in ver_map.items():
            if not want or ver in want:
                rows.append({"date": date, "downloads": int(count or 0), "label": ver})

    df = pd.DataFrame(rows, columns=["date", "downloads", "label"])
    df = _trim_months(df, months)
    df = _apply_granularity(df, granularity)
    return df


def to_markdown(df: pd.DataFrame) -> str:
    if df.empty:
        return "_no data_"
    wide = df.pivot_table(index="date", columns="label", values="downloads", fill_value=0).sort_index()
    return wide.to_markdown()


def to_csv(df: pd.DataFrame) -> str:
    if df.empty:
        return ""
    wide = df.pivot_table(index="date", columns="label", values="downloads", fill_value=0).sort_index()
    return wide.to_csv()
