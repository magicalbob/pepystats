from __future__ import annotations

import os
from typing import Iterable, Optional, Dict, Any

import pandas as pd
import requests

# Public API base
BASE = "https://api.pepy.tech"


def _headers(api_key: Optional[str]) -> Dict[str, str]:
    key = api_key or os.getenv("PEPY_API_KEY")
    return {"X-API-Key": key} if key else {}


def _parse_v2_downloads(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    v2 response example:
    {
      "total_downloads": 123,
      "id": "project",
      "versions": ["1.0", "2.0"],
      "downloads": {
        "2023-08-29": {"1.0": 10, "2.0": 5},
        "2023-08-28": {"1.0": 7, "2.0": 3}
      }
    }
    """
    return data.get("downloads") or {}


def _to_naive_utc(series: pd.Series) -> pd.Series:
    """Parse dates as UTC, then drop tz to make them tz-naive (consistent comparisons)."""
    return pd.to_datetime(series, utc=True).dt.tz_localize(None)


def _trim_months(df: pd.DataFrame, months: Optional[int]) -> pd.DataFrame:
    if df.empty or not months or months <= 0:
        return df
    out = df.copy()
    out["date"] = _to_naive_utc(out["date"])
    # Cutoff is "now in UTC", normalized to midnight, then made naive
    now_naive = pd.Timestamp.now(tz="UTC").normalize().tz_localize(None)
    cutoff = now_naive - pd.DateOffset(months=months)
    out = out[out["date"] >= cutoff]
    out["date"] = out["date"].dt.strftime("%Y-%m-%d")
    return out


def _apply_granularity(df: pd.DataFrame, granularity: str) -> pd.DataFrame:
    """
    Free API is daily-only; this provides client-side resampling.
    granularity: daily | weekly | monthly | yearly
    """
    if df.empty or granularity == "daily":
        return df

    out_frames = []
    # Work with tz-naive UTC consistently
    for label, grp in df.groupby("label"):
        g = grp.copy()
        g["date"] = _to_naive_utc(g["date"])
        g = g.set_index("date").sort_index()
        if granularity == "weekly":
            res = g["downloads"].resample("W-SAT").sum()
        elif granularity == "monthly":
            res = g["downloads"].resample("MS").sum()
        elif granularity == "yearly":
            res = g["downloads"].resample("YS").sum()
        else:
            return df  # unknown granularity → leave as-is
        oo = res.reset_index()
        oo["label"] = label
        out_frames.append(oo)

    out = pd.concat(out_frames, ignore_index=True) if out_frames else pd.DataFrame(columns=["date", "downloads", "label"])
    if not out.empty:
        out["date"] = out["date"].dt.strftime("%Y-%m-%d")
    return out[["date", "downloads", "label"]]


def get_overall(
    project: str,
    *,
    months: int = 3,
    granularity: str = "daily",
    include_ci: bool = True,  # kept for CLI parity; not used by public v2
    api_key: Optional[str] = None,
) -> pd.DataFrame:
    """
    Overall downloads across all versions (public API v2, daily; resampled client-side).

    Returns DataFrame with columns: [date, downloads, label], where label='total'.
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


def get_versions(
    project: str,
    *,
    versions: Iterable[str],
    months: int = 3,
    granularity: str = "daily",
    include_ci: bool = True,  # kept for CLI parity; not used by public v2
    api_key: Optional[str] = None,
) -> pd.DataFrame:
    """
    Per-version daily series (public API v2) filtered to the requested versions.
    Returns DataFrame with columns: [date, downloads, label] where label=<version>.
    """
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
