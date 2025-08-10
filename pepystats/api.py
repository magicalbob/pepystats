from __future__ import annotations
import os
import requests
import pandas as pd
from typing import Iterable, Optional, Dict, Any

BASE = "https://api.pepy.tech/api/v3"

def _headers(api_key: Optional[str]) -> Dict[str, str]:
    key = api_key or os.getenv("PEPY_API_KEY")
    return {"X-API-Key": key} if key else {}

def _time_range_from_months(months: int) -> str:
    # pepy supports: oneWeek, oneMonth, threeMonths, sixMonths, oneYear, all
    if months <= 1: return "oneMonth"
    if months <= 3: return "threeMonths"
    if months <= 6: return "sixMonths"
    if months <= 12: return "oneYear"
    return "all"

def _normalize_series_json(json_obj: Dict[str, Any]) -> pd.DataFrame:
    """
    pepy v3 returns `dailyDownloads`, `weeklyDownloads`, etc depending on granularity.
    We flatten any present key into a DataFrame with columns [date, downloads, label].
    """
    # Find the first key that exists among expected series
    for key in ("dailyDownloads","weeklyDownloads","monthlyDownloads","yearlyDownloads","downloads"):
        series = json_obj.get(key)
        if series:
            break
    else:
        # No series found; try to coerce overall style
        return pd.DataFrame(json_obj)
    rows = []
    label = json_obj.get("label") or json_obj.get("version") or "total"
    for item in series:
        rows.append({
            "date": item.get("date"),
            "downloads": item.get("downloads", 0),
            "label": label,
        })
    return pd.DataFrame(rows)

def get_overall(project: str, *, months: int = 3, granularity: str = "daily",
                include_ci: bool = True, api_key: Optional[str] = None) -> pd.DataFrame:
    """Overall downloads across all versions as a time series.
    
    Returns a DataFrame with columns [date, downloads, label], where label='total'.
    """
    timeRange = _time_range_from_months(months)
    url = f"{BASE}/projects/{project}"
    params = {
        "timeRange": timeRange,
        "category": "overall",
        "includeCIDownloads": str(include_ci).lower(),
        "granularity": granularity,
    }
    r = requests.get(url, headers=_headers(api_key), params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    # pepy returns { overall: {...series...} } for category=overall
    overall = data.get("overall") or data
    df = _normalize_series_json(overall)
    if not df.empty:
        df["label"] = "total"
    return df

def get_versions(project: str, *, versions: Iterable[str],
                 months: int = 3, granularity: str = "daily",
                 include_ci: bool = True, api_key: Optional[str] = None) -> pd.DataFrame:
    """Per-version time series. Returns DataFrame with [date, downloads, label] where label=version."""
    timeRange = _time_range_from_months(months)
    url = f"{BASE}/projects/{project}"
    params = {
        "versions": ",".join(versions),
        "timeRange": timeRange,
        "category": "version",
        "includeCIDownloads": str(include_ci).lower(),
        "granularity": granularity,
    }
    r = requests.get(url, headers=_headers(api_key), params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    # Expected form: { versions: [ { version: "x", dailyDownloads: [...] }, ... ] }
    vers = data.get("versions") or []
    frames = []
    for v in vers:
        df = _normalize_series_json(v)
        if not df.empty:
            # ensure label is the version string
            df["label"] = v.get("version") or df.get("label") or "unknown"
            frames.append(df)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=["date","downloads","label"])

def to_markdown(df: pd.DataFrame) -> str:
    if df.empty:
        return "_no data_"
    # pivot by label for a tidy MD table
    wide = df.pivot_table(index="date", columns="label", values="downloads", fill_value=0).sort_index()
    return wide.to_markdown()

def to_csv(df: pd.DataFrame) -> str:
    if df.empty:
        return ""
    wide = df.pivot_table(index="date", columns="label", values="downloads", fill_value=0).sort_index()
    return wide.to_csv()
