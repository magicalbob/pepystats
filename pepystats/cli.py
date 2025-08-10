from __future__ import annotations
import argparse
import sys
from .api import get_overall, get_versions, to_markdown, to_csv
import pandas as pd
import matplotlib.pyplot as plt

def _common_args(p):
    p.add_argument("project", help="PyPI project name (e.g., chunkwrap)")
    p.add_argument("--months", type=int, default=3, help="How many months back (default: 3)")
    p.add_argument("--granularity", choices=["daily","weekly","monthly","yearly"], default="daily")
    p.add_argument("--no-ci", action="store_true", help="Exclude CI downloads")
    p.add_argument("--api-key", help="pepy.tech API key (defaults to $PEPY_API_KEY)")
    p.add_argument("--fmt", choices=["plain","md","csv"], default="plain", help="Table format (default: plain)")
    p.add_argument("--plot", action="store_true", help="Plot the series with matplotlib")

def _print_df(df: pd.DataFrame, fmt: str):
    if fmt == "md":
        print(to_markdown(df))
    elif fmt == "csv":
        print(to_csv(df), end="")
    else:
        if df.empty:
            print("no data")
        else:
            print(df.sort_values(["label","date"]).to_string(index=False))

def main(argv=None):
    parser = argparse.ArgumentParser(prog="pepystats", description="pepy.tech stats from the command line")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_overall = sub.add_parser("overall", help="Overall downloads across all versions")
    _common_args(p_overall)

    p_versions = sub.add_parser("versions", help="Per-version downloads for specified versions")
    _common_args(p_versions)
    p_versions.add_argument("--versions", nargs="+", required=True, help="One or more version strings")

    args = parser.parse_args(argv)

    include_ci = not args.no_ci
    if args.cmd == "overall":
        df = get_overall(args.project, months=args.months, granularity=args.granularity, include_ci=include_ci, api_key=args.api_key)
    else:
        df = get_versions(args.project, versions=args.versions, months=args.months, granularity=args.granularity, include_ci=include_ci, api_key=args.api_key)

    _print_df(df, args.fmt)

    if args.plot and not df.empty:
        plt.figure()
        for label, part in df.groupby("label"):
            part = part.sort_values("date")
            plt.plot(part["date"], part["downloads"], label=label)
        plt.legend()
        plt.xlabel("date")
        plt.ylabel("downloads")
        plt.title(f"{args.project} downloads")
        plt.xticks(rotation=45, ha="right")
        plt.tight_layout()
        plt.show()

if __name__ == "__main__":
    sys.exit(main())
