import types
import requests
import pandas as pd


def make_response(payload, status=200):
    """Minimal fake Response for requests.get."""
    r = types.SimpleNamespace()
    r.status_code = status

    def json_func():
        return payload

    def raise_for_status():
        if status >= 400:
            raise requests.exceptions.HTTPError(f"{status} error")

    r.json = json_func
    r.raise_for_status = raise_for_status
    return r


def fixed_now(ts="2025-08-10T00:00:00Z"):
    """Return a tz-aware pandas Timestamp for monkeypatching Timestamp.now."""
    return pd.Timestamp(ts)
