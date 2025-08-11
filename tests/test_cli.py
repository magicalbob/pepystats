import io
import os
import sys
import pepystats.cli as cli


def _run_cli(argv, payload, status=200):
    import pepystats.api as api
    cf = __import__("tests.conftest", fromlist=[""])
    old_get = api.requests.get
    api.requests.get = lambda *a, **k: cf.make_response(payload, status=status)
    try:
        old_stdout = sys.stdout
        sys.stdout = io.StringIO()
        try:
            rc = cli.main(argv)
        except SystemExit as e:
            rc = e.code
        out = sys.stdout.getvalue()
    finally:
        api.requests.get = old_get
        sys.stdout = old_stdout
    return rc, out


def test_cli_overall_plain_sums(monkeypatch):
    os.environ["PEPY_API_KEY"] = "dummy"
    payload = {"downloads": {"2025-08-08": {"1.0": 1}, "2025-08-09": {"1.0": 2}}}
    rc, out = _run_cli(["overall", "chunkwrap", "--months", "0"], payload)
    assert rc in (None, 0)
    assert out.strip() == "3"  # 1 + 2


def test_cli_detailed_md_works(monkeypatch):
    os.environ["PEPY_API_KEY"] = "dummy"
    payload = {"downloads": {"2025-08-08": {"1.0": 1}, "2025-08-09": {"1.0": 2}}}
    rc, out = _run_cli(["detailed", "chunkwrap", "--fmt", "md", "--months", "0"], payload)
    assert rc in (None, 0)
    assert "total" in out and "2025-08-08" in out


def test_cli_versions_csv(monkeypatch):
    os.environ["PEPY_API_KEY"] = "dummy"
    payload = {"downloads": {"2025-08-08": {"1.0": 1, "2.0": 5}}}
    rc, out = _run_cli(["versions", "pkg", "--versions", "1.0", "2.0", "--fmt", "csv", "--months", "0"], payload)
    assert rc in (None, 0)
    assert "date" in out and "1.0" in out and "2.0" in out


def test_cli_handles_http_error(monkeypatch):
    os.environ["PEPY_API_KEY"] = "dummy"
    payload = {}
    rc, out = _run_cli(["overall", "pkg", "--months", "0"], payload, status=500)
    assert rc not in (None, 0)
