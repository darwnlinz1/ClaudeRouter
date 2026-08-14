import sys

import pytest

from orchestrator import cli


def test_cli_binds_loopback_and_disables_proxy_headers(monkeypatch):
    captured = {}
    monkeypatch.setattr(sys, "argv", ["ai-orchestrator", "--port", "9123"])
    monkeypatch.setattr(
        cli.uvicorn,
        "run",
        lambda *args, **kwargs: captured.update(
            {"args": args, "kwargs": kwargs}
        ),
    )

    cli.main()

    assert captured["args"] == ("server:app",)
    assert captured["kwargs"]["host"] == "127.0.0.1"
    assert captured["kwargs"]["port"] == 9123
    assert captured["kwargs"]["proxy_headers"] is False
    assert captured["kwargs"]["forwarded_allow_ips"] == ""


def test_cli_rejects_host_override(monkeypatch):
    monkeypatch.setattr(
        sys,
        "argv",
        ["ai-orchestrator", "--host", "0.0.0.0"],
    )

    with pytest.raises(SystemExit):
        cli.main()
