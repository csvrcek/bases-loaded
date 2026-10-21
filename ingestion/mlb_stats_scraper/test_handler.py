"""Unit tests for the MLB Stats scraper handler.

Pure-logic tests only — no network and no AWS. The handler module creates an
S3 client and reads S3_BUCKET_DATA at import time, so set those before import.
"""

import os
import sys

os.environ.setdefault("S3_BUCKET_DATA", "test-bucket")
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-2")
# Dummy creds so boto3.client("s3") at import time doesn't hit the credential chain.
os.environ.setdefault("AWS_ACCESS_KEY_ID", "testing")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "testing")

# handler.py lives alongside this test; make `import handler` work regardless of
# the cwd pytest is invoked from (CI runs `python -m pytest ingestion` from root).
sys.path.insert(0, os.path.dirname(__file__))

import pytest

import handler


@pytest.fixture(autouse=True)
def _clear_hand_cache():
    """Reset the module-level handedness cache between tests."""
    handler._PITCHER_HAND_CACHE.clear()
    yield
    handler._PITCHER_HAND_CACHE.clear()


def _person_payload(code):
    return {"people": [{"pitchHand": {"code": code}}]}


@pytest.mark.parametrize(
    "code,expected",
    [("L", "L"), ("R", "R"), ("S", ""), ("", "")],
)
def test_fetch_handedness_parses_pitch_hand_code(monkeypatch, code, expected):
    monkeypatch.setattr(handler.statsapi, "get", lambda *a, **k: _person_payload(code))
    assert handler.fetch_pitcher_handedness("123") == expected


def test_fetch_handedness_empty_people(monkeypatch):
    monkeypatch.setattr(handler.statsapi, "get", lambda *a, **k: {"people": []})
    assert handler.fetch_pitcher_handedness("123") == ""


def test_fetch_handedness_caches_and_short_circuits(monkeypatch):
    calls = {"n": 0}

    def fake_get(*a, **k):
        calls["n"] += 1
        return _person_payload("L")

    monkeypatch.setattr(handler.statsapi, "get", fake_get)

    assert handler.fetch_pitcher_handedness("477132") == "L"
    # Second call for the same pitcher must not hit the API again.
    monkeypatch.setattr(
        handler.statsapi,
        "get",
        lambda *a, **k: pytest.fail("statsapi.get called on cache hit"),
    )
    assert handler.fetch_pitcher_handedness("477132") == "L"
    assert calls["n"] == 1


def test_fetch_handedness_error_returns_empty_and_caches(monkeypatch):
    calls = {"n": 0}

    def boom(*a, **k):
        calls["n"] += 1
        raise RuntimeError("network down")

    monkeypatch.setattr(handler.statsapi, "get", boom)

    assert handler.fetch_pitcher_handedness("999") == ""
    # Failure is cached, so a retry within the run does not call again.
    assert handler.fetch_pitcher_handedness("999") == ""
    assert calls["n"] == 1
