"""Tests for app/utils/discovery.py."""

import app.utils.discovery as discovery


def test_get_teletask_ids_returns_empty_when_no_candidates(
    monkeypatch,
) -> None:
    monkeypatch.setattr(discovery, "getHighestTeletaskID", lambda: None)
    monkeypatch.setattr(discovery, "get_blacklisted_ids", lambda: [])

    assert discovery.get_teletask_ids() == set()


def test_get_teletask_ids_uses_vtt_upper_bound_when_blacklist_empty(
    monkeypatch,
) -> None:
    monkeypatch.setattr(discovery, "getHighestTeletaskID", lambda: 5)
    monkeypatch.setattr(discovery, "get_blacklisted_ids", lambda: [])

    assert discovery.get_teletask_ids() == {1, 2, 3, 4, 5}


def test_get_teletask_ids_treats_none_blacklist_as_empty(
    monkeypatch,
) -> None:
    monkeypatch.setattr(discovery, "getHighestTeletaskID", lambda: 3)
    monkeypatch.setattr(discovery, "get_blacklisted_ids", lambda: None)

    assert discovery.get_teletask_ids() == {1, 2, 3}


def test_get_teletask_ids_uses_max_of_vtt_and_blacklist_and_removes_blacklisted(
    monkeypatch,
) -> None:
    monkeypatch.setattr(discovery, "getHighestTeletaskID", lambda: 4)
    monkeypatch.setattr(discovery, "get_blacklisted_ids", lambda: [2, 6])

    assert discovery.get_teletask_ids() == {1, 3, 4, 5}
