"""
Sportmonks API client for live soccer scores and goal events.
Uses v3 API: https://docs.sportmonks.com/v3/
Syntax: https://docs.sportmonks.com/v3/api/syntax
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Optional

import httpx

BASE_URL = "https://api.sportmonks.com/v3/football"
GOAL_EVENT_TYPES = frozenset({"GOAL", "OWN_GOAL", "PENALTY", "PENALTY_SHOOTOUT_GOAL", "GOAL_CONFIRMED"})


@dataclass
class LiveFixture:
    """One live fixture with score and goal events."""
    fixture_id: int
    name: str
    home_team: str
    away_team: str
    home_score: int
    away_score: int
    home_participant_id: Optional[int]
    away_participant_id: Optional[int]
    events: list[dict]  # goal events only, sorted by sort_order
    league_name: str
    starting_at_timestamp: Optional[int] = None  # Unix timestamp of match kickoff
    minute: Optional[int] = None  # Current computed minute (if available)


def _get_event_type_code(ev: dict) -> Optional[str]:
    """Return event type code (e.g. GOAL) from event or event.type."""
    t = ev.get("type")
    if isinstance(t, dict):
        return (t.get("code") or t.get("name") or "").strip().upper()
    if isinstance(t, str):
        return t.strip().upper()
    return None


def _is_goal_event(ev: dict) -> bool:
    return _get_event_type_code(ev) in GOAL_EVENT_TYPES


def _parse_fixture(raw: dict) -> Optional[LiveFixture]:
    """Parse one fixture from API response (with includes scores, participants, events)."""
    try:
        fixture_id = int(raw["id"])
        name = (raw.get("name") or "").strip()
    except (KeyError, TypeError, ValueError):
        return None

    home_team = ""
    away_team = ""
    home_pid: Optional[int] = None
    away_pid: Optional[int] = None
    for p in raw.get("participants") or []:
        try:
            pid = p.get("id")
            n = (p.get("name") or "").strip()
            meta = p.get("meta") or {}
            loc = (meta.get("location") or meta.get("position") or "").strip().lower()
            if loc == "home":
                home_team = n
                home_pid = int(pid) if pid is not None else None
            elif loc == "away":
                away_team = n
                away_pid = int(pid) if pid is not None else None
        except (TypeError, ValueError):
            continue
    if not home_team and not away_team and name:
        parts = name.split(" vs ", 1)
        if len(parts) == 2:
            home_team, away_team = parts[0].strip(), parts[1].strip()
        else:
            home_team, away_team = name, ""

    home_score = 0
    away_score = 0
    for s in raw.get("scores") or []:
        try:
            desc = (s.get("description") or "").upper()
            if desc != "CURRENT":
                continue
            score_obj = s.get("score") or {}
            goals = int(score_obj.get("goals", 0))
            part = (s.get("participant") or score_obj.get("participant") or "").lower()
            if part == "home":
                home_score = goals
            elif part == "away":
                away_score = goals
        except (TypeError, ValueError, KeyError):
            continue

    goal_events = [e for e in (raw.get("events") or []) if _is_goal_event(e)]
    goal_events.sort(key=lambda e: (e.get("sort_order") or 0, e.get("id") or 0))

    league_name = ""
    for inc in (raw.get("league") or raw.get("leagues") or []):
        if isinstance(inc, dict):
            league_name = (inc.get("name") or inc.get("short_name") or "").strip()
            break
    if not league_name and isinstance(raw.get("league"), dict):
        league_name = (raw["league"].get("name") or raw["league"].get("short_name") or "").strip()

    # Extract starting timestamp and calculate current minute
    starting_at_ts: Optional[int] = None
    current_minute: Optional[int] = None
    try:
        starting_at_ts = int(raw.get("starting_at_timestamp", 0)) or None
        if starting_at_ts:
            import time
            elapsed_seconds = int(time.time()) - starting_at_ts
            if elapsed_seconds > 0:
                current_minute = elapsed_seconds // 60
    except (TypeError, ValueError):
        pass

    return LiveFixture(
        fixture_id=fixture_id,
        name=name,
        home_team=home_team or "Home",
        away_team=away_team or "Away",
        home_score=home_score,
        away_score=away_score,
        home_participant_id=home_pid,
        away_participant_id=away_pid,
        events=goal_events,
        league_name=league_name,
        starting_at_timestamp=starting_at_ts,
        minute=current_minute,
    )


class SportmonksClient:
    """Async client for Sportmonks v3 Football API."""

    def __init__(self, api_token: Optional[str] = None):
        self.api_token = (api_token or os.environ.get("SPORTMONKS_API_TOKEN", "")).strip()
        self._client: Optional[httpx.AsyncClient] = None

    def _url(self, path: str, **params: Any) -> str:
        base = BASE_URL.rstrip("/") + "/" + path.lstrip("/")
        if self.api_token:
            params.setdefault("api_token", self.api_token)
        if params:
            q = "&".join(f"{k}={v}" for k, v in sorted(params.items()))
            return f"{base}?{q}"
        return base

    async def _ensure_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=15.0)
        return self._client

    async def close(self):
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def get_inplay_livescores(self) -> list[LiveFixture]:
        """
        GET livescores/inplay with minimal fields (select + include) to reduce latency and payload.
        Returns list of live fixtures with current score and goal events.
        """
        if not self.api_token:
            return []
        client = await self._ensure_client()
        # Include scores, participants, events (buts), league. Pas de select/field filter sur livescores (400).
        url = self._url(
            "livescores/inplay",
            include="scores;participants;events.type;league",
        )
        try:
            r = await client.get(url)
            r.raise_for_status()
            data = r.json()
        except (httpx.HTTPError, ValueError) as e:
            raise RuntimeError(f"Sportmonks inplay request failed: {e}") from e

        raw_list = data.get("data")
        if raw_list is None:
            return []
        if not isinstance(raw_list, list):
            raw_list = [raw_list]
        out = []
        for raw in raw_list:
            f = _parse_fixture(raw)
            if f is not None:
                out.append(f)
        return out


    async def get_fixture_events(self, fixture_id: int) -> list[dict]:
        """
        GET fixture by ID with events (and event types).
        Returns raw list of events for this fixture (all types: goals, cards, penalties, etc.).
        """
        if not self.api_token:
            return []
        client = await self._ensure_client()
        url = self._url(f"fixtures/{fixture_id}", include="events.type")
        try:
            r = await client.get(url)
            r.raise_for_status()
            data = r.json()
        except (httpx.HTTPError, ValueError) as e:
            raise RuntimeError(f"Sportmonks fixture request failed: {e}") from e
        raw = data.get("data") if isinstance(data, dict) else data
        if not isinstance(raw, dict):
            return []
        events = raw.get("events") or []
        if not isinstance(events, list):
            events = [events] if events else []
        return events


async def get_inplay_livescores(api_token: Optional[str] = None) -> list[LiveFixture]:
    """Convenience: fetch inplay livescores and return parsed fixtures."""
    client = SportmonksClient(api_token=api_token)
    try:
        return await client.get_inplay_livescores()
    finally:
        await client.close()
