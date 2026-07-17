"""
Price Tracker - Measures how long it takes for odds to stabilize after a goal

Answers the key questions:
1. How many seconds until the price stabilizes after we receive the goal info?
2. Do we have enough time to place a trade and make profit?

Tracks: price at T+0, T+1, T+5, T+10, T+15, T+30, T+60, T+120 seconds
"""

import asyncio
import csv
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from dataclasses import dataclass, asdict
from typing import Optional

import httpx
from loguru import logger
from src.team_matching import teams_match as _teams_match_fn

DATA_DIR = Path(__file__).parent.parent / "data" / "live"
DATA_DIR.mkdir(parents=True, exist_ok=True)

CLOB_URL = "https://clob.polymarket.com"
GAMMA_URL = "https://gamma-api.polymarket.com"

# Intervals to sample (seconds after goal)
SAMPLE_INTERVALS = [0, 1, 2, 3, 5, 7, 10, 15, 20, 30, 45, 60, 90, 120]

# Price is "stabilized" when change < this over 10s window
STABILIZE_THRESHOLD = 0.005  # 0.5%


@dataclass
class PriceSample:
    """Price at a specific time after goal"""
    seconds_after_goal: int
    price: float
    timestamp: str


@dataclass
class PriceCurveAnalysis:
    """Analysis of price movement after a goal"""
    goal_timestamp: str
    match_slug: str
    scoring_team: str
    home_team: str
    away_team: str
    
    # Raw data
    samples: list[PriceSample]
    
    # Analysis results
    price_at_0s: Optional[float] = None
    price_at_60s: Optional[float] = None
    price_at_120s: Optional[float] = None
    # PnL réaliste: achat à l'ask, vente au bid
    entry_ask_0s: Optional[float] = None
    exit_bid_60s: Optional[float] = None
    
    time_to_stabilize_seconds: Optional[int] = None  # When price stopped moving
    max_price_seconds: Optional[int] = None  # When price peaked (best exit)
    min_price_seconds: Optional[int] = None  # Worst moment
    
    # Profit analysis
    profit_if_entry_0s_exit_60s: Optional[float] = None  # % gain
    profit_if_entry_0s_exit_120s: Optional[float] = None
    profit_window_seconds: Optional[int] = None  # How long we have to profit
    
    token_id: Optional[str] = None
    market_found: bool = False


class PriceTracker:
    """
    Tracks price movement after a goal to answer:
    - How long until stabilization?
    - Do we have time to trade profitably?
    """
    
    # Gamma: gameId WebSocket ≠ event id. On utilise la ligue (chi/rus/kor) → tag_id pour lister les events.
    _sports_tag_cache: dict[str, int] = {}  # league -> tag_id (class var pour partage)
    # Mapping codes WebSocket Polymarket → slug sport Gamma (quand différent)
    LEAGUE_TO_GAMMA_SPORT: dict[str, str] = {
        "elc": "efl",   # Championship = EFL
        "bundesliga": "bun",
        "ligue1": "fl1",
        "laliga": "lal",
    }
    # Quand slug = gameId numérique et ligue absente (WS n'a pas envoyé leagueAbbreviation), tenter ces ligues
    LEAGUES_TO_TRY_WHEN_UNKNOWN: tuple[str, ...] = (
        "epl", "lal", "bun", "fl1", "sea", "tur", "ere", "por", "mls", "ucl", "uel", "efa", "efl",
    )
    # UCL: Gamma peut exposer le tag sous différents slugs
    UCL_TAG_SLUGS: tuple[str, ...] = ("ucl", "uefa-champions-league", "champions-league")
    # Tag 100977 = matchs UCL (1234 = Winner / Top Scorer)
    UCL_MATCHES_TAG_ID: int = 100977

    def __init__(self):
        self._client: Optional[httpx.AsyncClient] = None
        self._curve_csv = DATA_DIR / f"price_curves_{datetime.now().strftime('%Y%m%d')}.csv"
        self._init_curve_csv()
    
    def _init_curve_csv(self):
        """Init CSV for price curves"""
        if not self._curve_csv.exists():
            headers = ["goal_ts", "slug", "scoring_team", "home", "away",
                       "price_0s", "price_1s", "price_5s", "price_10s", "price_30s", "price_60s", "price_120s",
                       "time_to_stabilize_s", "max_profit_at_s", "profit_0_60_pct", "market_found"]
            with open(self._curve_csv, "w", newline="") as f:
                csv.writer(f).writerow(headers)
    
    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=10.0)
        return self._client
    
    async def close(self):
        """Close the HTTP client to avoid resource leaks"""
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def _get_event_by_slug_or_id(self, slug: str):
        """
        Récupère un event Gamma par slug ou par id (gameId numérique).
        Si Polymarket a le match (WebSocket), il existe sur Gamma : soit slug soit id.
        """
        if not slug:
            return None
        client = await self._get_client()
        try:
            if slug.isdigit():
                # gameId du WebSocket = id Gamma (integer) ?
                r = await client.get(f"{GAMMA_URL}/events/{slug}")
            else:
                # slug texte (ex. soccer-orenburg-zenit-...)
                r = await client.get(f"{GAMMA_URL}/events/slug/{slug}")
            if r.status_code != 200:
                return None
            data = r.json()
            if isinstance(data, dict) and data.get("id"):
                return data
            if isinstance(data, list) and len(data) > 0:
                return data[0]
            return None
        except Exception as e:
            logger.debug(f"Gamma get event by slug/id: {e}")
            return None

    def _normalize_league_for_gamma(self, league: str) -> str:
        """WebSocket peut envoyer un code différent de Gamma (ex. elc vs efl)."""
        k = (league or "").strip().lower()
        return self.LEAGUE_TO_GAMMA_SPORT.get(k, k)

    async def _get_tag_id_for_league(self, league: str) -> Optional[int]:
        """Récupère le tag_id Gamma pour une ligue (chi, rus, kor, epl, ...) via GET /sports."""
        if not league:
            return None
        league = self._normalize_league_for_gamma(league)
        if league in self._sports_tag_cache:
            return self._sports_tag_cache[league]
        try:
            client = await self._get_client()
            r = await client.get(f"{GAMMA_URL}/sports")
            if r.status_code != 200:
                return None
            sports = r.json()
            if not isinstance(sports, list):
                return None
            # Tags génériques souvent en fin de liste (soccer général) ; on préfère le tag ligue-spécifique
            generic_tags = {1, 100639, 100350}
            for s in sports:
                if (s.get("sport") or "").lower() == league:
                    tags = (s.get("tags") or "").strip()
                    if tags:
                        parts = [int(p.strip()) for p in tags.split(",") if p.strip().isdigit()]
                        if parts:
                            # Prendre le dernier tag non générique (ex. fl1→102070, efl→102595)
                            for tid in reversed(parts):
                                if tid not in generic_tags:
                                    self._sports_tag_cache[league] = tid
                                    return tid
                            self._sports_tag_cache[league] = parts[-1]
                            return parts[-1]
            return None
        except Exception as e:
            logger.debug(f"Gamma /sports: {e}")
            return None

    async def _get_events_by_tag_id(self, tag_id: int) -> list:
        """GET /events?tag_id=X&closed=false pour les matchs en cours."""
        try:
            client = await self._get_client()
            r = await client.get(
                f"{GAMMA_URL}/events",
                params={"tag_id": tag_id, "closed": "false", "limit": 100},
            )
            if r.status_code != 200:
                return []
            data = r.json()
            return data if isinstance(data, list) else []
        except Exception as e:
            logger.debug(f"Gamma events tag_id: {e}")
            return []

    async def _get_events_by_tag_slug(self, tag_slug: str) -> list:
        """GET /events?tag_slug=X&closed=false. Utilisé pour les ligues sans tag_id dédié (ex. rou1)."""
        try:
            client = await self._get_client()
            r = await client.get(
                f"{GAMMA_URL}/events",
                params={"tag_slug": tag_slug, "closed": "false", "limit": 100},
            )
            if r.status_code != 200:
                return []
            data = r.json()
            return data if isinstance(data, list) else []
        except Exception as e:
            logger.debug(f"Gamma events tag_slug: {e}")
            return []

    async def get_live_soccer_events_from_gamma(
        self, league_slugs: Optional[set] = None
    ) -> list[dict]:
        """
        Appel API Gamma: récupère tous les matchs en direct (closed=false) pour les ligues soccer.
        Retourne une liste de {slug, home_team, away_team, league, score}.
        """
        if league_slugs is None:
            league_slugs = frozenset(
                {"epl", "fl1", "bun", "sea", "lal", "ere", "por", "tur", "rus", "efl", "efa", "mls"}
            )
        out: list[dict] = []
        try:
            client = await self._get_client()
            r = await client.get(f"{GAMMA_URL}/sports")
            if r.status_code != 200:
                return out
            sports = r.json()
            if not isinstance(sports, list):
                return out
            generic_tags = {1, 100639, 100350}
            for s in sports:
                league = (s.get("sport") or "").strip().lower()
                if league not in league_slugs:
                    continue
                tags = (s.get("tags") or "").strip()
                if not tags:
                    continue
                parts = [int(p.strip()) for p in tags.split(",") if p.strip().isdigit()]
                if not parts:
                    continue
                tag_id = next((tid for tid in reversed(parts) if tid not in generic_tags), parts[-1])
                # UCL: utiliser le tag des matchs (100977), pas Winner/Top Scorer (1234)
                if league == "ucl" and self.UCL_MATCHES_TAG_ID in parts:
                    tag_id = self.UCL_MATCHES_TAG_ID
                # Ligues sans tag dédié (ex. rou1, chi1, col1) : tag_slug=league peut être vide.
                if tag_id in generic_tags:
                    events = await self._get_events_by_tag_slug(league)
                    if not events:
                        all_soccer = await self._get_events_by_tag_slug("soccer")
                        prefix = f"{league}-"
                        events = [e for e in all_soccer if isinstance(e, dict) and (e.get("slug") or "").startswith(prefix)]
                else:
                    events = await self._get_events_by_tag_id(tag_id)
                for ev in events:
                    if not isinstance(ev, dict):
                        continue
                    game_id = ev.get("gameId")
                    slug = (ev.get("slug") or "").strip() or (str(game_id) if game_id is not None else "")
                    if not slug or "-more-markets" in slug.lower():
                        continue
                    title_raw = (ev.get("title") or "").strip()
                    if "halftime" in (title_raw or "").lower() or "halftime" in slug.lower():
                        continue  # sous-marché (Halftime Result), pas le match principal
                    # Ne garder que les matchs déjà commencés (startDate dans le passé)
                    start_s = ev.get("startDate") or ev.get("start_date")
                    if not start_s:
                        continue  # pas de date → on ne peut pas vérifier, on exclut
                    try:
                        start_dt = datetime.fromisoformat(str(start_s).replace("Z", "+00:00"))
                        if start_dt.tzinfo is None:
                            start_dt = start_dt.replace(tzinfo=timezone.utc)
                        if start_dt > datetime.now(timezone.utc):
                            continue  # pas encore commencé
                    except (ValueError, TypeError):
                        continue  # date invalide → on exclut par précaution
                    title = title_raw
                    if " vs. " in title:
                        parts_title = title.split(" vs. ", 1)
                    elif " vs " in title:
                        parts_title = title.split(" vs ", 1)
                    elif " - " in title:
                        # Ex: "FR Cluj - Dinamo București" (Roumanie rou1)
                        parts_title = title.split(" - ", 1)
                    else:
                        continue
                    home_team = parts_title[0].strip()
                    away_team = parts_title[1].strip() if len(parts_title) > 1 else ""
                    score = (ev.get("score") or "0-0").strip().replace("\u2013", "-").replace("\u2014", "-")
                    out.append({
                        "slug": slug,
                        "home_team": home_team,
                        "away_team": away_team,
                        "league": league,
                        "score": score,
                    })
            # Fallback EFL Championship: Polymarket utilise "elc" dans l'URL (elc-por-swa-...).
            # Si /sports ne liste pas "elc" ou "efl", on récupère quand même les events par tag_slug "elc".
            if "elc" in league_slugs or "efl" in league_slugs:
                existing_slugs = {e["slug"] for e in out}
                elc_events = await self._get_events_by_tag_slug("elc")
                for ev in elc_events or []:
                    if not isinstance(ev, dict):
                        continue
                    slug_elc = (ev.get("slug") or "").strip()
                    if not slug_elc or not slug_elc.startswith("elc-") or "-more-markets" in slug_elc.lower() or slug_elc in existing_slugs:
                        continue
                    title_raw = (ev.get("title") or "").strip()
                    if "halftime" in (title_raw or "").lower() or "halftime" in slug_elc.lower():
                        continue
                    start_s = ev.get("startDate") or ev.get("start_date")
                    if start_s:
                        try:
                            start_dt = datetime.fromisoformat(str(start_s).replace("Z", "+00:00"))
                            if start_dt.tzinfo is None:
                                start_dt = start_dt.replace(tzinfo=timezone.utc)
                            if start_dt > datetime.now(timezone.utc):
                                continue
                        except (ValueError, TypeError):
                            pass
                    if " vs. " in title_raw:
                        parts_title = title_raw.split(" vs. ", 1)
                    elif " vs " in title_raw:
                        parts_title = title_raw.split(" vs ", 1)
                    elif " - " in title_raw:
                        parts_title = title_raw.split(" - ", 1)
                    else:
                        continue
                    home_team = parts_title[0].strip()
                    away_team = parts_title[1].strip() if len(parts_title) > 1 else ""
                    score = (ev.get("score") or "0-0").strip().replace("\u2013", "-").replace("\u2014", "-")
                    out.append({
                        "slug": slug_elc,
                        "home_team": home_team,
                        "away_team": away_team,
                        "league": "elc",
                        "score": score,
                    })
                    existing_slugs.add(slug_elc)
        except Exception as e:
            logger.debug(f"Gamma get_live_soccer_events: {e}")
        return out

    # Abréviations pour slug Polymarket (efa FA Cup / elc Championship) : {league}-{home}-{away}-{YYYY-MM-DD}
    _POLY_SLUG_ABBREV: dict[str, str] = {
        "leeds united": "lee",
        "norwich city": "nor",
        "queens park rangers": "qpr",
        "middlesbrough": "mid",
        "west ham united": "whu",
        "brentford": "bre1",
    }
    # Roumanie (rou1) : slug ex. rou1-fcc-din-2026-03-09 (CFR Cluj vs Dinamo București)
    _ROU1_POLY_ABBREV: dict[str, str] = {
        "cfr cluj": "fcc",
        "fc cfr 1907 cluj": "fcc",
        "cluj": "fcc",
        "dinamo bucurești": "din",
        "dinamo bucureşti": "din",  # ş (U+015F) variante courante
        "dinamo bucuresti": "din",
        "dinamo 1948": "din",
        "dinamo": "din",
    }

    def _team_abbrev_for_poly_slug(self, team_name: str) -> str:
        """Abréviation 3 lettres pour slug Polymarket (efa/elc). Ex: Leeds United FC -> lee."""
        key = (team_name or "").strip().lower()
        for alias_key, abbrev in self._POLY_SLUG_ABBREV.items():
            if alias_key in key:
                return abbrev
        words = [w for w in key.split() if w and not w.isdigit() and w not in ("fc", "sk", "cf", "ac", "cfc")]
        if not words:
            return key[:3] if len(key) >= 3 else key
        return words[0][:3] if len(words[0]) >= 3 else (words[0] + "x")[:3]

    def _team_abbrev_for_rou1(self, team_name: str) -> Optional[str]:
        """Abréviation pour slug Polymarket rou1 (ex. CFR Cluj -> fcc, Dinamo București -> din)."""
        key = (team_name or "").strip().lower()
        for alias_key, abbrev in self._ROU1_POLY_ABBREV.items():
            if alias_key in key:
                return abbrev
        return None

    async def try_resolve_slug_for_teams(
        self, league_slug: str, home_team: str, away_team: str
    ) -> Optional[tuple[str, dict]]:
        """
        Pour les ligues non listées par Gamma (ex. rou1), tente de retrouver l'event par slug.
        Retourne (slug, event) si trouvé et les équipes matchent, sinon None.
        """
        league_key = (league_slug or "").strip().lower()
        if league_key != "rou1":
            return None
        ha = self._team_abbrev_for_rou1(home_team)
        aa = self._team_abbrev_for_rou1(away_team)
        if not ha or not aa:
            return None
        today = datetime.utcnow().date()
        for delta in (0, -1, 1):
            d = today + timedelta(days=delta)
            slug = f"rou1-{ha}-{aa}-{d:%Y-%m-%d}"
            ev = await self._get_event_by_slug_or_id(slug)
            if ev is not None and self._event_matches_teams(ev, home_team, away_team):
                return (slug, ev)
        return None

    async def _get_event_by_poly_slug(self, league: str, home_team: str, away_team: str):
        """
        Récupère un event Gamma par slug Polymarket pour efa/elc.
        Format: {league}-{home_abbrev}-{away_abbrev}-{YYYY-MM-DD}
        Ex. efa-lee-nor-2026-03-07 (FA Cup), elc-qpr-mid-2026-03-08 (Championship).
        Polymarket utilise "elc" dans l'URL pour la Championship, pas "efl".
        """
        league_key = (league or "").strip().lower()
        if league_key not in ("efa", "elc"):
            return None
        # Slug utilise le code URL (efa, elc), pas le code Gamma (efl)
        slug_league = league_key
        ha = self._team_abbrev_for_poly_slug(home_team)
        aa = self._team_abbrev_for_poly_slug(away_team)
        if not ha or not aa:
            return None
        today = datetime.utcnow().date()
        for delta in (0, -1, 1):
            d = today + timedelta(days=delta)
            slug = f"{slug_league}-{ha}-{aa}-{d:%Y-%m-%d}"
            ev = await self._get_event_by_slug_or_id(slug)
            if ev is not None:
                return ev
        return None

    @staticmethod
    def _event_matches_teams(event: dict, home_team: str, away_team: str) -> bool:
        """True if the event title refers to the same match (using rapidfuzz-based matching)."""
        title = (event.get("title") or "").strip()
        for sep in (" vs. ", " vs ", " - "):
            if sep in title:
                parts = title.split(sep, 1)
                ev_home, ev_away = parts[0].strip(), parts[1].strip()
                return _teams_match_fn(home_team, ev_home) and _teams_match_fn(away_team, ev_away)
        return False

    async def find_token_for_scoring_team(
        self,
        slug: str,
        home_team: str,
        away_team: str,
        scoring_team: str,
        league: Optional[str] = None,
    ) -> Optional[str]:
        """
        Find the token_id for "scoring team wins" market.
        Essayer d'abord GET /events/{slug}, puis events par tag_id (ligue normalisée).
        """
        # 1) Direct: GET /events/{id} ou /events/slug/{slug} (pas de dépendance ligue)
        ev = await self._get_event_by_slug_or_id(slug)
        if ev:
            for m in ev.get("markets", []):
                token_ids = m.get("clobTokenIds", "[]")
                if isinstance(token_ids, str):
                    try:
                        token_ids = json.loads(token_ids)
                    except (json.JSONDecodeError, TypeError):
                        token_ids = []
                if len(token_ids) >= 2:
                    idx = 0 if scoring_team == "home" else (2 if len(token_ids) > 2 else 1)
                    if idx < len(token_ids):
                        return token_ids[idx]
                    return token_ids[0] if scoring_team == "home" else token_ids[-1]

        # 1b) efa/elc : slug Polymarket {league}-{home}-{away}-{date} (ex. efa-whu-bre1-2026-03-07 FA Cup)
        league_key = (league or "").strip().lower() or None
        if league_key in ("efa", "elc") or (league and self._normalize_league_for_gamma(league) in ("efa", "efl")):
            ev = await self._get_event_by_poly_slug(league or "efa", home_team, away_team)
            if ev:
                for m in ev.get("markets", []):
                    token_ids = m.get("clobTokenIds", "[]")
                    if isinstance(token_ids, str):
                        try:
                            token_ids = json.loads(token_ids)
                        except (json.JSONDecodeError, TypeError):
                            token_ids = []
                    if len(token_ids) >= 2:
                        idx = 0 if scoring_team == "home" else (2 if len(token_ids) > 2 else 1)
                        if idx < len(token_ids):
                            return token_ids[idx]
                        return token_ids[0] if scoring_team == "home" else token_ids[-1]
        # 1c) Slug numérique sans ligue : tenter efa (FA Cup) puis elc (Championship) par slug avant tag_id
        if slug and slug.isdigit() and not (league or "").strip():
            for cup_league in ("efa", "elc"):
                ev = await self._get_event_by_poly_slug(cup_league, home_team, away_team)
                if ev:
                    for m in ev.get("markets", []):
                        token_ids = m.get("clobTokenIds", "[]")
                        if isinstance(token_ids, str):
                            try:
                                token_ids = json.loads(token_ids)
                            except (json.JSONDecodeError, TypeError):
                                token_ids = []
                        if len(token_ids) >= 2:
                            idx = 0 if scoring_team == "home" else (2 if len(token_ids) > 2 else 1)
                            if idx < len(token_ids):
                                return token_ids[idx]
                            return token_ids[0] if scoring_team == "home" else token_ids[-1]

        # 2) Slug numérique + ligue OU ligue UCL (slug texte ucl-ata1-bay1-... peut échouer en GET direct) → events par tag
        leagues_to_try: list[str] = []
        if slug and slug.isdigit():
            if (league or "").strip():
                leagues_to_try = [self._normalize_league_for_gamma(league)]
            else:
                leagues_to_try = list(self.LEAGUES_TO_TRY_WHEN_UNKNOWN)
        # UCL : même avec slug texte (ex. ucl-ata1-bay1-2026-03-10), tenter par tag_slug si pas trouvé en 1)
        elif (league or "").strip() and self._normalize_league_for_gamma(league) == "ucl":
            leagues_to_try = ["ucl"]
        for leg in leagues_to_try:
            events: list = []
            if leg == "ucl":
                events = await self._get_events_by_tag_id(self.UCL_MATCHES_TAG_ID)
            else:
                tag_id = await self._get_tag_id_for_league(leg)
                if tag_id is not None:
                    events = await self._get_events_by_tag_id(tag_id)
            if not events and leg == "ucl":
                for tag in self.UCL_TAG_SLUGS:
                    events = await self._get_events_by_tag_slug(tag)
                    if events:
                        break
            elif not events:
                tag_slugs = (leg,)
                for tag in tag_slugs:
                    events = await self._get_events_by_tag_slug(tag)
                    if events:
                        break
            for ev in events:
                if not self._event_matches_teams(ev, home_team, away_team):
                    continue
                if slug and slug.isdigit() and (ev.get("gameId") is not None):
                    if str(ev.get("gameId")) != slug:
                        continue
                for m in ev.get("markets", []):
                    token_ids = m.get("clobTokenIds", "[]")
                    if isinstance(token_ids, str):
                        try:
                            token_ids = json.loads(token_ids)
                        except (json.JSONDecodeError, TypeError):
                            token_ids = []
                    if len(token_ids) >= 2:
                        idx = 0 if scoring_team == "home" else (2 if len(token_ids) > 2 else 1)
                        if idx < len(token_ids):
                            return token_ids[idx]
                        return token_ids[0] if scoring_team == "home" else token_ids[-1]

        # 3) Broad fallback: search "sports" and "soccer" tags
        client = await self._get_client()
        attempts = [{"slug": slug, "closed": "false", "limit": 20}]
        if slug and not slug.isdigit():
            attempts.insert(0, {"slug": slug})
        for tag in ("sports", "soccer"):
            attempts.append({"tag_slug": tag, "closed": "false", "limit": 100})
        for attempt in attempts:
            try:
                r = await client.get(f"{GAMMA_URL}/events", params=attempt)
                if r.status_code != 200:
                    continue
                events_list = r.json()
                if not isinstance(events_list, list):
                    continue
                for ev in events_list:
                    if not self._event_matches_teams(ev, home_team, away_team):
                        continue
                    for m in ev.get("markets", []):
                        token_ids = m.get("clobTokenIds", "[]")
                        if isinstance(token_ids, str):
                            try:
                                token_ids = json.loads(token_ids)
                            except (json.JSONDecodeError, TypeError):
                                token_ids = []
                        if len(token_ids) >= 2:
                            idx = 0 if scoring_team == "home" else (2 if len(token_ids) > 2 else 1)
                            if idx < len(token_ids):
                                return token_ids[idx]
                            return token_ids[0] if scoring_team == "home" else token_ids[-1]
            except Exception as e:
                logger.debug(f"Gamma search failed: {e}")

        logger.info(
            f"Marché non trouvé Gamma: slug={slug!r} home={home_team!r} away={away_team!r}"
        )
        return None
    
    async def find_tokens_for_match(
        self,
        slug: str,
        home_team: str,
        away_team: str,
        league: Optional[str] = None,
    ) -> tuple[Optional[str], Optional[str], Optional[str]]:
        """
        Find token_id for Home, Draw, Away (3 outcomes).
        Returns (token_home, token_draw, token_away). token_draw is None si marché 2 issues.
        """
        def _trio(token_ids: list) -> tuple[Optional[str], Optional[str], Optional[str]]:
            if len(token_ids) >= 3:
                return (token_ids[0], token_ids[1], token_ids[2])
            if len(token_ids) >= 2:
                return (token_ids[0], None, token_ids[1])
            return (None, None, None)

        def _parse_markets_1x2(
            ev: dict, home_team: str, away_team: str
        ) -> tuple[Optional[str], Optional[str], Optional[str]] | None:
            """
            Polymarket soccer: souvent 3 marchés Yes/No séparés (Home win, Draw, Away win).
            On associe chaque marché à home/draw/away via la question, token Yes = premier token.
            """
            home_upper = (home_team or "").upper()
            away_upper = (away_team or "").upper()
            q_lower = lambda q: (q or "").lower()
            token_home, token_draw, token_away = None, None, None
            for m in ev.get("markets", []):
                token_ids = m.get("clobTokenIds", "[]")
                if isinstance(token_ids, str):
                    try:
                        token_ids = json.loads(token_ids)
                    except (json.JSONDecodeError, TypeError):
                        token_ids = []
                if len(token_ids) < 2:
                    continue
                q = q_lower(m.get("question") or "")
                # "Will X win" -> home si X = home_team, away si X = away_team; "end in a draw" -> draw
                if "draw" in q or "tie" in q:
                    token_draw = token_ids[0]
                elif "win" in q:
                    hq = q_lower(home_team or "")
                    aq = q_lower(away_team or "")
                    if hq and hq in q:
                        token_home = token_ids[0]
                    elif aq and aq in q:
                        token_away = token_ids[0]
                    else:
                        # Fallback: ordre des marchés = home, draw, away
                        if token_home is None:
                            token_home = token_ids[0]
                        elif token_away is None:
                            token_away = token_ids[0]
            if token_home is not None or token_away is not None:
                return (token_home, token_draw, token_away)
            return None

        def _best_trio(ev: dict, home_team: str, away_team: str) -> tuple[Optional[str], Optional[str], Optional[str]] | None:
            """Un seul marché 3 issues > 3 marchés 1X2 (Home/Draw/Away) > premier marché 2 issues."""
            for m in ev.get("markets", []):
                token_ids = m.get("clobTokenIds", "[]")
                if isinstance(token_ids, str):
                    try:
                        token_ids = json.loads(token_ids)
                    except (json.JSONDecodeError, TypeError):
                        token_ids = []
                if len(token_ids) >= 3:
                    return _trio(token_ids)
            parsed = _parse_markets_1x2(ev, home_team, away_team)
            if parsed is not None:
                return parsed
            for m in ev.get("markets", []):
                token_ids = m.get("clobTokenIds", "[]")
                if isinstance(token_ids, str):
                    try:
                        token_ids = json.loads(token_ids)
                    except (json.JSONDecodeError, TypeError):
                        token_ids = []
                if len(token_ids) >= 2:
                    return _trio(token_ids)
            return None

        # 1) Essayer d'abord GET /events/{slug} (fonctionne quand slug = id Gamma, pas de dépendance ligue)
        ev = await self._get_event_by_slug_or_id(slug)
        if ev:
            trio = _best_trio(ev, home_team, away_team)
            if trio is not None:
                return trio
        # 1b) efa/elc : slug Polymarket {league}-{home}-{away}-{date} (ex. efa-whu-bre1-2026-03-07 FA Cup)
        if slug and league and (league.lower() in ("efa", "elc") or self._normalize_league_for_gamma(league) in ("efa", "efl")):
            ev = await self._get_event_by_poly_slug(league, home_team, away_team)
            if ev:
                trio = _best_trio(ev, home_team, away_team)
                if trio is not None:
                    return trio
        # 1c) Slug numérique sans ligue : tenter efa (FA Cup) puis elc (Championship) par slug
        if slug and slug.isdigit() and not (league or "").strip():
            for cup_league in ("efa", "elc"):
                ev = await self._get_event_by_poly_slug(cup_league, home_team, away_team)
                if ev:
                    trio = _best_trio(ev, home_team, away_team)
                    if trio is not None:
                        return trio
        # 2) Sinon: events par tag_id ou tag_slug (ligue normalisée; UCL etc. sans tag_id → tag_slug=league)
        leagues_to_try: list[str] = []
        if slug and slug.isdigit():
            if (league or "").strip():
                leagues_to_try = [self._normalize_league_for_gamma(league)]
            else:
                leagues_to_try = list(self.LEAGUES_TO_TRY_WHEN_UNKNOWN)
        elif (league or "").strip() and self._normalize_league_for_gamma(league) == "ucl":
            leagues_to_try = ["ucl"]
        for leg in leagues_to_try:
            events_list: list = []
            # UCL: toujours utiliser le tag des matchs (100977), pas Winner/Top Scorer
            if leg == "ucl":
                events_list = await self._get_events_by_tag_id(self.UCL_MATCHES_TAG_ID)
            else:
                tag_id = await self._get_tag_id_for_league(leg)
                if tag_id is not None:
                    events_list = await self._get_events_by_tag_id(tag_id)
            if not events_list and leg == "ucl":
                for tag in self.UCL_TAG_SLUGS:
                    events_list = await self._get_events_by_tag_slug(tag)
                    if events_list:
                        break
            elif not events_list:
                tag_slugs = (leg,)
                for tag in tag_slugs:
                    events_list = await self._get_events_by_tag_slug(tag)
                    if events_list:
                        break
            for ev in events_list:
                if not self._event_matches_teams(ev, home_team, away_team):
                    continue
                # Slug numérique = gameId WS : privilégier l'event dont gameId correspond
                if slug and slug.isdigit() and (ev.get("gameId") is not None):
                    if str(ev.get("gameId")) != slug:
                        continue
                trio = _best_trio(ev, home_team, away_team)
                if trio is not None:
                    return trio

        # 3) Broad fallback: search "sports" and "soccer" tags
        client = await self._get_client()
        attempts = [{"slug": slug, "closed": "false", "limit": 20}]
        if slug and not slug.isdigit():
            attempts.insert(0, {"slug": slug})
        for tag in ("sports", "soccer"):
            attempts.append({"tag_slug": tag, "closed": "false", "limit": 100})
        for attempt in attempts:
            try:
                r = await client.get(f"{GAMMA_URL}/events", params=attempt)
                if r.status_code != 200:
                    continue
                events_found = r.json()
                if not isinstance(events_found, list):
                    continue
                for ev in events_found:
                    if not self._event_matches_teams(ev, home_team, away_team):
                        continue
                    trio = _best_trio(ev, home_team, away_team)
                    if trio is not None:
                        return trio
            except Exception as e:
                logger.debug(f"Gamma find_tokens: {e}")
        return (None, None, None)
    
    async def get_midpoint(self, token_id: str) -> Optional[float]:
        """Get current midpoint price for token"""
        try:
            client = await self._get_client()
            r = await client.get(f"{CLOB_URL}/midpoint", params={"token_id": token_id})
            if r.status_code != 200:
                return None
            data = r.json()
            return float(data.get("mid", 0))
        except Exception as e:
            logger.debug(f"Midpoint error: {e}")
            return None

    async def get_ask(self, token_id: str) -> Optional[float]:
        """Prix à l'ask (side=BUY = meilleure offre à l'achat). Pour backtest réaliste."""
        try:
            client = await self._get_client()
            r = await client.get(f"{CLOB_URL}/price", params={"token_id": token_id, "side": "BUY"})
            if r.status_code != 200:
                return None
            data = r.json()
            price = data.get("price")
            if price is not None:
                return float(price)
            return None
        except Exception as e:
            logger.debug(f"Ask price error: {e}")
            return None

    async def get_bid(self, token_id: str) -> Optional[float]:
        """Prix au bid (side=SELL = meilleure offre à la vente)."""
        try:
            client = await self._get_client()
            r = await client.get(f"{CLOB_URL}/price", params={"token_id": token_id, "side": "SELL"})
            if r.status_code != 200:
                return None
            data = r.json()
            price = data.get("price")
            if price is not None:
                return float(price)
            return None
        except Exception as e:
            logger.debug(f"Bid price error: {e}")
            return None

    async def get_price_for_record(self, token_id: str) -> Optional[float]:
        """Prix pour enregistrement (backtest réaliste): ask si dispo, sinon midpoint."""
        p = await self.get_ask(token_id)
        if p is not None:
            return p
        return await self.get_midpoint(token_id)
    
    async def track_prices_after_goal(
        self,
        slug: str,
        home_team: str,
        away_team: str,
        scoring_team: str,
        duration_seconds: int = 120,
        league: Optional[str] = None,
        token_id: Optional[str] = None,
    ) -> PriceCurveAnalysis:
        """
        When a goal is detected, track the scoring team's odds for 120 seconds.
        Returns analysis: time to stabilize, profit window, etc.
        If token_id is provided (e.g. from live cache), use it and skip lookup.
        """
        goal_ts = datetime.now()
        samples: list[PriceSample] = []
        
        if not token_id:
            token_id = await self.find_token_for_scoring_team(
                slug, home_team, away_team, scoring_team, league=league
            )
        if not token_id:
            return PriceCurveAnalysis(
                goal_timestamp=goal_ts.isoformat(),
                match_slug=slug,
                scoring_team=scoring_team,
                home_team=home_team,
                away_team=away_team,
                samples=[],
                market_found=False,
            )
        
        entry_ask_0s: Optional[float] = None
        exit_bid_60s: Optional[float] = None
        # Sample at each interval
        for target_sec in SAMPLE_INTERVALS:
            if target_sec > duration_seconds:
                break
            # Wait until we reach this second
            elapsed = 0
            while elapsed < target_sec:
                await asyncio.sleep(0.5)
                elapsed = (datetime.now() - goal_ts).total_seconds()
            
            price = await self.get_price_for_record(token_id)
            if target_sec == 0:
                ask_0 = await self.get_ask(token_id)
                if ask_0 is not None:
                    entry_ask_0s = ask_0
            if target_sec == 60:
                bid_60 = await self.get_bid(token_id)
                if bid_60 is not None:
                    exit_bid_60s = bid_60
            if price is not None:
                samples.append(PriceSample(
                    seconds_after_goal=target_sec,
                    price=price,
                    timestamp=datetime.now().isoformat(),
                ))
        
        # Analyze
        analysis = self._analyze_curve(
            goal_ts=goal_ts,
            slug=slug,
            home_team=home_team,
            away_team=away_team,
            scoring_team=scoring_team,
            samples=samples,
            token_id=token_id,
            entry_ask_0s=entry_ask_0s,
            exit_bid_60s=exit_bid_60s,
        )
        
        # Save to CSV
        self._save_curve_csv(analysis)
        
        return analysis
    
    def _analyze_curve(
        self,
        goal_ts: datetime,
        slug: str,
        home_team: str,
        away_team: str,
        scoring_team: str,
        samples: list[PriceSample],
        token_id: str,
        entry_ask_0s: Optional[float] = None,
        exit_bid_60s: Optional[float] = None,
    ) -> PriceCurveAnalysis:
        """Analyze the price curve"""
        prices_by_sec = {s.seconds_after_goal: s.price for s in samples}
        
        price_0 = prices_by_sec.get(0)
        price_60 = prices_by_sec.get(60)
        price_120 = prices_by_sec.get(120)
        
        # Time to stabilize: first moment when change < 0.5% for 10s
        time_stabilize = None
        sorted_secs = sorted(prices_by_sec.keys())
        for i, sec in enumerate(sorted_secs):
            if i + 2 >= len(sorted_secs):
                break
            window = [prices_by_sec.get(s) for s in sorted_secs[i:i+3] if s in prices_by_sec]
            if len(window) >= 2 and all(p for p in window):
                    max_p, min_p = max(window), min(window)
                    if max_p > 0 and (max_p - min_p) / max_p < STABILIZE_THRESHOLD:
                        time_stabilize = sec
                        break
        
        # Best exit: when price was highest
        max_sec = max(prices_by_sec.keys(), key=lambda s: prices_by_sec[s]) if prices_by_sec else None
        min_sec = min(prices_by_sec.keys(), key=lambda s: prices_by_sec[s]) if prices_by_sec else None
        
        # Profit if we enter at T+0 and exit at T+60
        profit_0_60 = None
        profit_0_120 = None
        if price_0 and price_0 > 0:
            if price_60:
                profit_0_60 = (price_60 / price_0 - 1) * 100
            if price_120:
                profit_0_120 = (price_120 / price_0 - 1) * 100
        
        # Profit window: how long does price stay above entry?
        profit_window = None
        if price_0 and samples:
            for s in sorted(samples, key=lambda x: x.seconds_after_goal):
                if s.price > price_0:
                    profit_window = s.seconds_after_goal
                    break
        
        return PriceCurveAnalysis(
            goal_timestamp=goal_ts.isoformat(),
            match_slug=slug,
            scoring_team=scoring_team,
            home_team=home_team,
            away_team=away_team,
            samples=samples,
            price_at_0s=price_0,
            price_at_60s=price_60,
            price_at_120s=price_120,
            entry_ask_0s=entry_ask_0s,
            exit_bid_60s=exit_bid_60s,
            time_to_stabilize_seconds=time_stabilize,
            max_price_seconds=max_sec,
            min_price_seconds=min_sec,
            profit_if_entry_0s_exit_60s=profit_0_60,
            profit_if_entry_0s_exit_120s=profit_0_120,
            profit_window_seconds=profit_window,
            token_id=token_id,
            market_found=True,
        )
    
    def _get_price_at(self, samples: list, sec: int) -> Optional[float]:
        for s in samples:
            if s.seconds_after_goal == sec:
                return s.price
        return None

    def _save_curve_csv(self, a: PriceCurveAnalysis):
        """Append analysis to CSV"""
        p1 = self._get_price_at(a.samples, 1)
        p5 = self._get_price_at(a.samples, 5)
        p10 = self._get_price_at(a.samples, 10)
        p30 = self._get_price_at(a.samples, 30)
        
        row = [
            a.goal_timestamp,
            a.match_slug,
            a.scoring_team,
            a.home_team,
            a.away_team,
            f"{a.price_at_0s:.3f}" if a.price_at_0s else "",
            f"{p1:.3f}" if p1 is not None else "",
            f"{p5:.3f}" if p5 is not None else "",
            f"{p10:.3f}" if p10 is not None else "",
            f"{p30:.3f}" if p30 is not None else "",
            f"{a.price_at_60s:.3f}" if a.price_at_60s else "",
            f"{a.price_at_120s:.3f}" if a.price_at_120s else "",
            a.time_to_stabilize_seconds or "",
            a.max_price_seconds or "",
            f"{a.profit_if_entry_0s_exit_60s:.1f}" if a.profit_if_entry_0s_exit_60s is not None else "",
            "yes" if a.market_found else "no",
        ]
        try:
            with open(self._curve_csv, "a", newline="") as f:
                csv.writer(f).writerow(row)
        except OSError as e:
            logger.warning(f"Could not append to price curve CSV {self._curve_csv}: {e}")
    
    def format_analysis_report(self, a: PriceCurveAnalysis) -> str:
        """Format analysis for WORK_LOG"""
        lines = [
            f"### But: {a.home_team} vs {a.away_team} - {a.scoring_team} a marqué",
            f"- **Prix T+0:** {a.price_at_0s:.1%}" if a.price_at_0s else "",
            f"- **Prix T+60:** {a.price_at_60s:.1%}" if a.price_at_60s else "",
            f"- **Stabilisation:** {a.time_to_stabilize_seconds}s" if a.time_to_stabilize_seconds else "- **Stabilisation:** N/A",
            f"- **Meilleur exit:** T+{a.max_price_seconds}s" if a.max_price_seconds else "",
            f"- **Profit T+0→60:** {a.profit_if_entry_0s_exit_60s:+.1f}%" if a.profit_if_entry_0s_exit_60s is not None else "",
            f"- **Assez de temps pour trader?** {'✅ OUI' if (a.profit_if_entry_0s_exit_60s or 0) > 0 else '❌ NON' if a.market_found else '⚠️ Marché non trouvé'}",
        ]
        return "\n".join(l for l in lines if l)
