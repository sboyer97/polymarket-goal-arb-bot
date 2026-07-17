"""
TheSports client MQTT over WebSockets pour la détection de buts en direct.

Protocole: MQTT over WebSockets (voir doc TheSports).
- Payload: liste de matchs ou objet { "results": [...] }. Chaque match: id, score, stats, incidents, tlive.
- score: array [match_id, status, home_array, away_array, ...], home_array[0]=buts domicile, away_array[0]=buts extérieur.
- incidents: array de { type, position (1=home, 2=away), time, home_score, away_score, ... }.
"""

import asyncio
import json
import threading
from typing import Callable, Optional

import paho.mqtt.client as mqtt
from loguru import logger

# Code type incident TheSports pour les buts
INCIDENT_GOAL = 1


def _norm(s: str) -> str:
    return (s or "").strip()


def _normalize_payload(data) -> list:
    """Retourne une liste de matchs (objets avec id, score, stats, incidents)."""
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        if "results" in data:
            return data.get("results") or []
        return [data]
    return []


def _parse_score_entry(entry: dict) -> Optional[tuple[str, str, str, str, str]]:
    """
    Extrait (home_team, away_team, score_str, scoring_team, minute) d'un item score.
    """
    if not isinstance(entry, dict):
        return None
    home = (
        _norm(entry.get("home_team") or entry.get("homeTeam") or entry.get("strHomeTeam") or "")
    )
    away = (
        _norm(entry.get("away_team") or entry.get("awayTeam") or entry.get("strAwayTeam") or "")
    )
    h = entry.get("home_score") or entry.get("homeScore") or entry.get("intHomeScore")
    a = entry.get("away_score") or entry.get("awayScore") or entry.get("intAwayScore")
    if h is None or a is None:
        try:
            h = int(h) if h is not None else 0
            a = int(a) if a is not None else 0
        except (TypeError, ValueError):
            return None
    else:
        try:
            h, a = int(h), int(a)
        except (TypeError, ValueError):
            return None
    score_str = f"{h}-{a}"
    scoring_team = "home"
    minute = _norm(
        entry.get("minute")
        or entry.get("strProgress")
        or entry.get("elapsed")
        or entry.get("period")
        or "?"
    )[:20]
    return (home, away, score_str, scoring_team, minute)


def _parse_score_array(score_arr) -> tuple[int, int]:
    """
    score = [match_id, status, home_array, away_array, ...]
    home_array[0] = buts domicile (regular time), away_array[0] = buts extérieur.
    """
    if not isinstance(score_arr, list) or len(score_arr) < 4:
        return (0, 0)
    home_arr = score_arr[2] if isinstance(score_arr[2], list) else []
    away_arr = score_arr[3] if isinstance(score_arr[3], list) else []
    h = int(home_arr[0]) if len(home_arr) > 0 else 0
    a = int(away_arr[0]) if len(away_arr) > 0 else 0
    try:
        return (h, a)
    except (TypeError, ValueError):
        return (0, 0)


def _process_match_item(
    item: dict,
    last_scores: dict,
    incidents_fired: set,
    loop: asyncio.AbstractEventLoop,
    on_goal: Callable[..., object],
    resolve_match_id: Optional[Callable[[str], Optional[tuple[str, str]]]],
) -> None:
    """
    Traite un match au format API TheSports: id, score (array), incidents (array).
    incidents_fired: set de (key, type, signature) pour ne déclencher qu'une fois par événement.
    """
    match_id = _norm(str(item.get("id") or ""))
    score_arr = item.get("score")
    incidents = item.get("incidents") or []
    stats = item.get("stats") or []
    if not isinstance(incidents, list):
        incidents = []
    if not isinstance(stats, list):
        stats = []

    h, a = _parse_score_array(score_arr) if isinstance(score_arr, list) else (0, 0)
    score_str = f"{h}-{a}"

    # Debug serveur: on récupère bien la data TheSports live
    logger.info(
        "TheSports live: match_id={} score={} incidents={} stats={}",
        match_id or "(no id)",
        score_str,
        len(incidents),
        len(stats),
    )

    home_team, away_team = "", ""
    if resolve_match_id and match_id:
        resolved = resolve_match_id(match_id)
        if resolved:
            home_team, away_team = _norm(resolved[0]), _norm(resolved[1])

    key = f"{home_team}|{away_team}" if (home_team and away_team) else match_id or ""
    prev = last_scores.get(key, (0, 0))
    last_scores[key] = (h, a)
    old_score = f"{prev[0]}-{prev[1]}"

    for inc in incidents:
        if not isinstance(inc, dict):
            continue
        inc_type = inc.get("type")
        minute = str(inc.get("time") or inc.get("minute") or "?")
        add_time = inc.get("add_time")
        if add_time is not None:
            minute = f"{minute}+{add_time}"
        position = inc.get("position", 1)
        team_side = "away" if position == 2 else "home"
        inc_h = inc.get("home_score")
        inc_a = inc.get("away_score")
        if inc_h is not None and inc_a is not None:
            inc_score = f"{int(inc_h)}-{int(inc_a)}"
        else:
            inc_score = score_str

        if inc_type == INCIDENT_GOAL and home_team and away_team and on_goal:
            event_key = (key, "goal", inc_score)
            if event_key in incidents_fired:
                continue
            incidents_fired.add(event_key)
            scoring_team = "home" if position == 1 else "away"
            if asyncio.iscoroutinefunction(on_goal):
                asyncio.run_coroutine_threadsafe(
                    on_goal(home_team, away_team, inc_score, scoring_team, minute, old_score),
                    loop,
                )
            else:
                on_goal(home_team, away_team, inc_score, scoring_team, minute, old_score)


def _process_payload(
    data: dict,
    last_scores: dict,
    loop: asyncio.AbstractEventLoop,
    on_goal: Callable[..., object],
) -> None:
    """
    Traite un payload au format ancien (dict avec score/scores list d'entrées, incidents).
    Appelle les callbacks via run_coroutine_threadsafe depuis le thread MQTT.
    """
    score_list = data.get("score") or data.get("scores")
    if not isinstance(score_list, list):
        score_list = []
    for entry in score_list:
        parsed = _parse_score_entry(entry)
        if not parsed:
            continue
        home_team, away_team, score_str, scoring_team, minute = parsed
        if not home_team or not away_team:
            continue
        try:
            parts = score_str.split("-")
            if len(parts) < 2:
                continue
            h, a = int(parts[0].strip()), int(parts[1].strip())
        except (ValueError, IndexError):
            continue
        key = f"{home_team}|{away_team}"
        prev = last_scores.get(key, (0, 0))
        ph, pa = prev
        last_scores[key] = (h, a)
        if h > ph or a > pa:
            if h > ph and a == pa:
                scoring_team = "home"
            elif a > pa and h == ph:
                scoring_team = "away"
            else:
                scoring_team = "home" if (h - ph) >= (a - pa) else "away"
            old_score = f"{ph}-{pa}"
            if asyncio.iscoroutinefunction(on_goal):
                asyncio.run_coroutine_threadsafe(
                    on_goal(home_team, away_team, score_str, scoring_team, minute, old_score),
                    loop,
                )
            else:
                on_goal(home_team, away_team, score_str, scoring_team, minute, old_score)

    incidents = data.get("incidents") or data.get("incident")
    if not isinstance(incidents, list):
        return
    for inc in incidents:
        if not isinstance(inc, dict):
            continue
        inc_type = _norm(
            str(inc.get("type") or inc.get("incident_type") or inc.get("card_type") or "")
        ).lower()
        minute = _norm(
            inc.get("minute")
            or inc.get("strProgress")
            or inc.get("elapsed")
            or "?"
        )[:20]

        if "goal" not in inc_type and "but" not in inc_type:
            continue
        home_team = _norm(
            inc.get("home_team")
            or inc.get("homeTeam")
            or inc.get("strHomeTeam")
            or ""
        )
        away_team = _norm(
            inc.get("away_team")
            or inc.get("awayTeam")
            or inc.get("strAwayTeam")
            or ""
        )
        if not home_team or not away_team:
            continue
        h = inc.get("home_score") or inc.get("homeScore") or inc.get("intHomeScore")
        a = inc.get("away_score") or inc.get("awayScore") or inc.get("intAwayScore")
        if h is None or a is None:
            continue
        try:
            h, a = int(h), int(a)
        except (TypeError, ValueError):
            continue
        score_str = f"{h}-{a}"
        scoring_team = _norm(
            str(inc.get("scoring_team") or inc.get("team") or "home")
        ).lower()
        if scoring_team not in ("home", "away"):
            scoring_team = "home"
        key_inc = f"{home_team}|{away_team}"
        prev_inc = last_scores.get(key_inc, (0, 0))
        old_score_inc = f"{prev_inc[0]}-{prev_inc[1]}"
        last_scores[key_inc] = (h, a)
        if asyncio.iscoroutinefunction(on_goal):
            asyncio.run_coroutine_threadsafe(
                on_goal(
                    home_team,
                    away_team,
                    score_str,
                    scoring_team,
                    minute,
                    old_score_inc,
                ),
                loop,
            )
        else:
            on_goal(
                home_team,
                away_team,
                score_str,
                scoring_team,
                minute,
                old_score_inc,
            )


async def run_thesports_ws_loop(
    host: str,
    port: int,
    user: str,
    secret: str,
    topic: str,
    on_goal: Callable[..., object],
    running: Callable[[], bool],
    last_scores: Optional[dict] = None,
    resolve_match_id: Optional[Callable[[str], Optional[tuple[str, str]]]] = None,
) -> None:
    """
    Connexion MQTT over WebSockets. Payload: liste de matchs ou { results: [] }.
    Chaque match: id, score (array), stats, incidents. Si resolve_match_id(match_id) retourne
    (home_team, away_team), les callbacks sont appelés; sinon debug log uniquement.
    """
    if last_scores is None:
        last_scores = {}
    loop = asyncio.get_running_loop()
    # Un événement = un seul callback (évite 4x le même but si MQTT renvoie 4x le même message)
    incidents_fired: set = set()
    _INCIDENTS_FIRED_MAX = 2000
    client_ref: dict = {}  # partagé avec run_mqtt et finally pour loop_stop/disconnect

    def on_connect(c, userdata, flags, rc):
        if rc == 0:
            c.subscribe(topic)
            logger.info("TheSports MQTT connecté, topic {}", topic)
        elif rc in (4, 5):
            logger.warning(
                "TheSports MQTT auth échouée (rc={}). Vérifier user, secret et IP whitelist.",
                rc,
            )
        else:
            logger.warning("TheSports MQTT connexion refusée rc={}", rc)

    def _is_new_format(item: dict) -> bool:
        """True si le match est au format API (id + score array ou id + stats/incidents)."""
        if not isinstance(item, dict):
            return False
        score_arr = item.get("score")
        has_id = bool(item.get("id"))
        has_score_array = (
            isinstance(score_arr, list)
            and len(score_arr) >= 4
            and isinstance(score_arr[2], list)
            and isinstance(score_arr[3], list)
        )
        return has_id and (has_score_array or "stats" in item or "incidents" in item)

    def on_message(c, userdata, msg):
        try:
            data = json.loads(msg.payload)
        except json.JSONDecodeError:
            logger.debug("TheSports MQTT payload non-JSON: {}", msg.payload[:200])
            return
        items = _normalize_payload(data)
        for item in items:
            if not isinstance(item, dict):
                continue
            if _is_new_format(item):
                if len(incidents_fired) > _INCIDENTS_FIRED_MAX:
                    incidents_fired.clear()
                _process_match_item(
                    item,
                    last_scores,
                    incidents_fired,
                    loop,
                    on_goal,
                    resolve_match_id,
                )
            else:
                _process_payload(
                    item,
                    last_scores,
                    loop,
                    on_goal,
                )

    def run_mqtt():
        client = mqtt.Client(transport="websockets")
        client.tls_set()
        client.username_pw_set(username=user, password=secret)
        client.on_connect = on_connect
        client.on_message = on_message
        try:
            client.connect(host, port, keepalive=60)
        except Exception as e:
            logger.warning("TheSports MQTT connect() failed: {}", str(e))
            return
        client_ref["client"] = client
        client.loop_start()

    thread = threading.Thread(target=run_mqtt, daemon=True)
    thread.start()

    try:
        while running():
            await asyncio.sleep(1)
    finally:
        c = client_ref.get("client")
        if c:
            try:
                c.loop_stop()
                c.disconnect()
            except Exception:
                pass
