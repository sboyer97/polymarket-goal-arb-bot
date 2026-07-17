#!/usr/bin/env python3
"""
🤖 Live Trading System with Auto-Improvement

This system:
1. Collects live soccer data from Polymarket → CSV
2. Runs live backtest on collected data
3. Launches AI agents to analyze and improve strategy
4. Updates WORK_LOG.md with findings
5. Loops forever, continuously improving

Usage:
    python live_system.py
"""

import asyncio
import csv
import json
import os
import re
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from dataclasses import dataclass
from typing import Optional
import statistics

import httpx
import websockets
from dotenv import load_dotenv
from loguru import logger

load_dotenv(Path(__file__).parent / ".env")
from rich.console import Console

from config.settings import settings

# Forcer DRY_RUN depuis l'env (évite souci de préfixe TRADING_ / parsing pydantic)
_dry_run_env = os.environ.get("DRY_RUN") or os.environ.get("TRADING_DRY_RUN") or ""
if _dry_run_env.strip().lower() in ("false", "0", "no", "off"):
    settings.trading.dry_run = False
from src.price_tracker import PriceTracker
from src.sportmonks_client import SportmonksClient
from src.team_matching import teams_match as _teams_match_fn
from src.thesports_ws import run_thesports_ws_loop
from src.polymarket.client import PolymarketClient
from src.utils.models import Order, OrderSide
from src.notifications import notify_trade, notify_error, notify_system, notify_sell_failed, notify_matches_followed, notify_goal
from src.trade_logger import trade_logger
from rich.table import Table
from rich.panel import Panel
from rich.live import Live

# Paths (avant Console pour pouvoir tee stdout/stderr)
PROJECT_ROOT = Path(__file__).parent
DATA_DIR = PROJECT_ROOT / "data" / "live"
DATA_DIR.mkdir(parents=True, exist_ok=True)
WORK_LOG = PROJECT_ROOT / "WORK_LOG.md"

# Fichier log console = tout ce qui s'affiche (stdout + stderr, Rich + loguru)
CONSOLE_LOG_FILE = DATA_DIR / "console.log"


class _Tee:
    """Écrit vers le stream d'origine et vers un fichier."""
    def __init__(self, stream, path: Path):
        self._stream = stream
        self._path = path
        self._file = open(path, "a", encoding="utf-8", errors="replace")
        self._file.write(f"\n=== Session {datetime.now(timezone.utc).isoformat()} ===\n")
        self._file.flush()

    def write(self, data: str):
        self._stream.write(data)
        try:
            self._file.write(data)
            self._file.flush()
        except OSError:
            pass

    def flush(self):
        self._stream.flush()
        try:
            self._file.flush()
        except OSError:
            pass

    def isatty(self):
        return self._stream.isatty()


sys.stdout = _Tee(sys.__stdout__, CONSOLE_LOG_FILE)
sys.stderr = _Tee(sys.__stderr__, CONSOLE_LOG_FILE)

# Log file dédié (loguru) pour debug TP/SL
LOG_FILE = DATA_DIR / "live_system.log"
logger.add(LOG_FILE, rotation="10 MB", retention="3 days", level="DEBUG")

console = Console()


@dataclass
class GoalRecord:
    """A recorded goal - PnL only from REAL PriceTracker data, never simulated"""
    timestamp: datetime
    match: str
    league: str
    home_team: str
    away_team: str
    scoring_team: str
    minute: str
    score_before: str
    score_after: str
    entry_odds: Optional[float] = None  # REAL from Polymarket CLOB
    exit_odds: Optional[float] = None   # REAL from Polymarket CLOB
    pnl: Optional[float] = None         # REAL: (exit/entry - 1) * bet - fees


class LiveTradingSystem:
    """
    Complete live system:
    - Data collection
    - Live backtesting
    - Strategy analysis
    - Auto-improvement
    """
    
    WS_URL = "wss://sports-api.polymarket.com/ws"
    GAMMA_URL = "https://gamma-api.polymarket.com"
    # Tous les codes soccer/football Polymarket (tag 100350 Gamma + anciens codes WS)
    SOCCER_LEAGUES = frozenset([
        "afc", "arg", "aus", "bol1", "bra", "bra2", "bun", "caf",
        "cde", "cdr", "chi", "chi1", "cof", "col", "col1", "con",
        "cze1", "den", "dfb", "efa", "efl", "egy1", "epl", "ere",
        "fif", "fl1", "ind", "itc", "ja2", "jap", "kor", "lal",
        "lcs", "lib", "mar1", "mex", "mls", "nor", "ofc", "per1",
        "por", "rou1", "rus", "sea", "spl", "ssc", "sud", "tur",
        "ucl", "uef", "uel", "ukr1", "uwcl",
        "soccer",
        # Anciens codes / alias (WS peut les envoyer)
        "laliga", "seriea", "bundesliga", "ligue1", "liga-mx", "acn", "elc",
    ])
    
    def __init__(self, bet_amount: float = 5.0, exit_after: int = 120):
        self.bet_amount = bet_amount
        self.exit_after = exit_after  # time exit après N secondes (TP/SL peuvent sortir avant)
        
        self._ws = None
        self._running = False
        self._matches: dict[str, dict] = {}
        
        # Live data
        self._goals: list[GoalRecord] = []
        self._csv_file = DATA_DIR / f"live_goals_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        # Strategy params (will be optimized)
        self.params = {
            "bet_amount": bet_amount,
            "exit_after_seconds": exit_after,
        }
        
        # Performance tracking
        self._start_time = None
        self._total_pnl = 0.0
        self._trades = 0
        self._wins = 0
        self._iteration = 0
        
        # Feedback: first message received (so user sees stream is active)
        self._first_message_received = False
        
        # Price tracker for real odds (stabilization, profit window)
        self._price_tracker = PriceTracker()
        self._http: Optional[httpx.AsyncClient] = None
        # Polymarket CLOB for live trading (when DRY_RUN=false and POLYMARKET_PRIVATE_KEY set)
        self._polymarket = PolymarketClient()
        self._live_orders: dict[str, dict] = {}  # slug -> {order_id, token_id, size, exit_at}
        
        self._token_cache: dict[str, tuple[Optional[str], Optional[str], Optional[str]]] = {}  # slug -> (token_home, token_draw, token_away)
        self._realtime_csv = DATA_DIR / f"live_prices_realtime_{datetime.now().strftime('%Y%m%d')}.csv"
        self._realtime_csv_initialized = False
        
        # Sportmonks: détection des buts (au lieu de Polymarket)
        self._sportmonks_token = (os.environ.get("SPORTMONKS_API_TOKEN") or "").strip()
        self._sportmonks_client: Optional[SportmonksClient] = None
        self._sportmonks_seen_goals: set[tuple[int, int]] = set()  # (fixture_id, event_id)
        self._sportmonks_last_score: dict[int, tuple[int, int]] = {}  # fixture_id -> (home, away)
        self._sportmonks_last_matched_count: Optional[int] = None  # pour n'afficher le nb de matchs que quand il change
        self._sportmonks_seen_score_goals: set[tuple[int, str]] = set()  # (fixture_id, "h-1-1") pour éviter doublon
        self._sportmonks_poll_interval = 0.15  # 150 ms — pas d'appel API si _matches vide
        # TheSports MQTT over WebSockets (priorité sur Sportmonks pour la détection buts)
        self._thesports_host = (os.environ.get("THESPORTS_HOST") or os.environ.get("THESPORTS_WS_URL") or "mq.thesports.com").strip()
        if self._thesports_host.startswith("wss://"):
            self._thesports_host = self._thesports_host.replace("wss://", "").split("/")[0]
        elif self._thesports_host.startswith("ws://"):
            self._thesports_host = self._thesports_host.replace("ws://", "").split("/")[0]
        try:
            self._thesports_port = int(os.environ.get("THESPORTS_PORT") or "443")
        except (TypeError, ValueError):
            self._thesports_port = 443
        self._thesports_user = (os.environ.get("THESPORTS_USER") or os.environ.get("THESPORTS_API_USER") or "").strip()
        self._thesports_secret = (os.environ.get("THESPORTS_SECRET") or os.environ.get("THESPORTS_API_SECRET") or "").strip()
        self._thesports_topic = (os.environ.get("THESPORTS_TOPIC") or "").strip()
        self._thesports_last_scores: dict[str, tuple[int, int]] = {}
        # TheSports match id -> (home_team, away_team) pour résoudre les events MQTT (rempli par API REST)
        self._thesports_id_to_teams: dict[str, tuple[str, str]] = {}
        self._thesports_api_match_list_url = (os.environ.get("THESPORTS_API_MATCH_LIST_URL") or "").strip()
        # Schedule and Results (date query) : https://www.thesports.com/fr/docs/football → BASIC DATA
        self._thesports_api_schedule_url = (os.environ.get("THESPORTS_API_SCHEDULE_URL") or "").strip()
        # Dédupe buts vus par les sources (TheSports, Sportmonks — plus Polymarket WS pour les buts)
        self._goal_seen: set = set()  # (slug, score) pour buts
        # Un seul BUY par but: (home_team, away_team, new_score)
        self._goals_traded: set[tuple[str, str, str]] = set()
        # Cache des minutes actuelles des matchs (mis à jour en background, pas de latence sur le BUY)
        self._match_current_minute: dict[str, int] = {}  # slug -> minute actuelle du match
        self._match_minute_max_staleness = 10  # Tolérance en minutes pour un but "récent"
        # Dernier set de slugs notifié (matchs suivis) pour ne notifier que quand la liste change
        self._last_notified_match_slugs: Optional[frozenset] = None
        # CSV: heure de détection (TheSports, Sportmonks); delta TheSports vs Sportmonks (écrit en async, pas sur le chemin critique)
        self._goal_detection_csv = DATA_DIR / f"goal_detection_times_{datetime.now().strftime('%Y%m%d')}.csv"
        self._goal_delta_csv = DATA_DIR / f"goal_delta_thesports_sportmonks_{datetime.now().strftime('%Y%m%d')}.csv"
        self._goal_detection_pending: dict[tuple[str, str], dict] = {}  # (slug, score) -> { thesports_ts, sportmonks_ts, ... }
        self._goal_detection_csv_initialized = False
        self._goal_delta_queue: asyncio.Queue = asyncio.Queue()
        self._goal_delta_csv_initialized = False

        # Initialize CSV
        self._init_csv()
    
    def _init_csv(self):
        """Initialize CSV file"""
        with open(self._csv_file, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                "timestamp", "match", "league", "home_team", "away_team",
                "scoring_team", "minute", "score_before", "score_after",
                "entry_odds", "exit_odds", "pnl", "cumulative_pnl"
            ])
    
    def _append_csv(self, goal: GoalRecord):
        """Append goal to CSV - entry/exit/pnl only when REAL (from PriceTracker)"""
        try:
            entry = f"{goal.entry_odds:.3f}" if goal.entry_odds is not None else ""
            exit_ = f"{goal.exit_odds:.3f}" if goal.exit_odds is not None else ""
            pnl = f"{goal.pnl:.2f}" if goal.pnl is not None else ""
            cum = f"{self._total_pnl:.2f}" if self._trades > 0 else ""
            with open(self._csv_file, "a", newline="") as f:
                writer = csv.writer(f)
                writer.writerow([
                    goal.timestamp.isoformat(),
                    goal.match,
                    goal.league,
                    goal.home_team,
                    goal.away_team,
                    goal.scoring_team,
                    goal.minute,
                    goal.score_before,
                    goal.score_after,
                    entry,
                    exit_,
                    pnl,
                    cum,
                ])
        except OSError as e:
            logger.warning(f"Could not append goal to CSV {self._csv_file}: {e}")
    
    def _ensure_goal_detection_csv(self):
        """Create goal detection times CSV with header if missing."""
        if self._goal_detection_csv_initialized:
            return
        self._goal_detection_csv_initialized = True
        if self._goal_detection_csv.exists() and self._goal_detection_csv.stat().st_size > 0:
            return
        try:
            with open(self._goal_detection_csv, "w", newline="", encoding="utf-8") as f:
                csv.writer(f).writerow([
                    "slug", "home_team", "away_team", "score_after",
                    "thesports_ts_utc", "sportmonks_ts_utc", "first_source",
                ])
        except OSError:
            pass

    def _record_goal_detection_ts(
        self,
        source: str,  # "thesports", "sportmonks"
        slug: str,
        score_str: str,
        home_team: str,
        away_team: str,
    ):
        """Enregistre l'heure de détection (TheSports / Sportmonks). CSV quand on a les 2. Delta (TheSports vs Sportmonks) en async."""
        now_iso = datetime.now(timezone.utc).isoformat()
        key = (slug, score_str)
        if key not in self._goal_detection_pending:
            self._goal_detection_pending[key] = {
                "thesports_ts_utc": "",
                "sportmonks_ts_utc": "",
                "home_team": home_team,
                "away_team": away_team,
                "first_seen_ts": now_iso,
            }
        rec = self._goal_detection_pending[key]
        if source == "thesports":
            rec["thesports_ts_utc"] = now_iso
        elif source == "sportmonks":
            rec["sportmonks_ts_utc"] = now_iso

        thesports_ts = rec.get("thesports_ts_utc") or ""
        sportmonks_ts = rec.get("sportmonks_ts_utc") or ""
        timestamps = [(thesports_ts, "TheSports"), (sportmonks_ts, "Sportmonks")]
        set_ts = [(t, name) for t, name in timestamps if t]
        if len(set_ts) >= 2:
            first = min(set_ts, key=lambda x: x[0])[1]
            self._ensure_goal_detection_csv()
            try:
                with open(self._goal_detection_csv, "a", newline="", encoding="utf-8") as f:
                    csv.writer(f).writerow([
                        slug,
                        rec.get("home_team", ""),
                        rec.get("away_team", ""),
                        score_str,
                        thesports_ts,
                        sportmonks_ts,
                        first,
                    ])
            except OSError:
                pass
            # Enqueue delta pour écriture async (pas sur le chemin critique)
            if thesports_ts and sportmonks_ts:
                try:
                    t1 = datetime.fromisoformat(thesports_ts.replace("Z", "+00:00"))
                    t2 = datetime.fromisoformat(sportmonks_ts.replace("Z", "+00:00"))
                    delta_sec = (t2 - t1).total_seconds()
                except (ValueError, TypeError):
                    delta_sec = ""
                self._goal_delta_queue.put_nowait({
                    "slug": slug,
                    "home_team": rec.get("home_team", ""),
                    "away_team": rec.get("away_team", ""),
                    "score_after": score_str,
                    "thesports_ts_utc": thesports_ts,
                    "sportmonks_ts_utc": sportmonks_ts,
                    "delta_sec": delta_sec,
                })
            del self._goal_detection_pending[key]

    def _flush_goal_detection_pending(self):
        """Écrit les buts en attente (une seule source) après 120s pour ne pas perdre de données."""
        now = datetime.now(timezone.utc)
        to_remove = []
        for key, rec in list(self._goal_detection_pending.items()):
            first_seen = rec.get("first_seen_ts") or ""
            if not first_seen:
                continue
            try:
                dt = datetime.fromisoformat(first_seen.replace("Z", "+00:00"))
                if (now - dt).total_seconds() < 120:
                    continue
            except (ValueError, TypeError):
                continue
            slug, score_str = key
            thesports_ts = rec.get("thesports_ts_utc") or ""
            sportmonks_ts = rec.get("sportmonks_ts_utc") or ""
            set_ts = [(t, name) for t, name in [(thesports_ts, "TheSports"), (sportmonks_ts, "Sportmonks")] if t]
            first = min(set_ts, key=lambda x: x[0])[1] if set_ts else "Sportmonks"
            self._ensure_goal_detection_csv()
            try:
                with open(self._goal_detection_csv, "a", newline="", encoding="utf-8") as f:
                    csv.writer(f).writerow([
                        slug,
                        rec.get("home_team", ""),
                        rec.get("away_team", ""),
                        score_str,
                        thesports_ts,
                        sportmonks_ts,
                        first,
                    ])
            except OSError:
                pass
            to_remove.append(key)
        for k in to_remove:
            self._goal_detection_pending.pop(k, None)

    def _ensure_goal_delta_csv(self):
        if self._goal_delta_csv_initialized:
            return
        self._goal_delta_csv_initialized = True
        try:
            if self._goal_delta_csv.exists() and self._goal_delta_csv.stat().st_size > 0:
                return
            with open(self._goal_delta_csv, "w", newline="", encoding="utf-8") as f:
                csv.writer(f).writerow([
                    "slug", "home_team", "away_team", "score_after",
                    "thesports_ts_utc", "sportmonks_ts_utc", "delta_sec",
                ])
        except OSError:
            pass

    async def _goal_delta_writer_loop(self):
        """Écrit les deltas TheSports/Sportmonks en async (hors chemin critique)."""
        while self._running:
            try:
                row = await asyncio.wait_for(self._goal_delta_queue.get(), timeout=5.0)
            except asyncio.TimeoutError:
                continue
            self._ensure_goal_delta_csv()
            try:
                with open(self._goal_delta_csv, "a", newline="", encoding="utf-8") as f:
                    csv.writer(f).writerow([
                        row.get("slug", ""),
                        row.get("home_team", ""),
                        row.get("away_team", ""),
                        row.get("score_after", ""),
                        row.get("thesports_ts_utc", ""),
                        row.get("sportmonks_ts_utc", ""),
                        str(row.get("delta_sec", "")),
                    ])
            except OSError as e:
                logger.debug("goal_delta_csv write: %s", e)

    async def _warm_token_cache_for_slug(
        self, slug: str, home_team: str, away_team: str, league: Optional[str] = None
    ) -> None:
        """Pre-fill token cache for a match so we don't block on find_tokens when a goal is detected."""
        if slug in self._token_cache:
            return
        try:
            tokens = await self._price_tracker.find_tokens_for_match(
                slug, home_team, away_team, league=league
            )
            self._token_cache[slug] = tokens
        except Exception as e:
            logger.debug(f"Token warm for {slug}: {e}")

    def _ensure_realtime_csv(self):
        """Create realtime CSV with header only if file missing or empty (ne pas écraser au redémarrage)."""
        if self._realtime_csv_initialized:
            return
        self._realtime_csv_initialized = True
        if self._realtime_csv.exists() and self._realtime_csv.stat().st_size > 0:
            return
        with open(self._realtime_csv, "w", newline="") as f:
            csv.writer(f).writerow([
                "timestamp_utc", "slug", "home_team", "away_team",
                "home_ask", "home_bid", "draw_ask", "draw_bid", "away_ask", "away_bid",
            ])
    
    def _append_realtime_prices(
        self,
        now: datetime,
        slug: str,
        home_team: str,
        away_team: str,
        ah: Optional[float],
        bh: Optional[float],
        ad: Optional[float],
        bd: Optional[float],
        aa: Optional[float],
        ba: Optional[float],
    ):
        """Append one row of real-time prices to the live CSV."""
        self._ensure_realtime_csv()
        try:
            with open(self._realtime_csv, "a", newline="") as f:
                csv.writer(f).writerow([
                    now.isoformat(),
                    slug,
                    home_team,
                    away_team,
                    f"{ah:.4f}" if ah is not None else "",
                    f"{bh:.4f}" if bh is not None else "",
                    f"{ad:.4f}" if ad is not None else "",
                    f"{bd:.4f}" if bd is not None else "",
                    f"{aa:.4f}" if aa is not None else "",
                    f"{ba:.4f}" if ba is not None else "",
                ])
        except OSError as e:
            logger.warning(f"Could not append to realtime CSV: {e}")
    
    async def start(self):
        """Start the live system"""
        self._running = True
        self._start_time = datetime.now(timezone.utc)
        
        await self._polymarket.initialize()
        live_mode = not settings.trading.dry_run and getattr(self._polymarket, "_clob_client", None) is not None
        clob_ok = getattr(self._polymarket, "_clob_client", None) is not None
        logger.info(f"DRY_RUN={settings.trading.dry_run} | CLOB init={clob_ok} → live_mode={live_mode}")
        
        # Set allowances au démarrage pour éviter les erreurs "not enough balance/allowance" sur SELL
        if live_mode:
            try:
                await self._polymarket.set_allowances()
                logger.info("Allowances initialisées au démarrage")
            except Exception as e:
                logger.warning(f"set_allowances au démarrage: {e}")
        
        # Notification démarrage du bot (toujours, fire-and-forget)
        bet_display = min(self.bet_amount, settings.trading.max_position_size_usdc)
        mode_msg = f"Mode: LIVE | Bet: ${bet_display}" if live_mode else "Mode: BACKTEST (DRY_RUN)"
        notify_system(f"🟢 Bot démarré\n{mode_msg}")
        
        if not live_mode:
            if settings.trading.dry_run:
                console.print("[yellow]→ Mode backtest car DRY_RUN=true dans .env. Mettre DRY_RUN=false pour trader.[/yellow]")
            elif not clob_ok:
                console.print("[yellow]→ Mode backtest car CLOB non initialisé. Vérifier POLYMARKET_PRIVATE_KEY et POLYMARKET_SMART_WALLET dans .env sur le serveur.[/yellow]")
        bet_display = min(self.bet_amount, settings.trading.max_position_size_usdc) if live_mode else self.bet_amount
        console.print(Panel.fit(
            "[bold cyan]🤖 Live System[/bold cyan]\n\n"
            + ("[red]LIVE TRADING - Ordres réels Polymarket[/red]\n\n" if live_mode else "[yellow]BACKTEST ONLY - Aucun trading réel[/yellow]\n\n")
            + f"Bet Amount: ${bet_display}\n"
            + f"Exit After: {self.exit_after}s\n"
            + f"CSV: {self._csv_file.name}\n\n"
            + "[dim]Données 100% réelles (Polymarket)[/dim]",
            title="Starting",
        ))
        if not settings.trading.dry_run and not live_mode:
            console.print("[yellow]DRY_RUN=false mais CLOB non initialisé (POLYMARKET_PRIVATE_KEY manquant?) — pas d'ordres réels[/yellow]")
        
        # Au relance: vider la liste (pas d'anciens matchs). On ne charge que via le WebSocket.
        self._matches.clear()
        self._token_cache.clear()
        self._goal_seen.clear()
        self._goals_traded.clear()
        self._write_matches_file()
        
        # Start background tasks (chacune encapsulée pour ne jamais faire crasher le processus)
        async def _run_task(name: str, coro_func):
            while self._running:
                try:
                    await coro_func()
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.exception(f"Task {name} error: {e}")
                    await asyncio.sleep(10)
        
        tasks = [
            asyncio.create_task(_run_task("collect_data", self._collect_data)),
            asyncio.create_task(_run_task("price_poll", self._live_price_poll_loop)),
            asyncio.create_task(_run_task("heartbeat", self._heartbeat_loop)),
            asyncio.create_task(_run_task("gamma_refresh", self._gamma_refresh_loop)),
            asyncio.create_task(_run_task("status", self._status_loop)),
            asyncio.create_task(_run_task("improvement", self._improvement_loop)),
            asyncio.create_task(_run_task("goal_delta_writer", self._goal_delta_writer_loop)),
        ]
        # TheSports en premier (priorité latence), puis Sportmonks
        if self._thesports_user and self._thesports_secret and self._thesports_topic:
            tasks.insert(0, asyncio.create_task(_run_task("thesports_ws", self._thesports_ws_loop)))
            if self._thesports_api_schedule_url or self._thesports_api_match_list_url:
                tasks.append(asyncio.create_task(_run_task("thesports_refresh", self._thesports_refresh_match_cache_loop)))
        if self._sportmonks_token:
            tasks.append(asyncio.create_task(_run_task("sportmonks_goals", self._sportmonks_goal_poll)))
        sources = []
        if self._thesports_user and self._thesports_secret and self._thesports_topic:
            sources.append("TheSports")
        if self._sportmonks_token:
            sources.append("Sportmonks")
        if sources:
            console.print("[dim]Détection buts: " + " + ".join(sources) + " (Polymarket WS désactivé pour buts)[/dim]")
        else:
            console.print("[yellow]THESPORTS_* ou SPORTMONKS_API_TOKEN — pas de détection buts[/yellow]")
        
        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            pass
    
    async def stop(self):
        """Stop the system"""
        self._running = False
        if self._ws:
            await self._ws.close()
        await self._price_tracker.close()
        if self._sportmonks_client:
            await self._sportmonks_client.close()
            self._sportmonks_client = None
        if self._http:
            await self._http.aclose()
            self._http = None
        await self._polymarket.close()
    
    def _team_names_match(self, a: str, b: str) -> bool:
        """Delegate to the unified team_matching module (rapidfuzz-based)."""
        return _teams_match_fn(a, b)

    def _match_is_live(self, m: dict) -> bool:
        """True si le match a un flux live (period/elapsed du WS Polymarket) et n'est pas terminé."""
        if m.get("ended_at"):
            return False
        period = (m.get("period") or "").strip().upper()
        elapsed = (m.get("elapsed") or "").strip()
        return period in ("1H", "2H", "HT") or bool(elapsed)

    def _find_slug_by_teams(self, home_team: str, away_team: str) -> Optional[str]:
        """Find Polymarket match slug from home/away team names (from _matches).
        Priorité: 1) matchs en live (period/elapsed), 2) tout match dont les équipes matchent (ex. UCL pas encore marqué live par le WS)."""
        if not (home_team or "").strip() and not (away_team or "").strip():
            return None
        # 1) D'abord les matchs en live
        for slug, m in self._matches.items():
            if not self._match_is_live(m):
                continue
            if self._team_names_match(home_team, m.get("home_team", "")) and self._team_names_match(away_team, m.get("away_team", "")):
                return slug
        # 2) Sinon tout match qui matche les équipes (ex. Galatasaray - Liverpool UCL listé par Gamma mais WS pas encore en live)
        for slug, m in self._matches.items():
            m_home = (m.get("home_team") or "").strip()
            m_away = (m.get("away_team") or "").strip()
            if not m_home or not m_away:
                continue
            if self._team_names_match(home_team, m_home) and self._team_names_match(away_team, m_away):
                return slug
        return None

    async def _try_resolve_and_add_match_from_sportmonks(self, f) -> Optional[str]:
        """
        Si le match Sportmonks n'est pas dans _matches (ex. rou1 pas listé par Gamma/WS),
        tente de le retrouver sur Gamma par slug (ex. rou1-fcc-din-2026-03-09) et l'ajoute à _matches.
        Retourne le slug si trouvé, sinon None.
        On tente toujours rou1 par noms d'équipes (Cluj, Dinamo...) — pas de filtre ligue Sportmonks.
        """
        result = await self._price_tracker.try_resolve_slug_for_teams("rou1", f.home_team, f.away_team)
        if result is None:
            return None
        slug, ev = result
        if slug in self._matches:
            return slug
        score_str = (ev.get("score") or "0-0").strip().replace("\u2013", "-").replace("\u2014", "-")
        try:
            parts = score_str.split("-")
            home_score = int(parts[0].strip()) if parts else 0
            away_score = int(parts[1].strip()) if len(parts) > 1 else 0
        except (ValueError, IndexError):
            home_score, away_score = 0, 0
        self._matches[slug] = {
            "home_team": f.home_team,
            "away_team": f.away_team,
            "home_score": home_score,
            "away_score": away_score,
            "league": "rou1",
            "period": "",
            "elapsed": "",
            "last_update": datetime.now(timezone.utc).isoformat(),
        }
        asyncio.create_task(self._warm_token_cache_for_slug(slug, f.home_team, f.away_team, "rou1"))
        logger.info(f"Match rou1 résolu via Gamma (Sportmonks): {f.home_team} - {f.away_team} -> {slug}")
        return slug

    async def _sportmonks_goal_poll(self):
        """Poll Sportmonks inplay livescores; detect new goals and call _on_goal (slug from _matches)."""
        while self._running:
            try:
                if not self._sportmonks_token:
                    await asyncio.sleep(60)
                    continue
                if not self._matches:
                    await asyncio.sleep(5)  # Pas d'appel API tant qu'on ne suit aucun match
                    continue
                if self._sportmonks_client is None:
                    self._sportmonks_client = SportmonksClient(api_token=self._sportmonks_token)
                fixtures = await self._sportmonks_client.get_inplay_livescores()
                matched = 0
                for f in fixtures:
                    if self._find_slug_by_teams(f.home_team, f.away_team):
                        matched += 1
                if matched != self._sportmonks_last_matched_count:
                    self._sportmonks_last_matched_count = matched
                    if matched > 0:
                        logger.info(f"Sportmonks: {matched} match(s) correspondent à un match Polymarket suivi")
                # Mise à jour du cache des minutes (background, pas de latence sur BUY)
                for f in fixtures:
                    slug = self._find_slug_by_teams(f.home_team, f.away_team)
                    if slug and hasattr(f, "minute") and f.minute:
                        try:
                            self._match_current_minute[slug] = int(f.minute)
                        except (ValueError, TypeError):
                            pass
                
                # 1) Détection par changement de score (réactivité immédiate dès que l’API met à jour le score)
                for f in fixtures:
                    slug = self._find_slug_by_teams(f.home_team, f.away_team)
                    if not slug:
                        slug = await self._try_resolve_and_add_match_from_sportmonks(f)
                    if not slug:
                        continue
                    had_prev = f.fixture_id in self._sportmonks_last_score
                    prev = self._sportmonks_last_score.get(f.fixture_id, (0, 0))
                    ph, pa = prev
                    nh, na = f.home_score, f.away_score
                    self._sportmonks_last_score[f.fixture_id] = (nh, na)
                    if had_prev and (nh > ph or na > pa):
                        if nh - ph == 1 and na == pa:
                            scoring_team = "home"
                        elif na - pa == 1 and nh == ph:
                            scoring_team = "away"
                        else:
                            scoring_team = "home" if (nh - ph) >= (na - pa) else "away"
                        score_key = (f.fixture_id, f"{nh}-{na}")
                        if score_key not in self._sportmonks_seen_score_goals:
                            self._sportmonks_seen_score_goals.add(score_key)
                            old_score = f"{ph}-{pa}"
                            new_score = f"{nh}-{na}"
                            ts = datetime.now(timezone.utc).strftime("%H:%M:%S.") + f"{datetime.now(timezone.utc).microsecond // 1000:03d}"
                            already = (slug, new_score) in self._goal_seen
                            console.print(f"[yellow][Sportmonks][/yellow] BUT à {ts} — {f.home_team} {new_score} {f.away_team}" + (" [dim](déjà enregistré)[/dim]" if already else ""))
                            self._record_goal_detection_ts("sportmonks", slug, new_score, f.home_team, f.away_team)
                            if already:
                                continue
                            self._goal_seen.add((slug, new_score))
                            league = (self._matches.get(slug) or {}).get("league", "") or (f.league_name or "sportmonks").strip().lower()[:20]
                            logger.info(f"But Sportmonks (score): {f.home_team} {new_score} {f.away_team} ({scoring_team}) -> slug {slug}")
                            await self._on_goal(
                                slug=slug,
                                league=league,
                                home_team=f.home_team,
                                away_team=f.away_team,
                                scoring_team=scoring_team,
                                minute="?",
                                old_score=old_score,
                                new_score=new_score,
                                source="Sportmonks",
                            )
                # 2) Détection par event (backup si l’event arrive avant la mise à jour du score)
                for f in fixtures:
                    for ev in f.events:
                        eid = ev.get("id")
                        if eid is None:
                            continue
                        key = (f.fixture_id, int(eid))
                        if key in self._sportmonks_seen_goals:
                            continue
                        self._sportmonks_seen_goals.add(key)
                        # Nouveau but (event) — ne pas refire si déjà traité par détection score
                        new_score_ev = f"{f.home_score}-{f.away_score}"
                        if (f.fixture_id, new_score_ev) in self._sportmonks_seen_score_goals:
                            continue
                        self._sportmonks_seen_score_goals.add((f.fixture_id, new_score_ev))
                        slug = self._find_slug_by_teams(f.home_team, f.away_team)
                        if not slug:
                            slug = await self._try_resolve_and_add_match_from_sportmonks(f)
                        if not slug:
                            logger.debug(f"But Sportmonks sans match Polymarket: {f.home_team} - {f.away_team}")
                            continue
                        team_id = ev.get("team_id")
                        if team_id is not None and f.home_participant_id is not None and f.away_participant_id is not None:
                            scoring_team = "home" if int(team_id) == f.home_participant_id else "away"
                        else:
                            # Fallback: déduire qui a marqué depuis le résultat de l'event
                            scoring_team = None
                            if ev.get("result"):
                                try:
                                    parts = str(ev["result"]).strip().split("-")
                                    if len(parts) == 2:
                                        ev_h, ev_a = int(parts[0]), int(parts[1])
                                        # Comparer avec le score précédent connu
                                        prev = self._sportmonks_last_score.get(f.fixture_id, (0, 0))
                                        ph, pa = prev
                                        if ev_h > ph and ev_a == pa:
                                            scoring_team = "home"
                                        elif ev_a > pa and ev_h == ph:
                                            scoring_team = "away"
                                except (ValueError, TypeError):
                                    pass
                            # Si toujours pas déterminé, déduire depuis le score actuel
                            if scoring_team is None:
                                prev = self._sportmonks_last_score.get(f.fixture_id, (0, 0))
                                ph, pa = prev
                                nh, na = f.home_score, f.away_score
                                if nh > ph and na == pa:
                                    scoring_team = "home"
                                elif na > pa and nh == ph:
                                    scoring_team = "away"
                                else:
                                    # Cas ambigu: on ne peut pas déterminer, skip cet event
                                    logger.warning(f"Sportmonks event: impossible de déterminer qui a marqué (prev={ph}-{pa}, cur={nh}-{na})")
                                    continue
                        minute = str(ev.get("minute") or "")
                        if ev.get("extra_minute"):
                            minute = f"{minute}+{ev['extra_minute']}"
                        result = (ev.get("result") or "").strip()
                        if "-" in result:
                            new_score = result
                            parts = result.split("-", 1)
                            try:
                                nh, na = int(parts[0]), int(parts[1])
                                old_h = nh - (1 if scoring_team == "home" else 0)
                                old_a = na - (1 if scoring_team == "away" else 0)
                                old_score = f"{old_h}-{old_a}"
                            except (ValueError, TypeError):
                                old_score = "0-0"
                        else:
                            new_score = f"{f.home_score}-{f.away_score}"
                            old_score = f"{f.home_score - (1 if scoring_team == 'home' else 0)}-{f.away_score - (1 if scoring_team == 'away' else 0)}"
                        ts = datetime.now(timezone.utc).strftime("%H:%M:%S.") + f"{datetime.now(timezone.utc).microsecond // 1000:03d}"
                        already = (slug, new_score) in self._goal_seen
                        console.print(f"[yellow][Sportmonks][/yellow] BUT à {ts} — {f.home_team} {new_score} {f.away_team}" + (" [dim](déjà enregistré)[/dim]" if already else ""))
                        self._record_goal_detection_ts("sportmonks", slug, new_score, f.home_team, f.away_team)
                        if already:
                            continue
                        self._goal_seen.add((slug, new_score))
                        league = (self._matches.get(slug) or {}).get("league", "") or (f.league_name or "sportmonks").strip().lower()[:20]
                        logger.info(f"But Sportmonks: {f.home_team} {new_score} {f.away_team} ({scoring_team}) -> slug {slug}")
                        await self._on_goal(
                            slug=slug,
                            league=league,
                            home_team=f.home_team,
                            away_team=f.away_team,
                            scoring_team=scoring_team,
                            minute=minute or "?",
                            old_score=old_score,
                            new_score=new_score,
                            source="Sportmonks",
                        )
                await asyncio.sleep(self._sportmonks_poll_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"Sportmonks goal poll: {e}")
                await asyncio.sleep(30)

    async def _on_thesports_goal(
        self,
        home_team: str,
        away_team: str,
        new_score: str,
        scoring_team: str,
        minute: str,
        old_score: str,
    ):
        """Callback appelé par le WebSocket TheSports quand un but est détecté. Optimisé pour latence minimale."""
        # --- CHEMIN CRITIQUE: slug lookup synchrone, pas de log avant le trade ---
        slug = self._find_slug_by_teams(home_team, away_team)
        if not slug:
            # Fallback async si pas trouvé - on lance en tâche pour ne pas bloquer
            asyncio.create_task(self._on_thesports_goal_fallback(
                home_team, away_team, new_score, scoring_team, minute, old_score
            ))
            return
        goal_key = (slug, new_score)
        if goal_key in self._goal_seen:
            return
        self._goal_seen.add(goal_key)
        league = (self._matches.get(slug) or {}).get("league", "") or "thesports"
        # Fire-and-forget: on lance _on_goal sans attendre
        asyncio.create_task(self._on_goal(
            slug=slug,
            league=league,
            home_team=home_team,
            away_team=away_team,
            scoring_team=scoring_team,
            minute=minute or "?",
            old_score=old_score,
            new_score=new_score,
            source="TheSports",
            trade_trigger="goal",
        ))
        # Logs APRÈS le create_task (non bloquant)
        now = datetime.now(timezone.utc)
        ts = now.strftime("%H:%M:%S.") + f"{now.microsecond // 1000:03d}"
        console.print(f"[green][TheSports][/green] BUT à {ts} — {home_team} {new_score} {away_team}")
        logger.info(f"But TheSports: {home_team} {new_score} {away_team} ({scoring_team}) -> slug {slug}")
        self._record_goal_detection_ts("thesports", slug, new_score, home_team, away_team)

    async def _on_thesports_goal_fallback(
        self,
        home_team: str,
        away_team: str,
        new_score: str,
        scoring_team: str,
        minute: str,
        old_score: str,
    ):
        """Fallback si slug non trouvé: résolution async puis trade."""
        slug = await self._try_resolve_and_add_match_from_sportmonks_thesports(home_team, away_team, new_score)
        if not slug:
            logger.info(
                "TheSports but: event reçu mais match non listé sur Polymarket — {} - {}",
                home_team, away_team,
            )
            return
        goal_key = (slug, new_score)
        if goal_key in self._goal_seen:
            return
        self._goal_seen.add(goal_key)
        league = (self._matches.get(slug) or {}).get("league", "") or "thesports"
        now = datetime.now(timezone.utc)
        ts = now.strftime("%H:%M:%S.") + f"{now.microsecond // 1000:03d}"
        console.print(f"[green][TheSports][/green] BUT à {ts} — {home_team} {new_score} {away_team} (fallback)")
        logger.info(f"But TheSports (fallback): {home_team} {new_score} {away_team} ({scoring_team}) -> slug {slug}")
        self._record_goal_detection_ts("thesports", slug, new_score, home_team, away_team)
        await self._on_goal(
            slug=slug,
            league=league,
            home_team=home_team,
            away_team=away_team,
            scoring_team=scoring_team,
            minute=minute or "?",
            old_score=old_score,
            new_score=new_score,
            source="TheSports",
            trade_trigger="goal",
        )

    @staticmethod
    def _teams_match(h1: str, a1: str, h2: str, a2: str) -> bool:
        """True si les paires (h1,a1) et (h2,a2) désignent le même match."""
        return _teams_match_fn(h1, h2) and _teams_match_fn(a1, a2)

    async def _try_resolve_and_add_match_from_sportmonks_thesports(
        self, home_team: str, away_team: str, score_str: str
    ) -> Optional[str]:
        """TheSports/Sportmonks (noms d'équipes) → slug Polymarket. 1) Match dans _matches (Gamma). 2) Sinon rou1."""
        home_team = (home_team or "").strip()
        away_team = (away_team or "").strip()
        if not home_team or not away_team:
            return None
        # 1) Déjà suivi via Gamma : matcher TheSports ↔ Polymarket par noms d'équipes.
        # Même critère strict (_team_names_match). Priorité: matchs en live, puis tout match qui matche (ex. UCL).
        for slug, info in self._matches.items():
            if not self._match_is_live(info):
                continue
            m_home = (info.get("home_team") or "").strip()
            m_away = (info.get("away_team") or "").strip()
            if not m_home or not m_away:
                continue
            if self._team_names_match(home_team, m_home) and self._team_names_match(away_team, m_away):
                return slug
        for slug, info in self._matches.items():
            m_home = (info.get("home_team") or "").strip()
            m_away = (info.get("away_team") or "").strip()
            if not m_home or not m_away:
                continue
            if self._team_names_match(home_team, m_home) and self._team_names_match(away_team, m_away):
                return slug
        # 2) Résolution rou1 (et autres ligues si on étend try_resolve_slug_for_teams)
        result = await self._price_tracker.try_resolve_slug_for_teams("rou1", home_team, away_team)
        if result is None:
            return None
        slug, ev = result
        if slug in self._matches:
            return slug
        try:
            parts = score_str.split("-")
            h = int(parts[0].strip()) if parts else 0
            a = int(parts[1].strip()) if len(parts) > 1 else 0
        except (ValueError, TypeError):
            h, a = 0, 0
        self._matches[slug] = {
            "home_team": home_team,
            "away_team": away_team,
            "home_score": h,
            "away_score": a,
            "league": "rou1",
            "period": "",
            "elapsed": "",
            "last_update": datetime.now(timezone.utc).isoformat(),
        }
        asyncio.create_task(self._warm_token_cache_for_slug(slug, home_team, away_team, "rou1"))
        return slug

    def _resolve_thesports_match_id(self, match_id: str) -> Optional[tuple[str, str]]:
        """Sync: retourne (home_team, away_team) pour un match_id TheSports si connu (cache rempli par API REST)."""
        return self._thesports_id_to_teams.get((match_id or "").strip())

    def _thesports_parse_matches_into_cache(self, data: dict) -> int:
        """Parse une réponse API (results/data/matches) avec home_team/away_team dans chaque item. Retourne le nb ajouté."""
        items = (
            data.get("results")
            if isinstance(data.get("results"), list)
            else data.get("data")
            if isinstance(data.get("data"), list)
            else data.get("matches")
            if isinstance(data.get("matches"), list)
            else []
        )
        n = 0
        for m in items:
            if not isinstance(m, dict):
                continue
            mid = (m.get("id") or m.get("match_id") or "").strip()
            if not mid:
                continue
            home = (m.get("home_team") or m.get("strHomeTeam") or m.get("homeTeam") or "").strip()
            away = (m.get("away_team") or m.get("strAwayTeam") or m.get("awayTeam") or "").strip()
            if home or away:
                self._thesports_id_to_teams[mid] = (home, away)
                n += 1
        return n

    def _thesports_parse_diary_into_cache(self, data: dict) -> int:
        """Parse la réponse diary (results + results_extra.team list). Format date yyyymmdd. Retourne le nb ajouté."""
        results = data.get("results")
        if not isinstance(results, list):
            return 0
        extra = data.get("results_extra") or {}
        team_list = extra.get("team") if isinstance(extra.get("team"), list) else []
        team_by_id: dict[str, str] = {}
        for t in team_list:
            if not isinstance(t, dict) or not t.get("id"):
                continue
            tid = (t.get("id") or "").strip()
            name = (t.get("name") or t.get("short_name") or t.get("en_name") or "").strip()
            if tid:
                team_by_id[tid] = name
                if tid.startswith("l"):
                    team_by_id[tid[1:]] = name
                else:
                    team_by_id["l" + tid] = name
        n = 0
        for m in results:
            if not isinstance(m, dict):
                continue
            mid = (m.get("id") or "").strip()
            if not mid:
                continue
            hid = (m.get("home_team_id") or "").strip()
            aid = (m.get("away_team_id") or "").strip()
            home = team_by_id.get(hid) or team_by_id.get("l" + hid) or ""
            away = team_by_id.get(aid) or team_by_id.get("l" + aid) or ""
            self._thesports_id_to_teams[mid] = (home, away)
            n += 1
        return n

    async def _thesports_refresh_match_cache(self):
        """Remplit _thesports_id_to_teams via Schedule and Results (date query) et/ou THESPORTS_API_MATCH_LIST_URL."""
        if not self._thesports_user or not self._thesports_secret:
            return
        if self._http is None:
            self._http = httpx.AsyncClient(timeout=15.0)
        total = 0

        # Schedule and Results - date query (doc: https://www.thesports.com/fr/docs/football → BASIC DATA)
        # Format date: yyyymmdd (ex. 20200101)
        if self._thesports_api_schedule_url:
            today = datetime.now(timezone.utc).strftime("%Y%m%d")
            tomorrow = (datetime.now(timezone.utc) + timedelta(days=1)).strftime("%Y%m%d")
            for date_str in (today, tomorrow):
                try:
                    url = self._thesports_api_schedule_url
                    sep = "&" if "?" in url else "?"
                    r = await self._http.get(
                        f"{url}{sep}user={self._thesports_user}&secret={self._thesports_secret}&date={date_str}"
                    )
                    r.raise_for_status()
                    data = r.json()
                    if data.get("results_extra") and isinstance(data.get("results"), list):
                        total += self._thesports_parse_diary_into_cache(data)
                    else:
                        total += self._thesports_parse_matches_into_cache(data)
                except Exception as e:
                    logger.debug("TheSports Schedule (date=%s): %s", date_str, e)

        # Liste de matchs (URL fixe) si configurée
        if self._thesports_api_match_list_url:
            try:
                url = self._thesports_api_match_list_url
                sep = "&" if "?" in url else "?"
                r = await self._http.get(
                    f"{url}{sep}user={self._thesports_user}&secret={self._thesports_secret}"
                )
                r.raise_for_status()
                data = r.json()
                total += self._thesports_parse_matches_into_cache(data)
            except Exception as e:
                logger.debug("TheSports API match list: %s", e)

        if total > 0:
            logger.debug("TheSports match cache: {} match(s)", len(self._thesports_id_to_teams))

    async def _thesports_refresh_match_cache_loop(self):
        """Rafraîchit le cache match_id -> (home_team, away_team) toutes les 2 min si une URL API est définie."""
        while self._running:
            await self._thesports_refresh_match_cache()
            for _ in range(120):
                if not self._running:
                    return
                await asyncio.sleep(1)

    async def _thesports_ws_loop(self):
        """WebSocket TheSports — détection buts en priorité (avant Sportmonks)."""
        if not self._thesports_user or not self._thesports_secret or not self._thesports_topic:
            await asyncio.sleep(3600)
            return
        if self._thesports_api_schedule_url or self._thesports_api_match_list_url:
            await self._thesports_refresh_match_cache()
        await run_thesports_ws_loop(
            host=self._thesports_host,
            port=self._thesports_port,
            user=self._thesports_user,
            secret=self._thesports_secret,
            topic=self._thesports_topic,
            on_goal=self._on_thesports_goal,
            running=lambda: self._running,
            last_scores=self._thesports_last_scores,
            resolve_match_id=self._resolve_thesports_match_id,
        )

    def _event_has_started(self, ev: dict) -> bool:
        """True seulement si l'event a commencé et n'est pas encore terminé (Gamma = matchs en cours uniquement)."""
        now = datetime.now(timezone.utc)
        start_s = ev.get("startDate") or ev.get("start_date")
        end_s = ev.get("endDate") or ev.get("end_date")
        try:
            # Pas encore commencé
            if start_s:
                start_dt = datetime.fromisoformat(start_s.replace("Z", "+00:00"))
                if start_dt.tzinfo is None:
                    start_dt = start_dt.replace(tzinfo=timezone.utc)
                if start_dt > now:
                    return False  # pas encore commencé
            # Déjà terminé
            if end_s:
                end_dt = datetime.fromisoformat(end_s.replace("Z", "+00:00"))
                if end_dt.tzinfo is None:
                    end_dt = end_dt.replace(tzinfo=timezone.utc)
                if end_dt < now:
                    return False
        except (ValueError, TypeError):
            return False
        return True

    def _event_has_ended(self, ev: dict) -> bool:
        """True si l'event est terminé (endDate dans le passé)."""
        end_s = ev.get("endDate") or ev.get("end_date")
        if not end_s:
            return False
        try:
            now = datetime.now(timezone.utc)
            end_dt = datetime.fromisoformat(end_s.replace("Z", "+00:00"))
            if end_dt.tzinfo is None:
                end_dt = end_dt.replace(tzinfo=timezone.utc)
            return end_dt < now
        except (ValueError, TypeError):
            return False

    async def _fetch_live_matches_from_gamma(self) -> int:
        """1) Appel API Gamma: ajoute tous les matchs soccer en direct. 2) Retire les matchs terminés / stale."""
        added = 0
        try:
            # 1) Récupérer tous les matchs en live via l'API Gamma (pas seulement le WebSocket)
            events = await self._price_tracker.get_live_soccer_events_from_gamma(
                league_slugs=set(self.SOCCER_LEAGUES),
            )
            now_iso = datetime.now(timezone.utc).isoformat()
            for ev in events:
                slug = (ev.get("slug") or "").strip()
                if not slug or slug in self._matches or "-more-markets" in slug.lower():
                    continue
                home_team = ev.get("home_team", "")
                away_team = ev.get("away_team", "")
                league = (ev.get("league") or "").strip()
                score_str = (ev.get("score") or "0-0").strip().replace("\u2013", "-").replace("\u2014", "-")
                try:
                    parts = score_str.split("-")
                    home_score = int(parts[0].strip()) if parts else 0
                    away_score = int(parts[1].strip()) if len(parts) > 1 else 0
                except (ValueError, IndexError):
                    home_score, away_score = 0, 0
                self._matches[slug] = {
                    "home_team": home_team,
                    "away_team": away_team,
                    "home_score": home_score,
                    "away_score": away_score,
                    "league": league,
                    "period": "",
                    "elapsed": "",
                    "last_update": now_iso,
                }
                added += 1
                asyncio.create_task(self._warm_token_cache_for_slug(slug, home_team, away_team, league or None))
                logger.info(f"Match ajouté via API Gamma: {home_team} - {away_team} ({league})")
            if added > 0:
                self._write_matches_file()
        except Exception as e:
            logger.debug(f"Gamma fetch live: {e}")

        # 2) Retirer les matchs terminés (closed/endDate) ou sans mise à jour depuis > 45 min
        if self._http is None:
            self._http = httpx.AsyncClient(timeout=15.0)
        finished_slugs: set[str] = set()
        try:
            for tag in ("sports", "soccer"):
                r_closed = await self._http.get(
                    f"{self.GAMMA_URL}/events",
                    params={"tag_slug": tag, "closed": "true", "limit": 100},
                )
                if r_closed.status_code != 200:
                    continue
                closed_events = r_closed.json()
                if isinstance(closed_events, list):
                    for ev in closed_events:
                        slug = (ev.get("slug") or "").strip()
                        if slug and self._event_has_ended(ev):
                            finished_slugs.add(slug)
                break
            for slug in finished_slugs:
                if slug in self._matches:
                    del self._matches[slug]
                    self._token_cache.pop(slug, None)
            # Retirer les "more-markets" (doublons)
            for slug in list(self._matches.keys()):
                if "-more-markets" in slug.lower():
                    del self._matches[slug]
                    self._token_cache.pop(slug, None)
                    finished_slugs.add(slug)
            now = datetime.now(timezone.utc)
            stale = []
            for slug, m in list(self._matches.items()):
                last = m.get("last_update", "")
                if not last:
                    continue
                try:
                    dt = datetime.fromisoformat(last.replace("Z", "+00:00"))
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    if (now - dt).total_seconds() > 45 * 60:
                        stale.append(slug)
                except (ValueError, TypeError):
                    pass
            for slug in stale:
                del self._matches[slug]
                self._token_cache.pop(slug, None)
            if finished_slugs or stale:
                self._write_matches_file()
        except Exception as e:
            logger.debug(f"Gamma fetch cleanup: {e}")
        return added
    
    async def _collect_data(self):
        """Collect live data from Polymarket - reconnecte automatiquement si déconnexion."""
        while self._running:
            try:
                async with websockets.connect(
                    self.WS_URL,
                    ping_interval=None,
                    ping_timeout=None,
                    close_timeout=5,
                ) as ws:
                    self._ws = ws
                    console.print("[green]✓ Connected to Polymarket[/green]")
                    console.print("[dim]En attente du flux (matchs soccer)...[/dim]")
                    async for message in ws:
                        if not self._running:
                            break
                        await self._handle_message(message)
            except websockets.ConnectionClosed as e:
                if self._running:
                    logger.warning(f"WebSocket déconnecté ({e.code}), reconnexion dans 5s...")
                    console.print("[yellow]⚠ Déconnecté, reconnexion dans 5s...[/yellow]")
                    await asyncio.sleep(5)
            except (OSError, ConnectionError) as e:
                if self._running:
                    logger.warning(f"Réseau: {e}, reconnexion dans 10s...")
                    await asyncio.sleep(10)
            except Exception as e:
                if self._running:
                    logger.exception(f"WebSocket error: {e}")
                    await asyncio.sleep(10)
    
    async def _handle_message(self, message: str):
        """Handle WebSocket message"""
        if message == "ping":
            await self._ws.send("pong")
            return
        
        try:
            data = json.loads(message)
        except json.JSONDecodeError:
            return
        
        # Feedback: flux actif dès le premier message reçu
        if not self._first_message_received:
            self._first_message_received = True
            console.print("[green]✓ Flux Polymarket actif[/green]")
        
        # Soccer = leagueAbbreviation dans SOCCER_LEAGUES OU eventState.type == "soccer"
        event_state = data.get("eventState") or {}
        sport_type = (event_state.get("type") or data.get("type") or "").strip().lower()
        slug_from_msg = (data.get("slug") or "").strip()
        game_id = data.get("gameId")
        slug = slug_from_msg or (str(game_id) if game_id is not None else "")
        
        league = (data.get("leagueAbbreviation") or data.get("league") or "").strip().lower()
        # Fallback: extraire la ligue du slug (ex. rou1-fcc-din-2026-03-09 -> rou1) pour FR Cluj - Dinamo București etc.
        if not league and slug and "-" in slug:
            league = slug.split("-")[0].strip().lower()
        is_soccer = (league in self.SOCCER_LEAGUES) or (sport_type == "soccer")
        if not is_soccer:
            return
        if not league and sport_type == "soccer":
            league = "soccer"
        
        if not slug:
            return
        
        # Ne garder que les matchs en cours (pas à venir, pas terminés)
        event_state = data.get("eventState") or {}
        status = (data.get("status") or event_state.get("status") or "").strip().lower()
        live = data.get("live", event_state.get("live", False))
        ended = data.get("ended", event_state.get("ended", False))
        period = (data.get("period") or event_state.get("period") or "").strip().upper()
        elapsed = (data.get("elapsed") or event_state.get("elapsed") or "")
        elapsed_str = str(elapsed).strip() if elapsed is not None else ""
        if ended:
            # Garder le match 5 min après "ended" pour continuer à enregistrer les prix (realtime CSV)
            if slug in self._matches:
                self._matches[slug]["ended_at"] = datetime.now(timezone.utc).isoformat()
                logger.info(f"Match terminé (ended): {slug}, on garde 5 min pour enregistrement prix")
            if game_id is not None and str(game_id) in self._matches and str(game_id) != slug:
                self._matches[str(game_id)]["ended_at"] = datetime.now(timezone.utc).isoformat()
            return
        # Score: top-level ou dans eventState; normaliser (espaces, tirets Unicode) — on en a besoin pour in_progress_score
        score_str = (data.get("score") or event_state.get("score") or "0-0").strip()
        score_str = score_str.replace("\u2013", "-").replace("\u2014", "-").replace(" ", "")
        # Accepter si en cours: live OU status inprogress OU period (1H/2H/HT) / elapsed OU score ≠ 0-0 (match a commencé)
        in_progress_status = status in ("inprogress", "in progress", "live")
        in_progress_period = period in ("1H", "2H", "HT") or bool(elapsed_str)
        try:
            sp = score_str.split("-")
            in_progress_score = (len(sp) >= 2 and (int(sp[0].strip() or 0) + int(sp[1].strip() or 0) > 0))
        except (ValueError, IndexError):
            in_progress_score = False
        if not live and not in_progress_status and not in_progress_period and not in_progress_score:
            return
        
        # (score_str déjà normalisé ci-dessus)
        try:
            parts = score_str.split("-")
            home_score = int(parts[0].strip()) if parts else 0
            away_score = int(parts[1].strip()) if len(parts) > 1 else 0
        except (ValueError, IndexError):
            return
        
        home_team = data.get("homeTeam", "")
        away_team = data.get("awayTeam", "")
        period = data.get("period") or event_state.get("period", "")
        elapsed = (data.get("elapsed") or event_state.get("elapsed") or "").strip()
        # Affichage brut: period + elapsed (ex. "2H 46"), pas d'ajout de temps selon la période
        minute_display = f"{period} {elapsed}".strip() if elapsed else period
        
        # Ancien état: sous slug ou sous gameId (au cas où la clé change entre messages)
        old_state = self._matches.get(slug)
        if old_state is None and game_id is not None:
            old_state = self._matches.get(str(game_id))
            if old_state is not None:
                self._matches[slug] = old_state
                del self._matches[str(game_id)]
        
        # Ne pas écraser avec un message plus ancien (score total qui baisse)
        cur = self._matches.get(slug)
        if cur:
            cur_total = cur.get("home_score", 0) + cur.get("away_score", 0)
            new_total = home_score + away_score
            if new_total < cur_total:
                return
            # But: on ne déclenche plus _on_goal depuis Polymarket WS (TheSports + Sportmonks uniquement)
            # On met quand même à jour l'état du match ci-dessous.
        is_new = slug not in self._matches
        
        # Mise à jour état du match (period + elapsed pour le monitor)
        self._matches[slug] = {
            "home_team": home_team,
            "away_team": away_team,
            "home_score": home_score,
            "away_score": away_score,
            "league": league,
            "period": period,
            "elapsed": elapsed,
            "last_update": datetime.now(timezone.utc).isoformat(),
        }
        
        # Mise à jour du cache des minutes pour vérification fraîcheur des buts
        if elapsed:
            try:
                min_val = int(str(elapsed).split("+")[0].strip())
                if period == "2H":
                    min_val += 45
                self._match_current_minute[slug] = min_val
            except (ValueError, TypeError):
                pass
        
        if is_new:
            asyncio.create_task(self._warm_token_cache_for_slug(slug, home_team, away_team, league or None))
            console.print(f"[dim]Match suivi: {home_team} - {away_team} ({league}) {score_str} {minute_display or period}[/dim]")
        
        # Write matches for monitor (throttled in _write_matches_file)
        self._write_matches_file()
    
    def _write_matches_file(self):
        """Write current soccer matches to JSON for monitor - all matches Polymarket sends us"""
        try:
            if not hasattr(self, "_last_matches_write") or (datetime.now(timezone.utc) - self._last_matches_write).total_seconds() > 10:
                self._last_matches_write = datetime.now(timezone.utc)
                out = {
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                    "matches_count": len(self._matches),
                    "leagues": list(set(m.get("league", "") for m in self._matches.values())),
                    "matches": [
                        {
                            "slug": slug,
                            "home": m.get("home_team", ""),
                            "away": m.get("away_team", ""),
                            "score": f"{m.get('home_score', 0)}-{m.get('away_score', 0)}",
                            "league": m.get("league", ""),
                            "period": m.get("period", ""),
                            "elapsed": m.get("elapsed", ""),
                        }
                        for slug, m in self._matches.items()
                    ],
                }
                (DATA_DIR / "current_matches.json").write_text(json.dumps(out, indent=2))
        except OSError:
            pass
    
    async def _on_goal(self, slug: str, league: str, home_team: str, away_team: str,
                       scoring_team: str, minute: str, old_score: str, new_score: str,
                       source: str = "Sportmonks",
                       force_bet_draw: Optional[bool] = None,
                       trade_key_override: Optional[tuple[str, str, str]] = None,
                       trade_trigger: Optional[str] = None):
        """Handle a detected goal. OPTIMISÉ LATENCE: BUY immédiat, logs après.
        force_bet_draw: si True, parier sur le nul; si False, ignorer nul; si None, logique score.
        trade_key_override: si fourni, utilisé pour dédup.
        """
        # --- CHEMIN CRITIQUE: calcul synchrone, BUY en premier ---
        token_home, token_draw, token_away = self._token_cache.get(slug, (None, None, None))
        bet_on_draw = False
        skip_bet = False
        skip_reason = ""
        h, a = 0, 0
        
        # Vérification fraîcheur: le but ne doit pas être trop ancien par rapport à la minute actuelle du match
        goal_minute = 0
        try:
            goal_minute = int(str(minute).replace("'", "").strip().split("+")[0])
        except (ValueError, TypeError):
            pass
        cached_minute = self._match_current_minute.get(slug, 0)
        if cached_minute > 0 and goal_minute > 0:
            staleness = cached_minute - goal_minute
            if staleness > self._match_minute_max_staleness:
                skip_bet = True
                skip_reason = f"but obsolète (minute {goal_minute}, match à {cached_minute})"
                logger.warning(f"BUT OBSOLÈTE détecté: {home_team} vs {away_team} - minute but={goal_minute}, minute match actuelle={cached_minute} (staleness={staleness})")
        
        # STRATÉGIE:
        # - Score devient NUL (égalisation) → parier sur le NUL (sa cote monte)
        # - L'équipe qui marque MÈNE → parier sur ELLE (momentum, sa cote monte)
        # - L'équipe qui marque ne mène pas → skip
        bet_team = scoring_team  # L'équipe qui vient de marquer
        bet_on_draw = False
        
        try:
            parts = new_score.strip().split("-")
            if len(parts) == 2:
                h, a = int(parts[0].strip()), int(parts[1].strip())
                
                # Score nul (égalisation) → parier sur le NUL
                if not skip_bet and h == a and h > 0:
                    bet_on_draw = True
                
                # Skip si écart >= 2 (match probablement plié, cote trop basse)
                if not skip_bet and not bet_on_draw and abs(h - a) >= 2:
                    skip_bet = True
                    skip_reason = f"écart >= 2 ({new_score})"
                
                # Skip si l'équipe qui marque ne mène pas (toujours perdante)
                if not skip_bet and not bet_on_draw:
                    scorer_goals = h if scoring_team == "home" else a
                    opponent_goals = a if scoring_team == "home" else h
                    if scorer_goals < opponent_goals:
                        skip_bet = True
                        skip_reason = f"équipe qui marque toujours perdante ({new_score})"
                
                # Filtre minute minimum
                if not skip_bet and goal_minute > 0:
                    min_minute = getattr(settings.trading, "min_minute_to_bet", 0)
                    if min_minute > 0 and goal_minute < min_minute:
                        skip_bet = True
                        skip_reason = f"minute {goal_minute} < min {min_minute}"
                        
        except (ValueError, TypeError):
            pass
        
        # Token: NUL si égalisation, sinon l'équipe qui marque (momentum)
        if bet_on_draw and token_draw:
            token_id = token_draw
            bet_label = "nul"
        else:
            token_id = token_home if bet_team == "home" else token_away
            bet_label = bet_team
        trade_key = trade_key_override or (home_team.strip(), away_team.strip(), new_score.strip())
        already_traded = trade_key in self._goals_traded
        # Conditions de trade (check rapide en mémoire)
        clob_ready = not settings.trading.dry_run and getattr(self._polymarket, "_clob_client", None)
        match_live = self._match_is_live(self._matches.get(slug) or {}) if slug else False
        uptime_ok = self._start_time and (datetime.now(timezone.utc) - self._start_time).total_seconds() >= 10
        want_trade = clob_ready and not already_traded and not skip_bet and uptime_ok and match_live
        # --- BUY IMMÉDIAT (avant tout log) ---
        if want_trade:
            self._goals_traded.add(trade_key)
            asyncio.create_task(self._place_live_entry_and_schedule_exit(
                slug, token_id, home_team, away_team, bet_label,
                trigger=trade_trigger or "goal",
                league=league,
                scoring_team=scoring_team,
                bet_on_draw=bet_on_draw,
                score=new_score,
                minute=goal_minute,
            ))
        # --- APRÈS BUY: logs, record, display (non bloquant) ---
        goal_ts_utc = datetime.now(timezone.utc)
        goal = GoalRecord(
            timestamp=goal_ts_utc,
            match=slug,
            league=league,
            home_team=home_team,
            away_team=away_team,
            scoring_team=scoring_team,
            minute=minute,
            score_before=old_score,
            score_after=new_score,
        )
        self._goals.append(goal)
        scorer = home_team if scoring_team == "home" else away_team
        bet_team_name = home_team if bet_team == "home" else away_team
        console.print(f"\n[bold green]⚽ BUT DÉTECTÉ[/bold green] [dim]({source})[/dim]")
        console.print(f"   {home_team} {new_score} {away_team} ({scorer} marque) — [cyan]{minute}[/cyan]")
        # Notif but sur match suivi (fire-and-forget)
        notify_goal(f"{home_team} vs {away_team}", new_score, minute=str(minute), source=source)
        if want_trade:
            if bet_on_draw:
                console.print(f"   [dim]→ ÉGALISATION: pari sur [bold]NUL[/bold][/dim]")
            else:
                console.print(f"   [dim]→ MOMENTUM: pari sur [bold]{bet_team_name}[/bold] ({bet_team})[/dim]")
        if skip_bet and skip_reason:
            console.print(f"   [dim]→ Skip: {skip_reason}[/dim]")
            logger.info(f"But ignoré ({skip_reason}): {home_team} {new_score} {away_team}")
            # Logging structuré + notification (fire-and-forget, pas de latence)
            trade_logger.log_skip(slug, home_team, away_team, new_score, goal_minute, skip_reason)
            notify_trade("SKIP", f"{home_team} vs {away_team}", "", new_score, 0, reason=skip_reason)
        elif not match_live:
            logger.info("Pas de pari: match pas encore en live — {}", slug)
        if already_traded:
            logger.info(f"But déjà tradé (autre source) — {home_team} {new_score} {away_team}")
        if token_id is None and want_trade:
            logger.warning(f"Token cache miss pour {slug} — résolution async")
        asyncio.create_task(self._track_prices_after_goal(
            goal, slug, home_team, away_team, scoring_team, league=league, token_id=token_id
        ))

    async def _place_live_entry_and_schedule_exit(
        self, slug: str, token_id: Optional[str], home_team: str, away_team: str, bet_label: str,
        trigger: str = "goal",
        *,
        league: Optional[str] = None,
        scoring_team: Optional[str] = None,
        bet_on_draw: bool = False,
        score: str = "",
        minute: int = 0,
    ):
        """Place un market order BUY (FOK). OPTIMISÉ LATENCE. Si token_id is None, résolution async."""
        import time
        t_start = time.perf_counter()
        # Check rapide en mémoire (pas de latence)
        if slug and not self._match_is_live(self._matches.get(slug) or {}):
            logger.info("BUY annulé: match pas en live — {}", slug)
            return
        # Résolution token si cache miss (ajoute latence mais inévitable)
        if token_id is None:
            m = self._matches.get(slug) or {}
            resolve_home = (m.get("home_team") or "").strip() or home_team
            resolve_away = (m.get("away_team") or "").strip() or away_team
            token_home, token_draw, token_away = await self._price_tracker.find_tokens_for_match(
                slug, resolve_home, resolve_away, league=league or ""
            )
            self._token_cache[slug] = (token_home, token_draw, token_away)
            # MOMENTUM: on parie sur l'équipe qui marque (bet_label contient déjà la bonne équipe)
            token_id = token_home if bet_label == "home" else (token_away if bet_label == "away" else token_draw)
            if not token_id:
                logger.warning("BUY annulé: pas de token pour {}", slug)
                return
        
        # --- CHECK LIQUIDITÉ ---
        min_liquidity = getattr(settings.trading, "min_liquidity_usd", 50.0)
        if min_liquidity > 0:
            try:
                bid_price = await self._polymarket.get_best_bid(token_id)
                if bid_price is None or bid_price <= 0:
                    logger.warning(f"BUY annulé: pas de bid pour {slug}")
                    console.print(f"[yellow]BUY annulé: pas de liquidité — {bet_label}[/yellow]")
                    return
            except Exception as e:
                logger.debug(f"get_best_bid: {e}")
        
        # --- ORDRE MARKET BUY (chemin critique) ---
        size_usd = min(self.bet_amount, settings.trading.max_position_size_usdc)
        result = await self._polymarket.place_market_order("BUY", token_id, size_usd)
        order_id = (result or {}).get("orderID") or (result or {}).get("order_id")
        
        # --- VÉRIFICATION SI ÉCHEC APPARENT ---
        async def check_actual_position() -> Optional[float]:
            """Vérifie si on a une position malgré un échec apparent."""
            await asyncio.sleep(1.0)
            try:
                pos = await self._polymarket.get_position_size(token_id)
                return pos if pos and pos >= 0.5 else None
            except Exception:
                return None
        
        if not order_id:
            console.print("[yellow]BUY FOK timeout, vérification position...[/yellow]")
            actual_pos = await check_actual_position()
            if actual_pos:
                console.print(f"[green]Position trouvée! {actual_pos:.1f} shares — {bet_label}[/green]")
                self._live_orders[slug] = {
                    "order_id": "recovered",
                    "token_id": token_id,
                    "size_shares": int(round(actual_pos)),
                    "entry_price": 0.55,  # Fallback
                    "trigger": trigger,
                }
                asyncio.create_task(self._exit_live_trade_tp_sl_loop(slug))
                return
            console.print("[red]Échec BUY confirmé[/red]")
            return
        
        # FOK: poll rapide (50ms) pour le fill
        fill_info = await self._polymarket.get_order_fill_info(order_id, timeout_seconds=2.0)
        if not fill_info:
            console.print(f"[yellow]BUY FOK timeout, vérification position...[/yellow]")
            actual_pos = await check_actual_position()
            if actual_pos:
                console.print(f"[green]Position trouvée! {actual_pos:.1f} shares — {bet_label}[/green]")
                self._live_orders[slug] = {
                    "order_id": order_id,
                    "token_id": token_id,
                    "size_shares": int(round(actual_pos)),
                    "entry_price": 0.55,  # Fallback
                    "trigger": trigger,
                }
                asyncio.create_task(self._exit_live_trade_tp_sl_loop(slug))
                return
            logger.warning(f"BUY FOK non exécuté — {slug}")
            console.print(f"[red]BUY non exécuté (FOK kill) — {bet_label}[/red]")
            return
        
        size_shares, entry_price = fill_info
        size_shares = max(1, int(round(size_shares)))
        latency_ms = (time.perf_counter() - t_start) * 1000
        
        # Déterminer la stratégie pour le logging
        strategy = "draw" if bet_on_draw else "momentum"
        bet_on = "draw" if bet_on_draw else bet_label
        
        self._live_orders[slug] = {
            "order_id": order_id,
            "token_id": token_id,
            "size_shares": size_shares,
            "entry_price": entry_price,
            "trigger": trigger,
            "home_team": home_team,
            "away_team": away_team,
            "score": score,
            "minute": minute,
            "strategy": strategy,
            "bet_on": bet_on,
        }
        console.print(f"[green]BUY OK — {size_shares} @ {entry_price:.2f} — {bet_label} ({latency_ms:.0f}ms)[/green]")
        
        # Logging structuré + notification (fire-and-forget)
        amount_usd = size_shares * entry_price
        trade_logger.log_entry(
            slug=slug, home_team=home_team, away_team=away_team, score=score,
            minute=minute, token_id=token_id, bet_on=bet_on, strategy=strategy,
            amount_usd=amount_usd, shares=size_shares, entry_price=entry_price,
            order_id=order_id, latency_ms=latency_ms,
        )
        notify_trade(
            "BUY", f"{home_team} vs {away_team}", bet_label, score,
            amount_usd=amount_usd, shares=size_shares, price=entry_price,
        )
        
        asyncio.create_task(self._exit_live_trade_tp_sl_loop(slug))

    async def _do_sell_position(self, slug: str, order_info: dict, reason: str) -> None:
        """Exécute le SELL (market) pour la position slug avec RETRY si position restante."""
        token_id = order_info["token_id"]
        size_shares = order_info["size_shares"]
        MAX_RETRIES = 2
        DUST_THRESHOLD = 0.5
        
        try:
            actual_shares = await self._polymarket.get_position_size(token_id)
        except Exception as e:
            logger.warning(f"get_position_size [{slug}]: {e}, utilisation size_shares={size_shares}")
            actual_shares = 0.0
        sell_size = max(0.0, round(actual_shares, 2))
        if sell_size < DUST_THRESHOLD:
            logger.info(f"Exiting [{slug}]: position=0 (actual_balance={actual_shares:.2f}), pas de SELL — {reason}")
            console.print(f"[dim]Position 0, pas de SELL ({reason}) — {slug}[/dim]")
            return
        
        for attempt in range(MAX_RETRIES + 1):
            sell_size = max(1.0, sell_size)
            if attempt == 0:
                logger.info(f"Exiting [{slug}]: SELL {sell_size} shares (actual_balance={actual_shares:.2f}) — {reason}")
                console.print(f"[cyan]Envoi SELL (market) {sell_size} shares — {reason} — {slug}[/cyan]")
            else:
                console.print(f"[yellow]Retry SELL #{attempt} — {sell_size:.1f} shares[/yellow]")
            
            try:
                result = await self._polymarket.place_sell_with_fallback(token_id, sell_size)
            except Exception as e:
                logger.exception(f"place_sell_with_fallback [{slug}]: {e}")
                result = None
            
            # Vérification position restante
            await asyncio.sleep(1.0)
            try:
                remaining = await self._polymarket.get_position_size(token_id)
            except Exception as e:
                logger.debug(f"Vérif position [{slug}]: {e}")
                remaining = 0.0
            
            if remaining <= DUST_THRESHOLD:
                logger.info(f"SELL OK [{slug}] — position restante={remaining:.2f}")
                console.print(f"[green]SELL exécuté — {reason} — {slug}[/green]")
                
                # Calcul P&L et logging (fire-and-forget)
                entry_price = order_info.get("entry_price", 0)
                try:
                    exit_price = await self._polymarket.get_bid_price(token_id)
                except Exception:
                    exit_price = entry_price
                
                pnl_pct = ((exit_price - entry_price) / entry_price * 100) if entry_price > 0 else 0
                pnl_usd = (exit_price - entry_price) * size_shares
                
                # Map reason to exit_reason code
                exit_reason_map = {"TP": "tp", "SL": "sl", "TIMEOUT": "timeout"}
                exit_code = exit_reason_map.get(reason.upper().split()[0], "manual")
                
                trade_logger.log_exit(slug, exit_price, pnl_usd, pnl_pct, exit_code)
                notify_trade(
                    reason.upper().split()[0] if reason else "SELL",
                    f"{order_info.get('home_team', '')} vs {order_info.get('away_team', '')}",
                    order_info.get("bet_on", ""),
                    order_info.get("score", ""),
                    amount_usd=pnl_usd,
                    shares=size_shares,
                    price=exit_price,
                    pnl_pct=pnl_pct,
                )
                return
            
            if attempt < MAX_RETRIES:
                logger.warning(f"Position restante {remaining:.2f}, retry SELL...")
                console.print(f"[yellow]Position restante {remaining:.2f}, retry...[/yellow]")
                sell_size = remaining
            else:
                logger.warning(f"SELL ÉCHEC final [{slug}] position restante={remaining:.2f}")
                console.print(f"[red]SELL échec — position restante {remaining:.2f} — {slug}[/red]")
                match_str = f"{order_info.get('home_team', '')} vs {order_info.get('away_team', '')}"
                notify_sell_failed(match_str, remaining, slug)

    async def _exit_live_trade_tp_sl_loop(self, slug: str):
        """Monitor position: exit on Take Profit (+%), Stop Loss (-%), or after exit_after seconds. Reads from _live_orders[slug]."""
        tp_pct = getattr(settings.trading, "take_profit_pct", 3.0)
        sl_pct = getattr(settings.trading, "stop_loss_pct", -15.0)
        poll_interval = 0.5  # Vérif TP/SL toutes les 0.5 s (2×/s)
        # Court délai pour que la position soit visible on-chain (fill déjà confirmé avant d'entrer ici)
        wait_position_sec = 5.0
        start = datetime.now(timezone.utc)
        poll_count = 0
        try:
            order_info = self._live_orders.get(slug)
            if not order_info:
                logger.debug(f"TP/SL [{slug}] order_info absent, sortie boucle")
                return
            token_id = order_info["token_id"]
            for _ in range(int(wait_position_sec / 2)):
                await asyncio.sleep(2.0)
                try:
                    pos = await self._polymarket.get_position_size(token_id)
                    if pos and pos >= 0.5:
                        break
                except Exception:
                    pass
            else:
                try:
                    pos = await self._polymarket.get_position_size(token_id)
                except Exception:
                    pos = 0.0
                if not pos or pos < 0.5:
                    self._live_orders.pop(slug, None)
                    logger.warning(f"TP/SL [{slug}]: position toujours 0 après {wait_position_sec:.0f}s (fill avait été confirmé)")
                    console.print(f"[dim]Position non visible pour {slug}, pas de TP/SL[/dim]")
                    return
            reason = "time_exit"  # défaut si on sort par time
            while True:
                await asyncio.sleep(poll_interval)
                poll_count += 1
                order_info = self._live_orders.get(slug)
                if not order_info:
                    logger.debug(f"TP/SL [{slug}] order_info absent, sortie boucle")
                    return
                token_id = order_info["token_id"]
                size_shares = order_info["size_shares"]
                entry_price = order_info.get("entry_price") or 0.55
                elapsed = (datetime.now(timezone.utc) - start).total_seconds()
                exit_timeout = getattr(settings.trading, "exit_after_seconds", self.exit_after)
                # Time exit en premier : garanti de sortir après exit_timeout s même si API bid échoue en live
                if elapsed >= exit_timeout:
                    reason = "time_exit"
                    logger.info(f"TP/SL [{slug}] time_exit après {elapsed:.0f}s")
                    break
                try:
                    bid = await self._polymarket.get_bid_price(token_id)
                except Exception as e:
                    logger.debug(f"TP/SL [{slug}] get_bid_price: {e}, on continue (time_exit à {exit_timeout}s)")
                    continue
                # Log fichier seulement toutes les ~60 s (pas la vérif : on check bien toutes les 0.5 s)
                if poll_count % 120 == 1 and poll_count > 1:
                    logger.info(f"TP/SL [{slug}] bid={bid} entry={entry_price:.3f} elapsed={elapsed:.0f}s")
                if bid is None or bid <= 0:
                    continue
                return_pct = (bid / entry_price - 1) * 100
                if return_pct >= tp_pct:
                    reason = "take_profit"
                    logger.info(f"TP/SL [{slug}] take_profit return_pct={return_pct:.1f}% >= {tp_pct}%")
                    break
                if return_pct <= sl_pct:
                    reason = "stop_loss"
                    logger.info(f"TP/SL [{slug}] stop_loss return_pct={return_pct:.1f}% <= {sl_pct}%")
                    break
            if slug in self._live_orders:
                order_info = self._live_orders.pop(slug)
                await self._do_sell_position(slug, order_info, reason)
            else:
                return
        except Exception as e:
            logger.exception(f"Exit live trade [{slug}]: {e}")
            if slug in self._live_orders:
                del self._live_orders[slug]
    
    async def _track_prices_after_goal(
        self,
        goal: GoalRecord,
        slug: str,
        home_team: str,
        away_team: str,
        scoring_team: str,
        league: str = "",
        token_id: Optional[str] = None,
    ):
        """Track real Polymarket prices for 120s - update goal with REAL PnL when done. Si token_id fourni (cache), on le réutilise."""
        try:
            analysis = await self._price_tracker.track_prices_after_goal(
                slug=slug,
                home_team=home_team,
                away_team=away_team,
                scoring_team=scoring_team,
                duration_seconds=120,
                league=league or None,
                token_id=token_id,
            )
            # PnL réaliste: entrée = ask (achat), sortie = bid (vente) si dispo; sinon fallback price 0s/60s
            entry = getattr(analysis, "entry_ask_0s", None) or analysis.price_at_0s
            exit_ = getattr(analysis, "exit_bid_60s", None) or analysis.price_at_60s
            if analysis.market_found and analysis.samples and entry and exit_ and entry > 0:
                # REAL PnL: (exit_bid/entry_ask - 1) * bet - fees - slippage
                gross = self.bet_amount * (exit_ / entry - 1)
                fees = self.bet_amount * 0.02
                slippage = self.bet_amount * 0.005
                net_pnl = gross - fees - slippage

                goal.entry_odds = entry
                goal.exit_odds = exit_
                goal.pnl = net_pnl

                self._trades += 1
                self._total_pnl += net_pnl
                if net_pnl > 0:
                    self._wins += 1

                # Une seule ligne CSV par but (avec PnL)
                self._append_backtest_row(goal)

                report = self._price_tracker.format_analysis_report(analysis)
                pnl_color = "green" if net_pnl > 0 else "red"
                console.print(f"\n[bold cyan]📈 Backtest RÉEL[/bold cyan]")
                console.print(f"   Entry (ask): {entry:.2f} → Exit (bid): {exit_:.2f}")
                console.print(f"   [{pnl_color}]PnL: ${net_pnl:+.2f}[/{pnl_color}] | Total: ${self._total_pnl:+.2f}")
                try:
                    with open(WORK_LOG, "a") as f:
                        f.write(f"\n\n## Price Analysis - {datetime.now().isoformat()}\n{report}\n")
                except OSError as e:
                    logger.warning(f"Could not append to WORK_LOG: {e}")
            else:
                if not analysis.market_found:
                    console.print("[dim]   (Marché Polymarket non trouvé - pas de backtest pour ce but)[/dim]")
                elif not entry or not exit_:
                    console.print("[dim]   (Prix ask T+0 ou bid T+60 manquant - pas de PnL pour ce but)[/dim]")
                # But enregistré même sans PnL (une ligne CSV)
                try:
                    self._append_csv(goal)
                except OSError as e:
                    logger.warning(f"Could not append goal to CSV: {e}")
        except Exception as e:
            logger.warning(f"Price tracking error: {e}")

    def _append_backtest_row(self, goal: GoalRecord):
        """Append a row with real backtest data (updates the CSV - we append since we can't edit)"""
        try:
            with open(self._csv_file, "a", newline="") as f:
                writer = csv.writer(f)
                writer.writerow([
                    goal.timestamp.isoformat(),
                    goal.match,
                    goal.league,
                    goal.home_team,
                    goal.away_team,
                    goal.scoring_team,
                    goal.minute,
                    goal.score_before,
                    goal.score_after,
                    f"{goal.entry_odds:.3f}",
                    f"{goal.exit_odds:.3f}",
                    f"{goal.pnl:.2f}",
                    f"{self._total_pnl:.2f}",
                ])
        except OSError as e:
            logger.warning(f"Could not append backtest row: {e}")
    
    async def _heartbeat_loop(self):
        """Affiche un signe de vie toutes les 30s + table des matchs suivi(s)"""
        while self._running:
            await asyncio.sleep(30)
            n = len(self._matches)
            n_live = sum(1 for m in self._matches.values() if m.get("period") or m.get("elapsed"))
            if n_live < n:
                console.print(f"[dim]En écoute… {n} match(s) suivi(s) ({n_live} avec flux live)[/dim]")
            else:
                console.print(f"[dim]En écoute… {n} match(s) soccer suivi(s)[/dim]")
            # Tableau : uniquement les matchs en live (avec period/elapsed)
            if n_live > 0:
                rows = []
                for slug, m in self._matches.items():
                    if not (m.get("period") or m.get("elapsed")):
                        continue
                    h = m.get("home_team", "")[:22]
                    a = m.get("away_team", "")[:22]
                    sc = f"{m.get('home_score', 0)}-{m.get('away_score', 0)}"
                    league = (m.get("league") or "")[:8]
                    rows.append((h, a, sc, league))
                rows.sort(key=lambda x: x[0])
                tbl = Table(show_header=True, header_style="bold dim", title="Matchs en live")
                tbl.add_column("Home", style="cyan", max_width=24)
                tbl.add_column("Away", style="cyan", max_width=24)
                tbl.add_column("Score", style="green", width=6)
                tbl.add_column("Ligue", style="dim", width=8)
                for h, a, sc, league in rows[:25]:
                    tbl.add_row(h, a, sc, league)
                if len(rows) > 25:
                    tbl.caption = f"… et {len(rows) - 25} autre(s)"
                console.print(tbl)
    
    async def _gamma_refresh_loop(self):
        """Rafraîchit la liste des matchs depuis Gamma API toutes les 2 min (fetch immédiat au démarrage)."""
        while self._running:
            n = await self._fetch_live_matches_from_gamma()
            if n > 0:
                self._write_matches_file()
            # Notif "matchs en live" uniquement quand la liste des matchs vraiment en direct change
            live_slugs = frozenset(
                slug for slug, m in self._matches.items()
                if self._match_is_live(m)
            )
            if live_slugs and live_slugs != self._last_notified_match_slugs:
                self._last_notified_match_slugs = live_slugs
                match_lines = []
                for slug in sorted(live_slugs):
                    m = self._matches.get(slug, {})
                    home = m.get("home_team", "")
                    away = m.get("away_team", "")
                    league = (m.get("league") or "").strip()
                    line = f"{home} vs {away}"
                    if league:
                        line += f" ({league})"
                    match_lines.append(line)
                notify_matches_followed(match_lines)
            await asyncio.sleep(120)
    
    async def _live_price_poll_loop(self):
        """Grab le prix (home + away) des matchs en live 3×/s; au but, garde 3 min avant + 3 min après en CSV."""
        _cleanup_counter = 0
        while self._running:
            await asyncio.sleep(1.0 / 3.0)  # 3 fois par seconde
            now = datetime.now(timezone.utc)
            _cleanup_counter += 1
            if _cleanup_counter >= 60:
                _cleanup_counter = 0
                self._flush_goal_detection_pending()
                for slug in list(self._matches.keys()):
                    m = self._matches.get(slug)
                    if not m or not m.get("ended_at"):
                        continue
                    try:
                        end_dt = datetime.fromisoformat(m["ended_at"].replace("Z", "+00:00"))
                        if end_dt.tzinfo is None:
                            end_dt = end_dt.replace(tzinfo=timezone.utc)
                        if (now - end_dt).total_seconds() > 5 * 60:
                            del self._matches[slug]
                            self._token_cache.pop(slug, None)
                            self._write_matches_file()
                    except (ValueError, TypeError, KeyError):
                        pass
            slugs = list(self._matches.keys())
            for slug in slugs:
                try:
                    m = self._matches.get(slug)
                    if not m:
                        continue
                    # Buffer + CSV realtime uniquement pour les matchs qu'on suit (period/elapsed du WebSocket)
                    if not m.get("period") and not m.get("elapsed"):
                        continue
                    home_team = m.get("home_team", "")
                    away_team = m.get("away_team", "")
                    if slug not in self._token_cache:
                        league = m.get("league", "") or ""
                        tokens = await self._price_tracker.find_tokens_for_match(
                            slug, home_team, away_team, league=league or None
                        )
                        self._token_cache[slug] = tokens
                    token_home, token_draw, token_away = self._token_cache[slug]
                    if token_home is None and token_away is None and token_draw is None:
                        self._append_realtime_prices(now, slug, home_team, away_team, None, None, None, None, None, None)
                        continue
                    ah, bh, ad, bd, aa, ba = await asyncio.gather(
                        self._price_tracker.get_ask(token_home) if token_home else asyncio.sleep(0, None),
                        self._price_tracker.get_bid(token_home) if token_home else asyncio.sleep(0, None),
                        self._price_tracker.get_ask(token_draw) if token_draw else asyncio.sleep(0, None),
                        self._price_tracker.get_bid(token_draw) if token_draw else asyncio.sleep(0, None),
                        self._price_tracker.get_ask(token_away) if token_away else asyncio.sleep(0, None),
                        self._price_tracker.get_bid(token_away) if token_away else asyncio.sleep(0, None),
                    )
                    # Toujours écrire une ligne realtime (1 par seconde) même si tous les prix sont None
                    self._append_realtime_prices(now, slug, home_team, away_team, ah, bh, ad, bd, aa, ba)
                except Exception as e:
                    logger.debug(f"Price poll for {slug}: {e}")

    async def _status_loop(self):
        """Print status periodically"""
        while self._running:
            await asyncio.sleep(120)  # Every 2 minutes
            self._print_status()
    
    def _print_status(self):
        """Print current status (réservé pour usage futur)."""
        pass
    
    async def _improvement_loop(self):
        """Analyze data and improve strategy periodically"""
        while self._running:
            await asyncio.sleep(300)  # Every 5 minutes
            
            if len(self._goals) < 3:
                continue
            
            self._iteration += 1
            await self._analyze_and_improve()
    
    async def _analyze_and_improve(self):
        """Analyze collected data - ONLY goals with REAL PnL from PriceTracker"""
        console.print("\n[bold cyan]🔄 Running Analysis & Improvement...[/bold cyan]")
        
        # Only use goals with REAL PnL (from Polymarket prices)
        goals_with_pnl = [g for g in self._goals if g.pnl is not None]
        if len(goals_with_pnl) < 2:
            console.print(f"[dim]Pas assez de backtests réels ({len(goals_with_pnl)}). Attendre plus de buts avec marché Polymarket.[/dim]")
            return
        
        pnls = [g.pnl for g in goals_with_pnl]
        wins = [g for g in goals_with_pnl if g.pnl > 0]
        losses = [g for g in goals_with_pnl if g.pnl <= 0]
        
        avg_pnl = statistics.mean(pnls) if pnls else 0
        win_rate = len(wins) / len(goals_with_pnl) if goals_with_pnl else 0
        avg_win = statistics.mean([g.pnl for g in wins]) if wins else 0
        avg_loss = statistics.mean([abs(g.pnl) for g in losses]) if losses else 0
        
        # Calculate Sharpe (simplified)
        if len(pnls) > 1 and statistics.stdev(pnls) > 0:
            sharpe = (avg_pnl / statistics.stdev(pnls)) * (252 ** 0.5)
        else:
            sharpe = 0
        
        # Analyze by minute (only real data)
        early_goals = [g for g in goals_with_pnl if self._get_minute(g.minute) < 45]
        late_goals = [g for g in goals_with_pnl if self._get_minute(g.minute) >= 45]
        
        early_win_rate = len([g for g in early_goals if g.pnl > 0]) / len(early_goals) if early_goals else 0
        late_win_rate = len([g for g in late_goals if g.pnl > 0]) / len(late_goals) if late_goals else 0
        
        # Generate insights
        insights = []
        
        if early_win_rate > late_win_rate + 0.1:
            insights.append("Early goals (< 45') perform better")
        elif late_win_rate > early_win_rate + 0.1:
            insights.append("Late goals (>= 45') perform better")
        
        if avg_loss > avg_win * 1.5:
            insights.append("Losses too large - consider smaller bet size")
            self.params["bet_amount"] = max(5, self.params["bet_amount"] * 0.9)
        
        if win_rate < 0.45:
            insights.append("Win rate low - strategy may need adjustment")
        
        # Update WORK_LOG
        self._update_work_log(
            avg_pnl=avg_pnl,
            win_rate=win_rate,
            sharpe=sharpe,
            insights=insights,
        )
        
        # Print findings
        console.print(f"\n[bold]Iteration {self._iteration} Results:[/bold]")
        console.print(f"  Avg PnL: ${avg_pnl:.2f}")
        console.print(f"  Win Rate: {win_rate:.1%}")
        console.print(f"  Sharpe: {sharpe:.2f}")
        console.print(f"  Early goals WR: {early_win_rate:.1%} | Late goals WR: {late_win_rate:.1%}")
        
        if insights:
            console.print(f"\n[yellow]Insights:[/yellow]")
            for insight in insights:
                console.print(f"  • {insight}")
        
        console.print(f"\n[green]Updated params: {self.params}[/green]")
    
    def _goal_minute_for_filter(self, minute: str) -> int:
        """Minute number for filter: 1H/2H = période (30/60), pas minute 1 ou 2."""
        s = (minute or "").strip().upper()
        if not s:
            return 45
        # 1H = première mi-temps → 30 ; 2H = deuxième mi-temps → 60
        if "1H" in s or (s.startswith("1") and "H" in s) or "1ST" in s or "FIRST" in s:
            return 30
        if "2H" in s or (s.startswith("2") and "H" in s) or "2ND" in s or "SECOND" in s:
            return 60
        try:
            digits = "".join(filter(str.isdigit, minute or "45"))
            if not digits:
                return 45
            n = int(digits)
            # Éviter de confondre "1" ou "2" seuls avec 1H/2H
            if n <= 2 and ("H" in s or "HALF" in s):
                return 30 if n == 1 else 60
            return min(max(n, 0), 90)
        except (ValueError, TypeError):
            return 45

    def _get_minute(self, minute_str: str) -> int:
        """Extract minute number from string (for analysis). Uses same 1H/2H logic."""
        return self._goal_minute_for_filter(minute_str)
    
    def _update_work_log(self, avg_pnl: float, win_rate: float, sharpe: float, insights: list):
        """Update WORK_LOG.md with analysis results"""
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        
        entry = f"""
---

## Live Analysis - Iteration {self._iteration}
**Timestamp:** {timestamp}

### Performance Metrics (Live Data)
| Metric | Value |
|--------|-------|
| Total Trades | {self._trades} |
| Win Rate | {win_rate:.1%} |
| Avg PnL/Trade | ${avg_pnl:.2f} |
| Total PnL | ${self._total_pnl:.2f} |
| Sharpe Ratio | {sharpe:.2f} |

### Current Parameters
```json
{json.dumps(self.params, indent=2)}
```

### Insights
{chr(10).join(f"- {i}" for i in insights) if insights else "- No significant patterns detected yet"}

### Data File
`{self._csv_file.name}`
"""
        
        # Append to WORK_LOG
        if WORK_LOG.exists():
            current = WORK_LOG.read_text()
            # Insert after "## Agent Activity" or at the end
            if "## Live Analysis" in current:
                # Find last occurrence and append after
                parts = current.rsplit("## Live Analysis", 1)
                current = parts[0] + entry + "\n## Live Analysis" + parts[1]
            else:
                current += entry
            WORK_LOG.write_text(current)
        else:
            WORK_LOG.write_text(f"# Work Log\n{entry}")
    
    def get_summary(self) -> dict:
        """Get system summary"""
        return {
            "trades": self._trades,
            "wins": self._wins,
            "total_pnl": self._total_pnl,
            "win_rate": self._wins / self._trades if self._trades > 0 else 0,
            "params": self.params,
            "csv_file": str(self._csv_file),
            "iteration": self._iteration,
        }


async def main():
    """Run the live system"""
    exit_after = int(settings.trading.exit_after_seconds)
    bet_amount = float(settings.trading.bet_amount_usdc)
    system = LiveTradingSystem(bet_amount=bet_amount, exit_after=exit_after)
    
    try:
        await system.start()
    except KeyboardInterrupt:
        await system.stop()
        
        # Final summary
        summary = system.get_summary()
        console.print(Panel.fit(
            f"[bold]Final Summary[/bold]\n\n"
            f"Trades: {summary['trades']}\n"
            f"Win Rate: {summary['win_rate']:.1%}\n"
            f"Total PnL: ${summary['total_pnl']:.2f}\n"
            f"Iterations: {summary['iteration']}\n\n"
            f"Data saved to: {summary['csv_file']}",
            title="Session Complete",
        ))


if __name__ == "__main__":
    asyncio.run(main())
