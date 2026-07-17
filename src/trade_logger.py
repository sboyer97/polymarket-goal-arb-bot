"""
Structured trade logging pour analyse post-trade.
Écrit les trades en JSON dans un fichier séparé pour analyse facile.
"""
import json
import asyncio
from pathlib import Path
from datetime import datetime
from typing import Optional, Any
from dataclasses import dataclass, asdict
from loguru import logger

# Répertoire des logs
TRADES_LOG_DIR = Path("server_logs/trades")
TRADES_LOG_DIR.mkdir(parents=True, exist_ok=True)


@dataclass
class TradeRecord:
    """Structure d'un trade pour logging."""
    timestamp: str
    event_type: str  # BUY, SELL, TP, SL, TIMEOUT, SKIP
    
    # Match info
    match_slug: str
    home_team: str
    away_team: str
    score: str
    minute: int
    
    # Trade info
    token_id: str
    bet_on: str  # "home", "away", "draw"
    strategy: str  # "momentum", "draw", "contrarian"
    
    # Execution
    amount_usd: float
    shares: float
    entry_price: float
    exit_price: float
    
    # Results
    pnl_usd: float
    pnl_pct: float
    hold_time_seconds: float
    
    # Metadata
    latency_ms: float
    order_id: str
    exit_reason: str  # "tp", "sl", "timeout", "manual"
    skip_reason: str
    
    # Market state
    bid_at_entry: float
    ask_at_entry: float
    spread_pct: float


class TradeLogger:
    """Logger structuré pour les trades - async, non-bloquant."""
    
    def __init__(self):
        self._log_file = TRADES_LOG_DIR / f"trades_{datetime.now().strftime('%Y%m%d')}.jsonl"
        self._pending_trades: dict[str, dict] = {}  # slug -> trade data
    
    def _write_record(self, record: dict) -> None:
        """Écrit un record JSON (fire-and-forget via thread)."""
        try:
            with open(self._log_file, "a") as f:
                f.write(json.dumps(record, ensure_ascii=False) + "\n")
        except Exception as e:
            logger.debug(f"Trade log write error: {e}")
    
    def log_entry(
        self,
        slug: str,
        home_team: str,
        away_team: str,
        score: str,
        minute: int,
        token_id: str,
        bet_on: str,
        strategy: str,
        amount_usd: float,
        shares: float,
        entry_price: float,
        order_id: str,
        latency_ms: float,
        bid: float = 0,
        ask: float = 0,
    ) -> None:
        """Log une entrée de position (BUY)."""
        spread = ((ask - bid) / bid * 100) if bid > 0 else 0
        
        record = {
            "timestamp": datetime.utcnow().isoformat(),
            "event_type": "BUY",
            "match_slug": slug,
            "home_team": home_team,
            "away_team": away_team,
            "score": score,
            "minute": minute,
            "token_id": token_id,
            "bet_on": bet_on,
            "strategy": strategy,
            "amount_usd": amount_usd,
            "shares": shares,
            "entry_price": entry_price,
            "order_id": order_id,
            "latency_ms": latency_ms,
            "bid_at_entry": bid,
            "ask_at_entry": ask,
            "spread_pct": spread,
        }
        
        # Stocker pour la sortie
        self._pending_trades[slug] = {
            "entry_time": datetime.utcnow(),
            "entry_price": entry_price,
            "shares": shares,
            "amount_usd": amount_usd,
            **record,
        }
        
        # Fire-and-forget write
        asyncio.get_event_loop().run_in_executor(None, self._write_record, record)
        logger.debug(f"Trade entry logged: {slug}")
    
    def log_exit(
        self,
        slug: str,
        exit_price: float,
        pnl_usd: float,
        pnl_pct: float,
        exit_reason: str,  # "tp", "sl", "timeout"
        latency_ms: float = 0,
    ) -> None:
        """Log une sortie de position (SELL)."""
        entry = self._pending_trades.pop(slug, {})
        
        hold_time = 0
        if entry.get("entry_time"):
            hold_time = (datetime.utcnow() - entry["entry_time"]).total_seconds()
        
        record = {
            "timestamp": datetime.utcnow().isoformat(),
            "event_type": exit_reason.upper(),
            "match_slug": slug,
            "home_team": entry.get("home_team", ""),
            "away_team": entry.get("away_team", ""),
            "score": entry.get("score", ""),
            "minute": entry.get("minute", 0),
            "token_id": entry.get("token_id", ""),
            "bet_on": entry.get("bet_on", ""),
            "strategy": entry.get("strategy", ""),
            "amount_usd": entry.get("amount_usd", 0),
            "shares": entry.get("shares", 0),
            "entry_price": entry.get("entry_price", 0),
            "exit_price": exit_price,
            "pnl_usd": pnl_usd,
            "pnl_pct": pnl_pct,
            "hold_time_seconds": hold_time,
            "latency_ms": latency_ms,
            "exit_reason": exit_reason,
        }
        
        asyncio.get_event_loop().run_in_executor(None, self._write_record, record)
        logger.debug(f"Trade exit logged: {slug} | P&L: {pnl_pct:+.2f}%")
    
    def log_skip(
        self,
        slug: str,
        home_team: str,
        away_team: str,
        score: str,
        minute: int,
        reason: str,
    ) -> None:
        """Log un trade skippé."""
        record = {
            "timestamp": datetime.utcnow().isoformat(),
            "event_type": "SKIP",
            "match_slug": slug,
            "home_team": home_team,
            "away_team": away_team,
            "score": score,
            "minute": minute,
            "skip_reason": reason,
        }
        
        asyncio.get_event_loop().run_in_executor(None, self._write_record, record)


# Instance globale
trade_logger = TradeLogger()
