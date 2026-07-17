#!/usr/bin/env python3
"""
Score-based Trading using Polymarket WebSocket

Trade on score changes detected directly from Polymarket's Sports WebSocket.
No external data provider needed.
"""

import asyncio
import json
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from typing import Optional, Callable, Awaitable
from enum import Enum

import websockets
from loguru import logger
from rich.console import Console
from rich.table import Table

console = Console()


class Sport(Enum):
    SOCCER = "soccer"
    NBA = "nba"
    NFL = "nfl"
    MLB = "mlb"


@dataclass
class ScoreChange:
    """Represents a score change event"""
    timestamp: datetime
    slug: str
    league: str
    home_team: str
    away_team: str
    old_home: int
    old_away: int
    new_home: int
    new_away: int
    period: str
    
    @property
    def home_scored(self) -> bool:
        return self.new_home > self.old_home
    
    @property
    def away_scored(self) -> bool:
        return self.new_away > self.old_away
    
    @property
    def points_scored(self) -> int:
        return (self.new_home + self.new_away) - (self.old_home + self.old_away)
    
    @property
    def scoring_team(self) -> str:
        if self.home_scored:
            return self.home_team
        elif self.away_scored:
            return self.away_team
        return "unknown"


@dataclass
class MatchState:
    """Current state of a match"""
    slug: str
    league: str
    home_team: str
    away_team: str
    home_score: int = 0
    away_score: int = 0
    period: str = ""
    status: str = ""
    last_update: datetime = field(default_factory=datetime.utcnow)


@dataclass
class TradeSignal:
    """Signal to place a trade"""
    timestamp: datetime
    slug: str
    direction: str  # "buy" or "sell"
    team: str
    reason: str
    confidence: float  # 0-1
    
    
class PolymarketScoreTrader:
    """
    Trade based on score changes from Polymarket WebSocket.
    
    Strategy: When a goal/score happens, the scoring team's win probability
    typically increases. We can:
    1. Buy the scoring team immediately after detection
    2. Sell after the market adjusts (usually seconds to minutes)
    """
    
    WS_URL = "wss://sports-api.polymarket.com/ws"
    
    def __init__(
        self,
        leagues: list[str] = None,
        on_score_change: Callable[[ScoreChange], Awaitable[None]] = None,
        on_signal: Callable[[TradeSignal], Awaitable[None]] = None,
    ):
        self.leagues = leagues or ["nba", "mls", "soccer"]
        self.on_score_change = on_score_change
        self.on_signal = on_signal
        
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._running = False
        self._matches: dict[str, MatchState] = {}
        self._score_history: list[ScoreChange] = []
        self._signals: list[TradeSignal] = []
        
        # Stats
        self._connect_time: Optional[datetime] = None
        self._messages_received = 0
        self._score_changes_detected = 0
    
    async def start(self):
        """Start listening to Polymarket WebSocket"""
        self._running = True
        self._connect_time = datetime.utcnow()
        
        console.print(f"[cyan]Polymarket Score Trader[/cyan]")
        console.print(f"[dim]Leagues: {', '.join(self.leagues)}[/dim]")
        console.print(f"[dim]Connecting to {self.WS_URL}...[/dim]\n")
        
        while self._running:
            try:
                async with websockets.connect(self.WS_URL) as ws:
                    self._ws = ws
                    console.print("[green]✓ Connected to Polymarket Sports WebSocket[/green]\n")
                    
                    async for message in ws:
                        if not self._running:
                            break
                        
                        await self._handle_message(message)
                        
            except websockets.ConnectionClosed:
                if self._running:
                    console.print("[yellow]Connection lost, reconnecting in 3s...[/yellow]")
                    await asyncio.sleep(3)
            except Exception as e:
                logger.error(f"WebSocket error: {e}")
                if self._running:
                    await asyncio.sleep(3)
        
        console.print("[dim]Trader stopped[/dim]")
    
    async def stop(self):
        """Stop the trader"""
        self._running = False
        if self._ws:
            await self._ws.close()
    
    async def _handle_message(self, message: str):
        """Handle incoming WebSocket message"""
        if message == "ping":
            await self._ws.send("pong")
            return
        
        self._messages_received += 1
        
        try:
            data = json.loads(message)
        except json.JSONDecodeError:
            return
        
        league = data.get("leagueAbbreviation", "").lower()
        if league not in self.leagues:
            return
        
        slug = data.get("slug", "")
        if not slug:
            return
        
        # Parse score
        score_str = data.get("score", "0-0")
        try:
            parts = score_str.split("-")
            home_score = int(parts[0])
            away_score = int(parts[1]) if len(parts) > 1 else 0
        except (ValueError, IndexError):
            return
        
        home_team = data.get("homeTeam", "")
        away_team = data.get("awayTeam", "")
        period = data.get("period", "")
        status = data.get("status", "")
        
        # Check if this is a score change
        old_state = self._matches.get(slug)
        
        if old_state:
            if home_score != old_state.home_score or away_score != old_state.away_score:
                # Score changed!
                change = ScoreChange(
                    timestamp=datetime.utcnow(),
                    slug=slug,
                    league=league,
                    home_team=home_team,
                    away_team=away_team,
                    old_home=old_state.home_score,
                    old_away=old_state.away_score,
                    new_home=home_score,
                    new_away=away_score,
                    period=period,
                )
                
                await self._on_score_change(change)
        
        # Update state
        self._matches[slug] = MatchState(
            slug=slug,
            league=league,
            home_team=home_team,
            away_team=away_team,
            home_score=home_score,
            away_score=away_score,
            period=period,
            status=status,
            last_update=datetime.utcnow(),
        )
    
    async def _on_score_change(self, change: ScoreChange):
        """Handle a detected score change"""
        self._score_changes_detected += 1
        self._score_history.append(change)
        
        # Log the score change
        emoji = "⚽" if change.league in ["mls", "soccer"] else "🏀"
        console.print(
            f"[bold green]{emoji} SCORE![/bold green] "
            f"[cyan]{change.home_team}[/cyan] {change.new_home}-{change.new_away} "
            f"[cyan]{change.away_team}[/cyan] "
            f"[dim]({change.period})[/dim] "
            f"@ {change.timestamp.strftime('%H:%M:%S.%f')[:-3]}"
        )
        
        # Generate trading signal
        signal = self._generate_signal(change)
        if signal:
            self._signals.append(signal)
            console.print(
                f"  [yellow]→ Signal: {signal.direction.upper()} {signal.team}[/yellow] "
                f"[dim]({signal.reason}, conf: {signal.confidence:.0%})[/dim]"
            )
            
            if self.on_signal:
                await self.on_signal(signal)
        
        # Call external handler if provided
        if self.on_score_change:
            await self.on_score_change(change)
    
    def _generate_signal(self, change: ScoreChange) -> Optional[TradeSignal]:
        """Generate a trading signal from a score change"""
        
        # Basic strategy: buy the team that just scored
        if change.home_scored:
            return TradeSignal(
                timestamp=change.timestamp,
                slug=change.slug,
                direction="buy",
                team=change.home_team,
                reason=f"scored ({change.old_home}->{change.new_home})",
                confidence=0.7,
            )
        elif change.away_scored:
            return TradeSignal(
                timestamp=change.timestamp,
                slug=change.slug,
                direction="buy",
                team=change.away_team,
                reason=f"scored ({change.old_away}->{change.new_away})",
                confidence=0.7,
            )
        
        return None
    
    def get_active_matches(self) -> list[MatchState]:
        """Get currently tracked matches"""
        cutoff = datetime.utcnow() - timedelta(minutes=5)
        return [m for m in self._matches.values() if m.last_update > cutoff]
    
    def get_stats(self) -> dict:
        """Get trader statistics"""
        uptime = (datetime.utcnow() - self._connect_time).total_seconds() if self._connect_time else 0
        return {
            "uptime_seconds": uptime,
            "messages_received": self._messages_received,
            "score_changes": self._score_changes_detected,
            "active_matches": len(self.get_active_matches()),
            "signals_generated": len(self._signals),
        }
    
    def print_status(self):
        """Print current status"""
        stats = self.get_stats()
        matches = self.get_active_matches()
        
        console.print("\n" + "=" * 60)
        console.print("[bold cyan]TRADER STATUS[/bold cyan]")
        console.print("=" * 60)
        
        console.print(f"\nUptime: {stats['uptime_seconds']:.0f}s")
        console.print(f"Messages: {stats['messages_received']}")
        console.print(f"Score changes: {stats['score_changes']}")
        console.print(f"Signals: {stats['signals_generated']}")
        
        if matches:
            table = Table(title=f"\nActive Matches ({len(matches)})")
            table.add_column("League", style="blue")
            table.add_column("Home", style="cyan")
            table.add_column("Score", justify="center", style="green")
            table.add_column("Away", style="cyan")
            table.add_column("Period")
            
            for m in sorted(matches, key=lambda x: x.league):
                table.add_row(
                    m.league.upper(),
                    m.home_team[:12],
                    f"{m.home_score}-{m.away_score}",
                    m.away_team[:12],
                    m.period or m.status,
                )
            
            console.print(table)
        
        if self._score_history:
            console.print(f"\n[bold]Recent Score Changes:[/bold]")
            for change in self._score_history[-5:]:
                emoji = "⚽" if change.league in ["mls", "soccer"] else "🏀"
                console.print(
                    f"  {emoji} {change.scoring_team} scored "
                    f"({change.old_home + change.old_away} → {change.new_home + change.new_away}) "
                    f"@ {change.timestamp.strftime('%H:%M:%S')}"
                )


async def main():
    """Demo run"""
    trader = PolymarketScoreTrader(leagues=["nba", "mls", "soccer"])
    
    async def status_loop():
        while True:
            await asyncio.sleep(30)
            trader.print_status()
    
    try:
        status_task = asyncio.create_task(status_loop())
        await trader.start()
    except KeyboardInterrupt:
        await trader.stop()
        trader.print_status()


if __name__ == "__main__":
    asyncio.run(main())
