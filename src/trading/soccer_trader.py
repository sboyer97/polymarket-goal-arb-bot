#!/usr/bin/env python3
"""
Soccer Goal Trader

Strategy: When a goal is scored, bet on the team that scored.
Uses Polymarket WebSocket directly for goal detection.
"""

import asyncio
import json
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from typing import Optional

import websockets
from loguru import logger
from rich.console import Console
from rich.table import Table

console = Console()


@dataclass
class Goal:
    """A detected goal event"""
    timestamp: datetime
    slug: str
    league: str
    home_team: str
    away_team: str
    scoring_team: str
    old_score: str  # "1-0"
    new_score: str  # "2-0"
    minute: str


@dataclass
class Trade:
    """A trade placed"""
    timestamp: datetime
    slug: str
    team: str
    side: str  # "buy"
    amount: float
    entry_price: Optional[float] = None
    exit_price: Optional[float] = None
    pnl: Optional[float] = None
    status: str = "pending"  # pending, open, closed


@dataclass 
class MatchState:
    """Current state of a soccer match"""
    slug: str
    league: str
    home_team: str
    away_team: str
    home_score: int = 0
    away_score: int = 0
    period: str = ""
    status: str = ""
    last_update: datetime = field(default_factory=datetime.utcnow)


class SoccerGoalTrader:
    """
    Trade soccer goals on Polymarket.
    
    Strategy:
    - Listen to Polymarket WebSocket for score changes
    - When a goal is detected, BUY the scoring team
    - Exit position after X seconds or Y% profit
    """
    
    WS_URL = "wss://sports-api.polymarket.com/ws"
    SOCCER_LEAGUES = ["mls", "soccer", "epl", "laliga", "seriea", "bundesliga", "ligue1", "ucl", "uel"]
    
    def __init__(
        self,
        bet_amount: float = 10.0,
        exit_after_seconds: int = 60,
        dry_run: bool = True,
    ):
        self.bet_amount = bet_amount
        self.exit_after_seconds = exit_after_seconds
        self.dry_run = dry_run
        
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._running = False
        self._matches: dict[str, MatchState] = {}
        
        # History
        self._goals: list[Goal] = []
        self._trades: list[Trade] = []
        
        # Stats
        self._connect_time: Optional[datetime] = None
        self._messages_received = 0
    
    async def start(self):
        """Start the trader"""
        self._running = True
        self._connect_time = datetime.utcnow()
        
        mode = "[yellow]DRY RUN[/yellow]" if self.dry_run else "[red]LIVE[/red]"
        
        console.print(f"\n[bold cyan]⚽ Soccer Goal Trader[/bold cyan] {mode}")
        console.print(f"[dim]Bet amount: ${self.bet_amount}[/dim]")
        console.print(f"[dim]Exit after: {self.exit_after_seconds}s[/dim]")
        console.print(f"[dim]Leagues: {', '.join(self.SOCCER_LEAGUES)}[/dim]\n")
        
        while self._running:
            try:
                async with websockets.connect(self.WS_URL) as ws:
                    self._ws = ws
                    console.print("[green]✓ Connected to Polymarket[/green]")
                    console.print("[dim]Waiting for goals...[/dim]\n")
                    
                    async for message in ws:
                        if not self._running:
                            break
                        await self._handle_message(message)
                        
            except websockets.ConnectionClosed:
                if self._running:
                    console.print("[yellow]Reconnecting...[/yellow]")
                    await asyncio.sleep(2)
            except Exception as e:
                logger.error(f"Error: {e}")
                if self._running:
                    await asyncio.sleep(2)
    
    async def stop(self):
        """Stop the trader"""
        self._running = False
        if self._ws:
            await self._ws.close()
    
    async def _handle_message(self, message: str):
        """Handle WebSocket message"""
        if message == "ping":
            await self._ws.send("pong")
            return
        
        self._messages_received += 1
        
        try:
            data = json.loads(message)
        except json.JSONDecodeError:
            return
        
        # Filter soccer only
        league = data.get("leagueAbbreviation", "").lower()
        if league not in self.SOCCER_LEAGUES:
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
        
        # Check for goal
        old_state = self._matches.get(slug)
        
        if old_state:
            home_diff = home_score - old_state.home_score
            away_diff = away_score - old_state.away_score
            
            if home_diff > 0:
                # Home team scored!
                await self._on_goal(
                    slug=slug,
                    league=league,
                    home_team=home_team,
                    away_team=away_team,
                    scoring_team=home_team,
                    old_score=f"{old_state.home_score}-{old_state.away_score}",
                    new_score=f"{home_score}-{away_score}",
                    period=period,
                )
            
            if away_diff > 0:
                # Away team scored!
                await self._on_goal(
                    slug=slug,
                    league=league,
                    home_team=home_team,
                    away_team=away_team,
                    scoring_team=away_team,
                    old_score=f"{old_state.home_score}-{old_state.away_score}",
                    new_score=f"{home_score}-{away_score}",
                    period=period,
                )
        
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
    
    async def _on_goal(
        self,
        slug: str,
        league: str,
        home_team: str,
        away_team: str,
        scoring_team: str,
        old_score: str,
        new_score: str,
        period: str,
    ):
        """Handle a detected goal"""
        now = datetime.utcnow()
        
        goal = Goal(
            timestamp=now,
            slug=slug,
            league=league,
            home_team=home_team,
            away_team=away_team,
            scoring_team=scoring_team,
            old_score=old_score,
            new_score=new_score,
            minute=period,
        )
        self._goals.append(goal)
        
        # Display goal
        console.print(f"\n[bold green]⚽⚽⚽ GOAL! ⚽⚽⚽[/bold green]")
        console.print(f"[cyan]{home_team}[/cyan] {new_score} [cyan]{away_team}[/cyan]")
        console.print(f"[yellow]Scorer: {scoring_team}[/yellow]")
        console.print(f"[dim]League: {league.upper()} | Period: {period}[/dim]")
        console.print(f"[dim]Time: {now.strftime('%H:%M:%S.%f')[:-3]}[/dim]")
        
        # Place trade
        await self._place_bet(goal)
    
    async def _place_bet(self, goal: Goal):
        """Place a bet on the scoring team"""
        
        trade = Trade(
            timestamp=goal.timestamp,
            slug=goal.slug,
            team=goal.scoring_team,
            side="buy",
            amount=self.bet_amount,
            status="pending",
        )
        self._trades.append(trade)
        
        console.print(f"\n[bold yellow]📈 PLACING BET[/bold yellow]")
        console.print(f"[yellow]BUY {goal.scoring_team} for ${self.bet_amount}[/yellow]")
        
        if self.dry_run:
            console.print(f"[dim](Dry run - no real trade)[/dim]")
            trade.status = "simulated"
            trade.entry_price = 0.50  # Simulated
        else:
            # TODO: Integrate with Polymarket CLOB API
            # 1. Find the market for this match
            # 2. Find the outcome for scoring_team
            # 3. Place market buy order
            console.print(f"[red]Live trading not implemented yet[/red]")
            trade.status = "failed"
        
        # Schedule exit
        asyncio.create_task(self._schedule_exit(trade))
    
    async def _schedule_exit(self, trade: Trade):
        """Exit position after delay"""
        await asyncio.sleep(self.exit_after_seconds)
        
        if trade.status in ["simulated", "open"]:
            console.print(f"\n[cyan]📉 EXITING POSITION[/cyan]")
            console.print(f"[cyan]SELL {trade.team} after {self.exit_after_seconds}s[/cyan]")
            
            if self.dry_run:
                trade.exit_price = 0.55  # Simulated profit
                trade.pnl = (trade.exit_price - trade.entry_price) * trade.amount
                trade.status = "closed"
                console.print(f"[green]Simulated PnL: +${trade.pnl:.2f}[/green]")
            else:
                # TODO: Place sell order
                pass
    
    def get_active_matches(self) -> list[MatchState]:
        """Get matches updated in last 5 minutes"""
        cutoff = datetime.utcnow() - timedelta(minutes=5)
        return [
            m for m in self._matches.values() 
            if m.last_update > cutoff
        ]
    
    def print_status(self):
        """Print current status"""
        uptime = (datetime.utcnow() - self._connect_time).total_seconds() if self._connect_time else 0
        matches = self.get_active_matches()
        
        console.print("\n" + "=" * 50)
        console.print("[bold cyan]⚽ TRADER STATUS[/bold cyan]")
        console.print("=" * 50)
        
        console.print(f"\nUptime: {uptime:.0f}s ({uptime/60:.1f} min)")
        console.print(f"Messages: {self._messages_received}")
        console.print(f"Goals detected: {len(self._goals)}")
        console.print(f"Trades: {len(self._trades)}")
        console.print(f"Active soccer matches: {len(matches)}")
        
        if matches:
            table = Table(title="Active Matches")
            table.add_column("League", style="blue")
            table.add_column("Match", style="cyan")
            table.add_column("Score", justify="center", style="green")
            table.add_column("Period")
            
            for m in sorted(matches, key=lambda x: x.league):
                table.add_row(
                    m.league.upper(),
                    f"{m.home_team[:10]} vs {m.away_team[:10]}",
                    f"{m.home_score}-{m.away_score}",
                    m.period or m.status,
                )
            console.print(table)
        
        if self._goals:
            console.print(f"\n[bold]Recent Goals:[/bold]")
            for g in self._goals[-5:]:
                console.print(
                    f"  ⚽ {g.scoring_team} ({g.old_score} → {g.new_score}) "
                    f"@ {g.timestamp.strftime('%H:%M:%S')}"
                )
        
        if self._trades:
            total_pnl = sum(t.pnl or 0 for t in self._trades)
            console.print(f"\n[bold]Trades: {len(self._trades)} | Total PnL: ${total_pnl:.2f}[/bold]")


async def main():
    trader = SoccerGoalTrader(
        bet_amount=10.0,
        exit_after_seconds=60,
        dry_run=True,
    )
    
    async def status_loop():
        while True:
            await asyncio.sleep(60)
            trader.print_status()
    
    try:
        asyncio.create_task(status_loop())
        await trader.start()
    except KeyboardInterrupt:
        await trader.stop()
        trader.print_status()


if __name__ == "__main__":
    asyncio.run(main())
