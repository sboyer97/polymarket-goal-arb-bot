#!/usr/bin/env python3
"""
Live Data Collector - Records all soccer events from Polymarket

Collects:
- Every score change with timestamp
- Odds before/after (when available)
- Match metadata

Saves to CSV for backtesting and strategy improvement.
"""

import asyncio
import csv
import json
from datetime import datetime
from pathlib import Path
from dataclasses import dataclass, asdict
from typing import Optional

import websockets
from loguru import logger
from rich.console import Console

console = Console()

DATA_DIR = Path(__file__).parent.parent / "data" / "live"
DATA_DIR.mkdir(parents=True, exist_ok=True)


@dataclass
class LiveGoalEvent:
    """A goal event recorded live"""
    timestamp: str
    match_slug: str
    league: str
    home_team: str
    away_team: str
    scoring_team: str
    minute: str
    old_home_score: int
    old_away_score: int
    new_home_score: int
    new_away_score: int
    # Odds at time of goal (if we can capture them)
    home_odds_before: Optional[float] = None
    away_odds_before: Optional[float] = None
    home_odds_after: Optional[float] = None
    away_odds_after: Optional[float] = None


class LiveDataCollector:
    """
    Connects to Polymarket Sports WebSocket and records ALL soccer events.
    """
    
    WS_URL = "wss://sports-api.polymarket.com/ws"
    SOCCER_LEAGUES = ["mls", "soccer", "epl", "laliga", "seriea", "bundesliga", "ligue1", "ucl", "uel", "liga-mx"]
    
    def __init__(self):
        self._ws = None
        self._running = False
        self._matches: dict[str, dict] = {}  # slug -> last known state
        self._goals: list[LiveGoalEvent] = []
        self._csv_file = DATA_DIR / f"goals_{datetime.now().strftime('%Y%m%d')}.csv"
        self._stats = {
            "start_time": None,
            "messages": 0,
            "goals": 0,
            "matches_tracked": 0,
        }
        
        # Initialize CSV
        self._init_csv()
    
    def _init_csv(self):
        """Initialize CSV file with headers"""
        if not self._csv_file.exists():
            with open(self._csv_file, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow([
                    "timestamp", "match_slug", "league", "home_team", "away_team",
                    "scoring_team", "minute", "old_home_score", "old_away_score",
                    "new_home_score", "new_away_score",
                    "home_odds_before", "away_odds_before",
                    "home_odds_after", "away_odds_after"
                ])
    
    def _append_to_csv(self, goal: LiveGoalEvent):
        """Append a goal event to CSV"""
        with open(self._csv_file, "a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                goal.timestamp, goal.match_slug, goal.league,
                goal.home_team, goal.away_team, goal.scoring_team,
                goal.minute, goal.old_home_score, goal.old_away_score,
                goal.new_home_score, goal.new_away_score,
                goal.home_odds_before, goal.away_odds_before,
                goal.home_odds_after, goal.away_odds_after
            ])
    
    async def start(self):
        """Start collecting data"""
        self._running = True
        self._stats["start_time"] = datetime.utcnow()
        
        console.print("\n[bold cyan]📊 Live Data Collector Started[/bold cyan]")
        console.print(f"[dim]CSV: {self._csv_file}[/dim]")
        console.print(f"[dim]Leagues: {', '.join(self.SOCCER_LEAGUES)}[/dim]\n")
        
        while self._running:
            try:
                async with websockets.connect(self.WS_URL) as ws:
                    self._ws = ws
                    console.print("[green]✓ Connected to Polymarket Sports WebSocket[/green]")
                    
                    async for message in ws:
                        if not self._running:
                            break
                        await self._handle_message(message)
                        
            except websockets.ConnectionClosed:
                if self._running:
                    console.print("[yellow]Connection lost, reconnecting...[/yellow]")
                    await asyncio.sleep(2)
            except Exception as e:
                logger.error(f"Error: {e}")
                if self._running:
                    await asyncio.sleep(2)
    
    async def stop(self):
        """Stop collector"""
        self._running = False
        if self._ws:
            await self._ws.close()
    
    async def _handle_message(self, message: str):
        """Handle incoming message"""
        if message == "ping":
            await self._ws.send("pong")
            return
        
        self._stats["messages"] += 1
        
        try:
            data = json.loads(message)
        except json.JSONDecodeError:
            return
        
        league = data.get("leagueAbbreviation", "").lower()
        if league not in self.SOCCER_LEAGUES:
            return
        
        slug = data.get("slug", "")
        if not slug:
            return
        
        # Parse current state
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
        
        # Check for goal
        old_state = self._matches.get(slug)
        
        if old_state:
            old_home = old_state.get("home_score", 0)
            old_away = old_state.get("away_score", 0)
            
            # Detect goals
            if home_score > old_home:
                await self._record_goal(
                    slug=slug,
                    league=league,
                    home_team=home_team,
                    away_team=away_team,
                    scoring_team="home",
                    minute=period,
                    old_home=old_home,
                    old_away=old_away,
                    new_home=home_score,
                    new_away=away_score,
                )
            
            if away_score > old_away:
                await self._record_goal(
                    slug=slug,
                    league=league,
                    home_team=home_team,
                    away_team=away_team,
                    scoring_team="away",
                    minute=period,
                    old_home=old_home,
                    old_away=old_away,
                    new_home=home_score,
                    new_away=away_score,
                )
        else:
            self._stats["matches_tracked"] += 1
        
        # Update state
        self._matches[slug] = {
            "home_team": home_team,
            "away_team": away_team,
            "home_score": home_score,
            "away_score": away_score,
            "league": league,
            "period": period,
            "last_update": datetime.utcnow().isoformat(),
        }
    
    async def _record_goal(self, slug: str, league: str, home_team: str, away_team: str,
                           scoring_team: str, minute: str, old_home: int, old_away: int,
                           new_home: int, new_away: int):
        """Record a goal event"""
        now = datetime.utcnow()
        
        goal = LiveGoalEvent(
            timestamp=now.isoformat(),
            match_slug=slug,
            league=league,
            home_team=home_team,
            away_team=away_team,
            scoring_team=scoring_team,
            minute=minute,
            old_home_score=old_home,
            old_away_score=old_away,
            new_home_score=new_home,
            new_away_score=new_away,
        )
        
        self._goals.append(goal)
        self._stats["goals"] += 1
        
        # Save to CSV
        self._append_to_csv(goal)
        
        # Display
        scorer = home_team if scoring_team == "home" else away_team
        console.print(
            f"[bold green]⚽ GOAL RECORDED![/bold green] "
            f"[cyan]{scorer}[/cyan] scores! "
            f"{home_team} {new_home}-{new_away} {away_team} "
            f"[dim]({minute})[/dim]"
        )
        console.print(f"[dim]   → Saved to CSV (Total: {self._stats['goals']} goals)[/dim]")
    
    def get_stats(self) -> dict:
        """Get collector statistics"""
        uptime = 0
        if self._stats["start_time"]:
            uptime = (datetime.utcnow() - self._stats["start_time"]).total_seconds()
        
        return {
            **self._stats,
            "uptime_seconds": uptime,
            "csv_file": str(self._csv_file),
            "goals_list": self._goals,
        }


async def main():
    """Run the collector"""
    collector = LiveDataCollector()
    
    async def status_loop():
        while True:
            await asyncio.sleep(60)
            stats = collector.get_stats()
            console.print(
                f"\n[dim]--- Stats: {stats['uptime_seconds']:.0f}s | "
                f"Messages: {stats['messages']} | "
                f"Goals: {stats['goals']} | "
                f"Matches: {stats['matches_tracked']} ---[/dim]\n"
            )
    
    try:
        asyncio.create_task(status_loop())
        await collector.start()
    except KeyboardInterrupt:
        await collector.stop()
        console.print(f"\n[cyan]Final stats: {collector.get_stats()['goals']} goals recorded[/cyan]")


if __name__ == "__main__":
    asyncio.run(main())
