#!/usr/bin/env python3
"""
Latency Test Tool

Compare latency between AllSportsAPI and Polymarket Sports WebSocket.
Measures how fast each source reports score changes.
"""

import asyncio
import json
from datetime import datetime
from dataclasses import dataclass, field
from typing import Optional
import re

import websockets
from loguru import logger
from rich.console import Console
from rich.table import Table
from rich.live import Live

console = Console()


@dataclass
class ScoreEvent:
    """A score change event"""
    source: str  # "allsports" or "polymarket"
    timestamp: datetime
    match_id: str
    home_team: str
    away_team: str
    home_score: int
    away_score: int
    raw_data: dict = field(default_factory=dict)


@dataclass
class LatencyMeasurement:
    """Measurement of latency between two sources for same event"""
    match_description: str
    allsports_time: Optional[datetime] = None
    polymarket_time: Optional[datetime] = None
    allsports_score: str = ""
    polymarket_score: str = ""
    
    @property
    def latency_ms(self) -> Optional[float]:
        if self.allsports_time and self.polymarket_time:
            diff = (self.polymarket_time - self.allsports_time).total_seconds() * 1000
            return diff
        return None
    
    @property
    def who_first(self) -> str:
        if self.latency_ms is None:
            return "?"
        if self.latency_ms > 0:
            return "AllSports"
        elif self.latency_ms < 0:
            return "Polymarket"
        return "Tie"


class LatencyTester:
    """Test latency between AllSportsAPI and Polymarket"""
    
    ALLSPORTS_WS_FOOTBALL = "wss://wss.allsportsapi.com/live_events"
    ALLSPORTS_WS_BASKETBALL = "wss://wss.allsportsapi.com/basketball/live_events"
    POLYMARKET_WS = "wss://sports-api.polymarket.com/ws"
    
    def __init__(self, api_key: str, sport: str = "soccer"):
        self.api_key = api_key
        self.sport = sport.lower()
        
        if self.sport in ["nba", "basketball"]:
            self.allsports_ws_url = self.ALLSPORTS_WS_BASKETBALL
            self.polymarket_leagues = ["nba", "cbb"]
        else:
            self.allsports_ws_url = self.ALLSPORTS_WS_FOOTBALL
            self.polymarket_leagues = ["soccer", "mls"]
        self._allsports_ws = None
        self._polymarket_ws = None
        self._running = False
        
        self._allsports_scores: dict[str, tuple[int, int]] = {}
        self._polymarket_scores: dict[str, tuple[int, int]] = {}
        self._polymarket_matches: dict[str, dict] = {}  # Track all PM matches
        
        self._events: list[ScoreEvent] = []
        self._measurements: list[LatencyMeasurement] = []
        
        self._allsports_connected = False
        self._polymarket_connected = False
        self._allsports_last_msg = None
        self._polymarket_last_msg = None
        
        console.print(f"[cyan]Sport: {self.sport.upper()}[/cyan]")
        console.print(f"[dim]AllSports WS: {self.allsports_ws_url}[/dim]")
        console.print(f"[dim]Polymarket leagues: {self.polymarket_leagues}[/dim]\n")
    
    async def start(self, duration_seconds: int = 300):
        """Run latency test for specified duration"""
        self._running = True
        
        console.print(f"[cyan]Starting latency test for {duration_seconds} seconds...[/cyan]\n")
        
        tasks = [
            asyncio.create_task(self._connect_allsports()),
            asyncio.create_task(self._connect_polymarket()),
            asyncio.create_task(self._display_status(duration_seconds)),
        ]
        
        await asyncio.sleep(duration_seconds)
        
        self._running = False
        for task in tasks:
            task.cancel()
        
        self._print_results()
    
    async def _connect_allsports(self):
        """Connect to AllSportsAPI WebSocket"""
        url = f"{self.allsports_ws_url}?APIkey={self.api_key}"
        
        try:
            async with websockets.connect(url) as ws:
                self._allsports_ws = ws
                self._allsports_connected = True
                logger.info("Connected to AllSportsAPI WebSocket")
                
                async for message in ws:
                    if not self._running:
                        break
                    
                    self._allsports_last_msg = datetime.utcnow()
                    
                    try:
                        data = json.loads(message)
                        await self._process_allsports(data)
                    except json.JSONDecodeError:
                        pass
                        
        except Exception as e:
            logger.error(f"AllSportsAPI WebSocket error: {e}")
            self._allsports_connected = False
    
    async def _connect_polymarket(self):
        """Connect to Polymarket Sports WebSocket"""
        try:
            async with websockets.connect(self.POLYMARKET_WS) as ws:
                self._polymarket_ws = ws
                self._polymarket_connected = True
                logger.info("Connected to Polymarket Sports WebSocket")
                
                async for message in ws:
                    if not self._running:
                        break
                    
                    if message == "ping":
                        await ws.send("pong")
                        continue
                    
                    self._polymarket_last_msg = datetime.utcnow()
                    
                    try:
                        data = json.loads(message)
                        await self._process_polymarket(data)
                    except json.JSONDecodeError:
                        pass
                        
        except Exception as e:
            logger.error(f"Polymarket WebSocket error: {e}")
            self._polymarket_connected = False
    
    async def _process_allsports(self, matches: list):
        """Process AllSportsAPI messages"""
        if not isinstance(matches, list):
            return
        
        for match in matches:
            match_id = str(match.get("event_key", ""))
            home_team = match.get("event_home_team", "")
            away_team = match.get("event_away_team", "")
            
            score_str = match.get("event_final_result", "0 - 0")
            home_score, away_score = self._parse_score(score_str)
            
            key = self._normalize_match_key(home_team, away_team)
            old_score = self._allsports_scores.get(key)
            
            if old_score and (home_score, away_score) != old_score:
                event = ScoreEvent(
                    source="allsports",
                    timestamp=datetime.utcnow(),
                    match_id=match_id,
                    home_team=home_team,
                    away_team=away_team,
                    home_score=home_score,
                    away_score=away_score,
                    raw_data=match
                )
                self._events.append(event)
                self._record_event(event)
                
                icon = "🏀" if self.sport in ["nba", "basketball"] else "⚽"
                console.print(f"[green]{icon} AllSports:[/green] {home_team} {home_score}-{away_score} {away_team} @ {event.timestamp.strftime('%H:%M:%S.%f')[:-3]}")
            
            self._allsports_scores[key] = (home_score, away_score)
    
    async def _process_polymarket(self, data: dict):
        """Process Polymarket Sports messages"""
        if not isinstance(data, dict):
            return
        
        league = data.get("leagueAbbreviation", "")
        home_team = data.get("homeTeam", "")
        away_team = data.get("awayTeam", "")
        score_str = data.get("score", "0-0")
        status = data.get("status", "")
        slug = data.get("slug", "")
        
        # Track all Polymarket matches for display
        if slug and league:
            if slug not in self._polymarket_matches:
                self._polymarket_matches[slug] = {
                    "league": league,
                    "home": home_team,
                    "away": away_team,
                    "score": score_str,
                    "status": status
                }
                console.print(f"[dim]📺 Polymarket match: [{league}] {home_team} vs {away_team} ({score_str}) - {status}[/dim]")
        
        # Only process target sport leagues for latency comparison
        if league not in self.polymarket_leagues:
            return
        
        parts = score_str.split("-")
        if len(parts) >= 2:
            try:
                home_score = int(parts[0])
                away_score = int(parts[1])
            except ValueError:
                return
        else:
            return
        
        key = self._normalize_match_key(home_team, away_team)
        old_score = self._polymarket_scores.get(key)
        
        # Record event if score changed OR if this is first time seeing this match
        if old_score is None or (home_score, away_score) != old_score:
            event = ScoreEvent(
                source="polymarket",
                timestamp=datetime.utcnow(),
                match_id=slug,
                home_team=home_team,
                away_team=away_team,
                home_score=home_score,
                away_score=away_score,
                raw_data=data
            )
            
            if old_score is not None:  # Only count as event if score actually changed
                self._events.append(event)
                self._record_event(event)
                console.print(f"[blue]📊 Polymarket GOAL:[/blue] {home_team} {home_score}-{away_score} {away_team} @ {event.timestamp.strftime('%H:%M:%S.%f')[:-3]}")
            else:
                console.print(f"[cyan]⚽ Polymarket Soccer:[/cyan] {home_team} {home_score}-{away_score} {away_team} ({status})")
        
        self._polymarket_scores[key] = (home_score, away_score)
    
    def _record_event(self, event: ScoreEvent):
        """Try to match event with existing measurement or create new one"""
        key = self._normalize_match_key(event.home_team, event.away_team)
        score = f"{event.home_score}-{event.away_score}"
        
        for m in reversed(self._measurements[-20:]):
            m_key = self._normalize_match_key(
                m.match_description.split(" vs ")[0] if " vs " in m.match_description else "",
                m.match_description.split(" vs ")[1] if " vs " in m.match_description else ""
            )
            
            if m_key == key:
                if event.source == "allsports" and m.allsports_time is None:
                    m.allsports_time = event.timestamp
                    m.allsports_score = score
                    return
                elif event.source == "polymarket" and m.polymarket_time is None:
                    m.polymarket_time = event.timestamp
                    m.polymarket_score = score
                    return
        
        measurement = LatencyMeasurement(
            match_description=f"{event.home_team} vs {event.away_team}"
        )
        
        if event.source == "allsports":
            measurement.allsports_time = event.timestamp
            measurement.allsports_score = score
        else:
            measurement.polymarket_time = event.timestamp
            measurement.polymarket_score = score
        
        self._measurements.append(measurement)
    
    def _normalize_match_key(self, home: str, away: str) -> str:
        """Normalize team names for matching"""
        def clean(name: str) -> str:
            name = name.lower()
            name = re.sub(r'[^a-z0-9]', '', name)
            replacements = {
                'fc': '', 'sc': '', 'cf': '', 'united': 'utd', 'city': '',
                'sporting': '', 'real': '', 'atletico': 'atl', 'athletic': 'ath'
            }
            for old, new in replacements.items():
                name = name.replace(old, new)
            return name[:8]
        
        h, a = clean(home), clean(away)
        return f"{min(h,a)}_{max(h,a)}"
    
    def _parse_score(self, score_str: str) -> tuple[int, int]:
        """Parse score string like '2 - 1'"""
        try:
            parts = score_str.replace(" ", "").split("-")
            return int(parts[0]), int(parts[1])
        except (ValueError, IndexError):
            return 0, 0
    
    async def _display_status(self, duration: int):
        """Display live status"""
        start = datetime.utcnow()
        
        while self._running:
            await asyncio.sleep(5)
            
            elapsed = (datetime.utcnow() - start).total_seconds()
            remaining = max(0, duration - elapsed)
            
            console.print(f"[dim]--- Status: {int(elapsed)}s elapsed, {int(remaining)}s remaining | "
                         f"AllSports: {'✓' if self._allsports_connected else '✗'} | "
                         f"Polymarket: {'✓' if self._polymarket_connected else '✗'} | "
                         f"Events: {len(self._events)} ---[/dim]")
    
    def _print_results(self):
        """Print final results"""
        console.print("\n" + "=" * 70)
        console.print("[bold cyan]LATENCY TEST RESULTS[/bold cyan]")
        console.print("=" * 70)
        
        console.print(f"\nTotal events captured: {len(self._events)}")
        console.print(f"- AllSportsAPI: {len([e for e in self._events if e.source == 'allsports'])}")
        console.print(f"- Polymarket: {len([e for e in self._events if e.source == 'polymarket'])}")
        
        matched = [m for m in self._measurements if m.latency_ms is not None]
        
        if matched:
            console.print(f"\n[green]Matched score changes: {len(matched)}[/green]\n")
            
            table = Table(title="Latency Measurements")
            table.add_column("Match", style="cyan")
            table.add_column("AllSports", style="green")
            table.add_column("Polymarket", style="blue")
            table.add_column("Latency", style="yellow", justify="right")
            table.add_column("First", justify="center")
            
            for m in matched:
                latency_str = f"{m.latency_ms:+.0f}ms" if m.latency_ms else "N/A"
                first_color = "green" if m.who_first == "AllSports" else "blue" if m.who_first == "Polymarket" else "white"
                
                table.add_row(
                    m.match_description[:25],
                    m.allsports_score,
                    m.polymarket_score,
                    latency_str,
                    f"[{first_color}]{m.who_first}[/{first_color}]"
                )
            
            console.print(table)
            
            latencies = [m.latency_ms for m in matched if m.latency_ms is not None]
            if latencies:
                avg_latency = sum(latencies) / len(latencies)
                allsports_first = len([l for l in latencies if l > 0])
                polymarket_first = len([l for l in latencies if l < 0])
                
                console.print(f"\n[bold]Summary:[/bold]")
                console.print(f"  Average latency: {avg_latency:+.0f}ms (positive = AllSports faster)")
                console.print(f"  AllSports first: {allsports_first} times")
                console.print(f"  Polymarket first: {polymarket_first} times")
                
                if avg_latency > 0:
                    console.print(f"\n[green]✓ AllSportsAPI is faster by ~{abs(avg_latency):.0f}ms on average[/green]")
                elif avg_latency < 0:
                    console.print(f"\n[red]✗ Polymarket is faster by ~{abs(avg_latency):.0f}ms on average[/red]")
                    console.print("[yellow]⚠ Arbitrage may not be profitable with this data source[/yellow]")
        else:
            console.print("\n[yellow]No matched score changes between sources.[/yellow]")
            console.print("This could mean:")
            console.print("  - No soccer matches on Polymarket right now")
            console.print("  - Team names don't match between sources")
            console.print("  - No goals scored during the test period")
        
        unmatched_allsports = [m for m in self._measurements if m.allsports_time and not m.polymarket_time]
        unmatched_polymarket = [m for m in self._measurements if m.polymarket_time and not m.allsports_time]
        
        if unmatched_allsports or unmatched_polymarket:
            console.print(f"\n[dim]Unmatched events: {len(unmatched_allsports)} AllSports, {len(unmatched_polymarket)} Polymarket[/dim]")
        
        # Show all Polymarket matches seen
        if self._polymarket_matches:
            console.print(f"\n[cyan]Polymarket matches seen during test ({len(self._polymarket_matches)}):[/cyan]")
            
            soccer_matches = {k: v for k, v in self._polymarket_matches.items() if v['league'] in ['soccer', 'mls']}
            other_matches = {k: v for k, v in self._polymarket_matches.items() if v['league'] not in ['soccer', 'mls']}
            
            if soccer_matches:
                console.print(f"\n[green]Soccer/MLS matches ({len(soccer_matches)}):[/green]")
                for slug, m in soccer_matches.items():
                    console.print(f"  {m['home']} vs {m['away']} ({m['score']}) - {m['status']}")
            
            if other_matches:
                leagues = {}
                for slug, m in other_matches.items():
                    league = m['league']
                    if league not in leagues:
                        leagues[league] = 0
                    leagues[league] += 1
                console.print(f"\n[dim]Other sports: {', '.join(f'{k}: {v}' for k, v in leagues.items())}[/dim]")


async def main():
    """Run latency test"""
    import os
    from pathlib import Path
    from dotenv import load_dotenv
    
    load_dotenv(Path(__file__).parent.parent / ".env")
    
    api_key = os.getenv("DATA_PROVIDER_API_KEY")
    if not api_key:
        console.print("[red]Error: DATA_PROVIDER_API_KEY not set in .env[/red]")
        return
    
    tester = LatencyTester(api_key)
    await tester.start(duration_seconds=120)


if __name__ == "__main__":
    asyncio.run(main())
