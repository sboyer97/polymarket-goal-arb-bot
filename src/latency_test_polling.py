#!/usr/bin/env python3
"""
Latency Test with REST Polling for Basketball

Compare AllSportsAPI REST polling vs Polymarket WebSocket for NBA.
"""

import asyncio
import json
from datetime import datetime
from dataclasses import dataclass, field
from typing import Optional
import re

import httpx
import websockets
from loguru import logger
from rich.console import Console
from rich.table import Table

console = Console()


@dataclass
class ScoreEvent:
    source: str
    timestamp: datetime
    match_id: str
    home_team: str
    away_team: str
    home_score: int
    away_score: int


@dataclass
class LatencyMeasurement:
    match_key: str
    match_description: str
    allsports_time: Optional[datetime] = None
    polymarket_time: Optional[datetime] = None
    allsports_score: str = ""
    polymarket_score: str = ""
    
    @property
    def latency_ms(self) -> Optional[float]:
        if self.allsports_time and self.polymarket_time:
            return (self.polymarket_time - self.allsports_time).total_seconds() * 1000
        return None
    
    @property
    def who_first(self) -> str:
        if self.latency_ms is None:
            return "?"
        return "AllSports" if self.latency_ms > 0 else "Polymarket" if self.latency_ms < 0 else "Tie"


class NBALatencyTester:
    """Test NBA latency: AllSportsAPI REST polling vs Polymarket WebSocket"""
    
    ALLSPORTS_REST = "https://apiv2.allsportsapi.com/basketball/"
    POLYMARKET_WS = "wss://sports-api.polymarket.com/ws"
    
    def __init__(self, api_key: str, poll_interval: float = 2.0):
        self.api_key = api_key
        self.poll_interval = poll_interval
        
        self._http_client: Optional[httpx.AsyncClient] = None
        self._running = False
        
        self._allsports_scores: dict[str, tuple[int, int, datetime]] = {}
        self._polymarket_scores: dict[str, tuple[int, int, datetime]] = {}
        
        self._measurements: list[LatencyMeasurement] = []
        self._events: list[ScoreEvent] = []
        
        console.print(f"[cyan]NBA Latency Test[/cyan]")
        console.print(f"[dim]AllSports: REST polling every {poll_interval}s[/dim]")
        console.print(f"[dim]Polymarket: WebSocket push[/dim]\n")
    
    async def start(self, duration_seconds: int = 120):
        """Run the test"""
        self._running = True
        self._http_client = httpx.AsyncClient(timeout=10.0)
        
        console.print(f"[green]Starting {duration_seconds}s test...[/green]\n")
        
        tasks = [
            asyncio.create_task(self._poll_allsports()),
            asyncio.create_task(self._listen_polymarket()),
            asyncio.create_task(self._status_display(duration_seconds)),
        ]
        
        await asyncio.sleep(duration_seconds)
        self._running = False
        
        for task in tasks:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        
        await self._http_client.aclose()
        self._print_results()
    
    async def _poll_allsports(self):
        """Poll AllSportsAPI REST endpoint"""
        console.print("[blue]Starting AllSportsAPI polling...[/blue]")
        
        while self._running:
            try:
                response = await self._http_client.get(
                    self.ALLSPORTS_REST,
                    params={
                        "met": "Livescore",
                        "APIkey": self.api_key,
                        "leagueId": "766"  # NBA
                    },
                    follow_redirects=True
                )
                
                if response.status_code == 200:
                    data = response.json()
                    if data.get("success") == 1:
                        await self._process_allsports(data.get("result", []))
                
            except Exception as e:
                logger.error(f"AllSports poll error: {e}")
            
            await asyncio.sleep(self.poll_interval)
    
    async def _listen_polymarket(self):
        """Listen to Polymarket WebSocket"""
        console.print("[blue]Connecting to Polymarket WebSocket...[/blue]")
        
        try:
            async with websockets.connect(self.POLYMARKET_WS) as ws:
                console.print("[green]Polymarket connected![/green]")
                
                async for message in ws:
                    if not self._running:
                        break
                    
                    if message == "ping":
                        await ws.send("pong")
                        continue
                    
                    try:
                        data = json.loads(message)
                        await self._process_polymarket(data)
                    except json.JSONDecodeError:
                        pass
                        
        except Exception as e:
            logger.error(f"Polymarket WebSocket error: {e}")
    
    async def _process_allsports(self, matches: list):
        """Process AllSportsAPI matches"""
        now = datetime.utcnow()
        
        for match in matches:
            # event_live == "1" means match is currently live
            if str(match.get("event_live", "0")) != "1":
                continue
            
            home = match.get("event_home_team", "")
            away = match.get("event_away_team", "")
            score_str = match.get("event_final_result", "0 - 0")
            
            home_score, away_score = self._parse_score(score_str)
            key = self._normalize_key(home, away)
            
            old = self._allsports_scores.get(key)
            if old is None:
                # First time seeing this match
                console.print(f"[dim]Tracking: {home} vs {away} ({score_str})[/dim]")
            elif (home_score, away_score) != (old[0], old[1]):
                # Score changed!
                event = ScoreEvent("allsports", now, str(match.get("event_key", "")),
                                   home, away, home_score, away_score)
                self._events.append(event)
                self._record_event(event, key)
                
                console.print(f"[green]🏀 AllSports:[/green] {home} {home_score}-{away_score} {away} @ {now.strftime('%H:%M:%S.%f')[:-3]}")
            
            self._allsports_scores[key] = (home_score, away_score, now)
    
    async def _process_polymarket(self, data: dict):
        """Process Polymarket message"""
        league = data.get("leagueAbbreviation", "")
        if league not in ["nba"]:
            return
        
        now = datetime.utcnow()
        home = data.get("homeTeam", "")
        away = data.get("awayTeam", "")
        score_str = data.get("score", "0-0")
        
        parts = score_str.split("-")
        if len(parts) < 2:
            return
        
        try:
            home_score, away_score = int(parts[0]), int(parts[1])
        except ValueError:
            return
        
        key = self._normalize_key(home, away)
        old = self._polymarket_scores.get(key)
        
        if old and (home_score, away_score) != (old[0], old[1]):
            event = ScoreEvent("polymarket", now, data.get("slug", ""),
                               home, away, home_score, away_score)
            self._events.append(event)
            self._record_event(event, key)
            
            console.print(f"[blue]📊 Polymarket:[/blue] {home} {home_score}-{away_score} {away} @ {now.strftime('%H:%M:%S.%f')[:-3]}")
        
        self._polymarket_scores[key] = (home_score, away_score, now)
    
    def _record_event(self, event: ScoreEvent, key: str):
        """Record event and try to match with other source"""
        score = f"{event.home_score}-{event.away_score}"
        score_rev = f"{event.away_score}-{event.home_score}"
        total = event.home_score + event.away_score
        
        # Find measurement for this score change (check both normal and reversed scores)
        for m in reversed(self._measurements[-50:]):
            if m.match_key != key:
                continue
            
            # Match by total score (handles home/away swap)
            m_total = sum(int(x) for x in (m.allsports_score or m.polymarket_score or "0-0").split("-"))
            if m_total != total:
                continue
                
            if event.source == "allsports" and m.allsports_time is None:
                m.allsports_time = event.timestamp
                m.allsports_score = score
                return
            elif event.source == "polymarket" and m.polymarket_time is None:
                m.polymarket_time = event.timestamp
                m.polymarket_score = score
                return
        
        m = LatencyMeasurement(
            match_key=key,
            match_description=f"{event.home_team} vs {event.away_team}"
        )
        if event.source == "allsports":
            m.allsports_time = event.timestamp
            m.allsports_score = score
        else:
            m.polymarket_time = event.timestamp
            m.polymarket_score = score
        
        self._measurements.append(m)
    
    def _normalize_key(self, home: str, away: str) -> str:
        """Normalize team names for matching"""
        abbrevs = {
            "boston celtics": "bos", "brooklyn nets": "bkn", "new york knicks": "nyk",
            "philadelphia 76ers": "phi", "toronto raptors": "tor", "chicago bulls": "chi",
            "cleveland cavaliers": "cle", "detroit pistons": "det", "indiana pacers": "ind",
            "milwaukee bucks": "mil", "atlanta hawks": "atl", "charlotte hornets": "cha",
            "miami heat": "mia", "orlando magic": "orl", "washington wizards": "was",
            "denver nuggets": "den", "minnesota timberwolves": "min", "oklahoma city thunder": "okc",
            "portland trail blazers": "por", "utah jazz": "uta", "golden state warriors": "gs",
            "la clippers": "lac", "los angeles lakers": "lal", "phoenix suns": "phx",
            "sacramento kings": "sac", "dallas mavericks": "dal", "houston rockets": "hou",
            "memphis grizzlies": "mem", "new orleans pelicans": "nop", "san antonio spurs": "sas"
        }
        
        def to_abbrev(name: str) -> str:
            name_lower = name.lower().strip()
            for full, abbr in abbrevs.items():
                if full in name_lower or name_lower in full:
                    return abbr
            # Already abbreviated
            return name_lower.replace(" ", "")[:3]
        
        h, a = to_abbrev(home), to_abbrev(away)
        return f"{min(h,a)}_{max(h,a)}"
    
    def _parse_score(self, score_str: str) -> tuple[int, int]:
        try:
            parts = score_str.replace(" ", "").split("-")
            return int(parts[0]), int(parts[1])
        except:
            return 0, 0
    
    async def _status_display(self, duration: int):
        """Display status every 10 seconds"""
        start = datetime.utcnow()
        while self._running:
            await asyncio.sleep(10)
            elapsed = (datetime.utcnow() - start).total_seconds()
            remaining = max(0, duration - elapsed)
            matched = len([m for m in self._measurements if m.latency_ms is not None])
            console.print(f"[dim]--- {int(elapsed)}s / {duration}s | Events: {len(self._events)} | Matched: {matched} ---[/dim]")
    
    def _print_results(self):
        """Print final results"""
        console.print("\n" + "=" * 70)
        console.print("[bold cyan]NBA LATENCY TEST RESULTS[/bold cyan]")
        console.print("=" * 70)
        
        console.print(f"\nTotal score changes: {len(self._events)}")
        console.print(f"  - AllSportsAPI: {len([e for e in self._events if e.source == 'allsports'])}")
        console.print(f"  - Polymarket: {len([e for e in self._events if e.source == 'polymarket'])}")
        
        matched = [m for m in self._measurements if m.latency_ms is not None]
        
        if matched:
            console.print(f"\n[green]Matched score changes: {len(matched)}[/green]\n")
            
            table = Table(title="Latency Measurements")
            table.add_column("Match", style="cyan", max_width=25)
            table.add_column("Score", justify="center")
            table.add_column("Latency", style="yellow", justify="right")
            table.add_column("First", justify="center")
            
            for m in matched[-20:]:
                latency = f"{m.latency_ms:+.0f}ms"
                color = "green" if m.who_first == "AllSports" else "blue"
                table.add_row(
                    m.match_description[:25],
                    m.allsports_score or m.polymarket_score,
                    latency,
                    f"[{color}]{m.who_first}[/{color}]"
                )
            
            console.print(table)
            
            latencies = [m.latency_ms for m in matched]
            avg = sum(latencies) / len(latencies)
            allsports_wins = len([l for l in latencies if l > 0])
            polymarket_wins = len([l for l in latencies if l < 0])
            
            console.print(f"\n[bold]Summary:[/bold]")
            console.print(f"  Average latency: {avg:+.0f}ms")
            console.print(f"  AllSports first: {allsports_wins} ({100*allsports_wins/len(latencies):.0f}%)")
            console.print(f"  Polymarket first: {polymarket_wins} ({100*polymarket_wins/len(latencies):.0f}%)")
            
            console.print(f"\n[bold]Note:[/bold] AllSports uses {self.poll_interval}s polling, adding ~{self.poll_interval*500:.0f}ms avg latency")
            
            if avg > 0:
                console.print(f"\n[green]✓ AllSports REST is faster by ~{abs(avg):.0f}ms on average[/green]")
            else:
                console.print(f"\n[yellow]⚠ Polymarket WebSocket is faster by ~{abs(avg):.0f}ms[/yellow]")
                console.print(f"[dim]But AllSports polling adds {self.poll_interval*500:.0f}ms. True source latency unknown.[/dim]")
        else:
            console.print("\n[yellow]No matched score changes.[/yellow]")


async def main():
    import os
    from pathlib import Path
    from dotenv import load_dotenv
    
    load_dotenv(Path(__file__).parent.parent / ".env")
    
    api_key = os.getenv("DATA_PROVIDER_API_KEY")
    if not api_key:
        console.print("[red]Error: DATA_PROVIDER_API_KEY not set[/red]")
        return
    
    tester = NBALatencyTester(api_key, poll_interval=2.0)
    await tester.start(duration_seconds=180)


if __name__ == "__main__":
    asyncio.run(main())
