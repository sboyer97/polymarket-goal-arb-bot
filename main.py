#!/usr/bin/env python3
"""
Polymarket Soccer Arbitrage Bot
Main entry point for running the trading bot or backtests
"""

import asyncio
import sys
from datetime import datetime, timedelta
from pathlib import Path

import click
from dotenv import load_dotenv
from loguru import logger
from rich.console import Console
from rich.table import Table

load_dotenv(Path(__file__).parent / ".env")

from config.settings import settings
from src.polymarket.client import PolymarketClient
from src.data_provider.sportradar import SportradarProvider
from src.data_provider.allsportsapi import AllSportsAPIProvider
from src.trading.engine import TradingEngine
from src.trading.strategy import GoalArbitrageStrategy
from src.backtest.simulator import BacktestSimulator
from src.backtest.data_loader import HistoricalDataLoader

console = Console()


def setup_logging():
    """Configure logging"""
    logger.remove()
    logger.add(
        sys.stderr,
        format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        level=settings.log_level
    )
    logger.add(
        "logs/bot_{time}.log",
        rotation="1 day",
        retention="7 days",
        level="DEBUG"
    )


@click.group()
def cli():
    """Polymarket Soccer Arbitrage Bot"""
    setup_logging()


@cli.command()
@click.option("--provider", type=click.Choice(["allsportsapi"]), default="allsportsapi", help="Data provider to use")
@click.option("--dry-run/--live", default=True, help="Run in dry-run mode (no real trades)")
def run(provider: str, dry_run: bool):
    """Run the trading bot"""
    
    if dry_run:
        console.print("[yellow]Running in DRY-RUN mode - no real trades will be placed[/yellow]")
    else:
        console.print("[red]Running in LIVE mode - real trades will be placed![/red]")
        if not click.confirm("Are you sure you want to continue?"):
            return
    
    settings.trading.dry_run = dry_run
    
    async def _run():
        polymarket = PolymarketClient()
        
        data_provider = AllSportsAPIProvider()
        console.print("[blue]Using AllSportsAPI data provider (WebSocket)[/blue]")
        
        strategy = GoalArbitrageStrategy()
        engine = TradingEngine(polymarket, data_provider, strategy)
        
        console.print("[green]Starting trading engine...[/green]")
        
        try:
            await engine.start()
        except KeyboardInterrupt:
            console.print("\n[yellow]Shutting down...[/yellow]")
        finally:
            await engine.stop()
            
            stats = engine.get_stats()
            
            table = Table(title="Trading Session Summary")
            table.add_column("Metric", style="cyan")
            table.add_column("Value", style="green")
            
            for key, value in stats.items():
                if isinstance(value, float):
                    table.add_row(key.replace("_", " ").title(), f"{value:.2f}")
                else:
                    table.add_row(key.replace("_", " ").title(), str(value))
            
            console.print(table)
    
    asyncio.run(_run())


@cli.command()
@click.option("--days", default=30, help="Number of days to backtest")
@click.option("--generate-data", is_flag=True, help="Generate sample data first")
@click.option("--num-samples", default=50, help="Number of sample matches to generate")
@click.option("--plot/--no-plot", default=True, help="Show results plot")
def backtest(days: int, generate_data: bool, num_samples: int, plot: bool):
    """Run a backtest on historical data"""
    
    console.print(f"[blue]Running backtest for past {days} days[/blue]")
    
    if generate_data:
        console.print(f"[yellow]Generating {num_samples} sample matches...[/yellow]")
        loader = HistoricalDataLoader()
        loader.generate_sample_data(num_samples)
    
    end_date = datetime.utcnow()
    start_date = end_date - timedelta(days=days)
    
    simulator = BacktestSimulator(
        strategy=GoalArbitrageStrategy(),
        initial_capital=10000.0,
        latency_ms=100.0
    )
    
    result = simulator.run(start_date, end_date)
    
    simulator.print_summary(result)
    
    if plot and result.num_trades > 0:
        simulator.plot_results(result, save_path="backtest_results.png")


@cli.command()
def markets():
    """List available soccer markets on Polymarket"""
    
    async def _list_markets():
        client = PolymarketClient()
        await client.initialize()
        
        markets = await client.search_soccer_markets()
        
        if not markets:
            console.print("[yellow]No soccer markets found[/yellow]")
            return
        
        table = Table(title="Soccer Markets on Polymarket")
        table.add_column("ID", style="dim")
        table.add_column("Question", style="cyan", max_width=50)
        table.add_column("Active", style="green")
        
        for market in markets[:20]:
            table.add_row(
                market.get("id", "")[:8],
                market.get("question", ""),
                "Yes" if market.get("active") else "No"
            )
        
        console.print(table)
        console.print(f"\n[dim]Showing {min(20, len(markets))} of {len(markets)} markets[/dim]")
        
        await client.close()
    
    asyncio.run(_list_markets())


@cli.command()
@click.argument("match_id")
@click.argument("market_id")
def link(match_id: str, market_id: str):
    """Link a data provider match to a Polymarket market"""
    console.print(f"[green]Linking match {match_id} to market {market_id}[/green]")
    console.print("[yellow]Note: This link will only persist during a bot run session[/yellow]")


@cli.command()
def live():
    """List currently live matches from AllSportsAPI"""
    
    async def _live():
        provider = AllSportsAPIProvider()
        
        console.print("[blue]Connecting to AllSportsAPI...[/blue]")
        
        if not await provider.connect():
            console.print("[red]Failed to connect. Check your API key in .env[/red]")
            return
        
        console.print("[green]Connected! Fetching live matches...[/green]\n")
        
        matches = await provider.get_live_matches()
        
        if not matches:
            console.print("[yellow]No live matches at the moment[/yellow]")
            await provider.disconnect()
            return
        
        table = Table(title=f"Live Matches ({len(matches)} total)")
        table.add_column("ID", style="dim")
        table.add_column("League", style="blue")
        table.add_column("Home", style="cyan")
        table.add_column("Score", style="green", justify="center")
        table.add_column("Away", style="cyan")
        table.add_column("Min", justify="right")
        
        for match in matches[:30]:
            table.add_row(
                match.match_id,
                match.league[:20] if len(match.league) > 20 else match.league,
                match.home_team[:15] if len(match.home_team) > 15 else match.home_team,
                f"{match.home_score} - {match.away_score}",
                match.away_team[:15] if len(match.away_team) > 15 else match.away_team,
                str(match.current_minute) + "'"
            )
        
        console.print(table)
        
        if len(matches) > 30:
            console.print(f"\n[dim]Showing 30 of {len(matches)} matches[/dim]")
        
        await provider.disconnect()
    
    asyncio.run(_live())


@cli.command()
def leagues():
    """List available leagues from AllSportsAPI"""
    
    async def _leagues():
        provider = AllSportsAPIProvider()
        
        if not await provider.connect():
            console.print("[red]Failed to connect. Check your API key in .env[/red]")
            return
        
        leagues_data = await provider.get_leagues()
        
        if not leagues_data:
            console.print("[yellow]No leagues found[/yellow]")
            await provider.disconnect()
            return
        
        table = Table(title=f"Available Leagues ({len(leagues_data)} total)")
        table.add_column("ID", style="dim")
        table.add_column("Country", style="blue")
        table.add_column("League", style="cyan")
        
        for league in leagues_data[:50]:
            table.add_row(
                str(league.get("league_key", "")),
                league.get("country_name", ""),
                league.get("league_name", "")
            )
        
        console.print(table)
        
        if len(leagues_data) > 50:
            console.print(f"\n[dim]Showing 50 of {len(leagues_data)} leagues[/dim]")
        
        await provider.disconnect()
    
    asyncio.run(_leagues())


@cli.command()
@click.option("--duration", default=60, help="Duration in seconds to monitor")
@click.option("--league", default=None, help="Filter by league ID")
def monitor(duration: int, league: str):
    """Monitor live goals in real-time via WebSocket"""
    
    async def _monitor():
        provider = AllSportsAPIProvider()
        
        console.print("[blue]Connecting to AllSportsAPI WebSocket...[/blue]")
        
        if not await provider.connect():
            console.print("[red]Failed to connect. Check your API key in .env[/red]")
            return
        
        if league:
            await provider.subscribe_league(league)
            console.print(f"[green]Subscribed to league {league}[/green]")
        
        console.print(f"[green]Monitoring goals for {duration} seconds...[/green]")
        console.print("[dim]Press Ctrl+C to stop[/dim]\n")
        
        goal_count = 0
        start_time = datetime.utcnow()
        
        try:
            async for goal in provider.stream_events():
                goal_count += 1
                elapsed = (datetime.utcnow() - start_time).total_seconds()
                
                console.print(f"[bold green]⚽ GOAL![/bold green] "
                             f"[cyan]{goal.minute}'[/cyan] - "
                             f"Score: [yellow]{goal.home_score} - {goal.away_score}[/yellow] - "
                             f"Scorer: {goal.scorer or 'Unknown'} - "
                             f"Match: {goal.match_id}")
                
                if elapsed >= duration:
                    break
                    
        except KeyboardInterrupt:
            pass
        
        console.print(f"\n[cyan]Monitoring complete. Goals detected: {goal_count}[/cyan]")
        await provider.disconnect()
    
    asyncio.run(_monitor())


@cli.command()
@click.option("--duration", default=120, help="Test duration in seconds")
@click.option("--sport", default="soccer", type=click.Choice(["soccer", "nba"]), help="Sport to test")
def latency(duration: int, sport: str):
    """Test latency between AllSportsAPI and Polymarket WebSocket"""
    
    async def _latency():
        from src.latency_test import LatencyTester
        
        api_key = settings.data_provider.api_key
        if not api_key:
            console.print("[red]Error: DATA_PROVIDER_API_KEY not set in .env[/red]")
            return
        
        tester = LatencyTester(api_key, sport=sport)
        await tester.start(duration_seconds=duration)
    
    asyncio.run(_latency())


@cli.command()
@click.option("--duration", default=180, help="Test duration in seconds")
@click.option("--poll-interval", default=2.0, help="Polling interval for AllSportsAPI (seconds)")
def latency_nba(duration: int, poll_interval: float):
    """Test NBA latency: AllSportsAPI REST polling vs Polymarket WebSocket"""
    
    async def _latency():
        from src.latency_test_polling import NBALatencyTester
        
        api_key = settings.data_provider.api_key
        if not api_key:
            console.print("[red]Error: DATA_PROVIDER_API_KEY not set in .env[/red]")
            return
        
        tester = NBALatencyTester(api_key, poll_interval=poll_interval)
        await tester.start(duration_seconds=duration)
    
    asyncio.run(_latency())


@cli.command()
@click.option("--amount", default=10.0, help="Bet amount in USD")
@click.option("--exit-after", default=60, help="Exit position after X seconds")
@click.option("--live", is_flag=True, help="Enable live trading (default: dry run)")
def soccer(amount: float, exit_after: int, live: bool):
    """Run the soccer goal trader - bet on scoring team"""
    
    async def _run():
        from src.trading.soccer_trader import SoccerGoalTrader
        
        trader = SoccerGoalTrader(
            bet_amount=amount,
            exit_after_seconds=exit_after,
            dry_run=not live,
        )
        
        async def status_loop():
            while True:
                await asyncio.sleep(120)
                trader.print_status()
        
        try:
            asyncio.create_task(status_loop())
            await trader.start()
        except KeyboardInterrupt:
            pass
        finally:
            await trader.stop()
            trader.print_status()
    
    asyncio.run(_run())


@cli.command()
def polymarket_live():
    """Show live matches from Polymarket Sports WebSocket"""
    
    async def _pm_live():
        import websockets
        
        console.print("[blue]Connecting to Polymarket Sports WebSocket...[/blue]")
        
        matches = {}
        
        try:
            async with websockets.connect("wss://sports-api.polymarket.com/ws") as ws:
                console.print("[green]Connected! Receiving data for 10 seconds...[/green]\n")
                
                start = datetime.utcnow()
                
                while (datetime.utcnow() - start).total_seconds() < 10:
                    try:
                        message = await asyncio.wait_for(ws.recv(), timeout=1.0)
                        
                        if message == "ping":
                            await ws.send("pong")
                            continue
                        
                        data = json.loads(message)
                        slug = data.get("slug", "")
                        if slug:
                            matches[slug] = data
                            
                    except asyncio.TimeoutError:
                        continue
                
                table = Table(title=f"Polymarket Sports ({len(matches)} events)")
                table.add_column("League", style="blue")
                table.add_column("Slug", style="dim", max_width=30)
                table.add_column("Home", style="cyan")
                table.add_column("Score", style="green", justify="center")
                table.add_column("Away", style="cyan")
                table.add_column("Status")
                table.add_column("Period")
                
                for slug, m in sorted(matches.items(), key=lambda x: x[1].get("leagueAbbreviation", "")):
                    table.add_row(
                        m.get("leagueAbbreviation", ""),
                        slug[:30],
                        str(m.get("homeTeam", ""))[:12],
                        m.get("score", ""),
                        str(m.get("awayTeam", ""))[:12],
                        m.get("status", ""),
                        m.get("period", "")
                    )
                
                console.print(table)
                
                soccer_count = len([m for m in matches.values() if m.get("leagueAbbreviation") in ["soccer", "mls"]])
                console.print(f"\n[cyan]Soccer/MLS matches: {soccer_count}[/cyan]")
                
        except Exception as e:
            console.print(f"[red]Error: {e}[/red]")
    
    import json
    asyncio.run(_pm_live())


if __name__ == "__main__":
    cli()
