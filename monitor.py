#!/usr/bin/env python3
"""
Monitor the live system - shows status and recent activity.
Pour monitorer les données du serveur sans relancer le live : après rsync pull,
lancer avec --data-dir server_logs/server_data_live
"""

import argparse
import json
import os
import sys
import time
from pathlib import Path
from datetime import datetime

from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.live import Live

console = Console()

PROJECT_ROOT = Path(__file__).parent
WORK_LOG = PROJECT_ROOT / "WORK_LOG.md"


def parse_args():
    p = argparse.ArgumentParser(description="Monitor live system status and recent goals")
    p.add_argument(
        "--data-dir",
        type=Path,
        default=None,
        help="Dossier des données (défaut: data/live). Utiliser server_logs/server_data_live pour monitorer les données serveur après rsync.",
    )
    return p.parse_args()


def get_latest_csv(data_dir: Path):
    """Get the most recent CSV file"""
    csvs = list(data_dir.glob("live_goals_*.csv"))
    if not csvs:
        return None
    return max(csvs, key=lambda x: x.stat().st_mtime)


def get_price_curves_csv(data_dir: Path):
    """Get price curves CSV if exists"""
    curves = list(data_dir.glob("price_curves_*.csv"))
    if not curves:
        return None
    return max(curves, key=lambda x: x.stat().st_mtime)


def count_csv_rows(csv_file: Path) -> int:
    """Count rows in CSV (excluding header)"""
    if not csv_file or not csv_file.exists():
        return 0
    with open(csv_file) as f:
        return sum(1 for _ in f) - 1


def get_last_rows(csv_file: Path, n: int = 5) -> list:
    """Get last N rows from CSV"""
    if not csv_file or not csv_file.exists():
        return []
    
    with open(csv_file) as f:
        lines = f.readlines()
    
    if len(lines) <= 1:
        return []
    
    return [l.strip().split(",") for l in lines[-n:]]


def display_status(data_dir: Path, is_remote: bool = False):
    """Display current status. is_remote=True when reading server data locally (skip process check)."""
    os.system('clear' if os.name != 'nt' else 'cls')
    
    title = "[bold cyan]🤖 Live System Monitor[/bold cyan]"
    if is_remote:
        title += " [dim](données serveur)[/dim]"
    console.print(Panel.fit(title, border_style="cyan"))
    
    # Check if process is running (only meaningful when monitoring local data)
    if is_remote:
        status = "[dim]Données serveur (process N/A en local)[/dim]"
    else:
        try:
            import subprocess
            result = subprocess.run(
                ["pgrep", "-f", "live_system.py"],
                capture_output=True, text=True
            )
            is_running = bool(result.stdout.strip())
        except (OSError, FileNotFoundError, subprocess.SubprocessError):
            is_running = False
        status = "[green]🟢 RUNNING[/green]" if is_running else "[red]🔴 NOT RUNNING[/red]"
    console.print(f"\nSystem Status: {status}")
    
    # Liste des matchs qu'on suit (ceux avec data Sportmonks/WebSocket: period ou elapsed)
    matches_file = data_dir / "current_matches.json"
    if matches_file.exists():
        try:
            data = json.loads(matches_file.read_text())
            updated = data.get("updated_at", "")[:19].replace("T", " ")
            all_matches = data.get("matches", [])
            matches_list = [m for m in all_matches if m.get("period") or m.get("elapsed")]
            console.print(f"\n[bold]⚽ Matchs suivis[/bold] ({len(matches_list)}) — MAJ: {updated}")
            if matches_list:
                table_m = Table(show_header=True, header_style="bold cyan")
                table_m.add_column("Ligue", style="blue", width=8)
                table_m.add_column("Domicile", style="cyan", width=14)
                table_m.add_column("Score", justify="center", style="green", width=6)
                table_m.add_column("Extérieur", style="cyan", width=14)
                table_m.add_column("Période", style="dim", width=12)
                for m in matches_list:
                    period = m.get("period", "")
                    elapsed = m.get("elapsed", "")
                    period_str = f"{period} {elapsed}".strip() if elapsed else period
                    table_m.add_row(
                        (m.get("league") or "").upper(),
                        (m.get("home") or "")[:14],
                        m.get("score", ""),
                        (m.get("away") or "")[:14],
                        period_str,
                    )
                console.print(table_m)
            else:
                console.print("   [dim]Aucun match suivi (en attente données WebSocket/Sportmonks)[/dim]")
        except (json.JSONDecodeError, OSError):
            console.print("   [dim]Fichier matchs indisponible[/dim]")
    else:
        console.print("\n[bold]⚽ Matchs soccer[/bold] — [dim]en attente (lance live_system.py)[/dim]")
    
    # Get CSV info
    csv_file = get_latest_csv(data_dir)
    if csv_file:
        rows = count_csv_rows(csv_file)
        console.print(f"CSV File: {csv_file.name}")
        console.print(f"Goals Recorded: {rows}")
        
        # Show last goals
        last_rows = get_last_rows(csv_file, 5)
        if last_rows:
            console.print("\n[bold]Recent Goals:[/bold]")
            table = Table()
            table.add_column("Time", style="dim")
            table.add_column("Match")
            table.add_column("Min", style="cyan", width=6)  # minute in match
            table.add_column("Score")
            table.add_column("PnL", justify="right")
            table.add_column("Total", justify="right")
            
            for row in last_rows:
                if len(row) >= 13:
                    time_str = row[0].split("T")[1][:8] if "T" in row[0] else row[0]
                    match = f"{row[3][:8]} vs {row[4][:8]}"
                    minute = row[6] if len(row) > 6 else ""  # match_period (e.g. 67')
                    score = row[8]
                    pnl = row[11] if len(row) > 11 else ""
                    total = row[12] if len(row) > 12 else ""
                    try:
                        pnl_val = float(pnl) if pnl else 0
                    except (ValueError, TypeError):
                        pnl_val = 0
                    pnl_color = "green" if pnl_val > 0 else "red" if pnl_val < 0 else "dim"
                    pnl_str = f"${pnl}" if pnl else "[dim]—[/dim]"
                    total_str = f"${total}" if total else "[dim]—[/dim]"
                    table.add_row(
                        time_str,
                        match,
                        minute,
                        score,
                        f"[{pnl_color}]{pnl_str}[/{pnl_color}]",
                        total_str
                    )
            
            console.print(table)
    else:
        console.print("[yellow]No data file found yet[/yellow]")
    
    # Price curves (stabilization analysis)
    curves_file = get_price_curves_csv(data_dir)
    if curves_file and curves_file.exists():
        with open(curves_file) as f:
            lines = f.readlines()
        if len(lines) > 1:
            console.print("\n[bold]📈 Price Analysis (dernier but)[/bold]")
            last = lines[-1].strip().split(",")
            if len(last) >= 16:
                console.print(f"  Stabilisation: {last[12] or '?'}s | Meilleur exit: T+{last[13] or '?'}s")
                profit = last[14] if len(last) > 14 else ""
                console.print(f"  Profit T+0→60: {profit}% | Marché: {last[15] if len(last) > 15 else '?'}")
                if profit and profit != "":
                    try:
                        p = float(profit)
                        verdict = "[green]✅ Assez de temps pour trader![/green]" if p > 0 else "[red]❌ Pas assez de temps[/red]"
                        console.print(f"  {verdict}")
                    except (ValueError, TypeError):
                        pass
    
    # Show WORK_LOG excerpt
    if WORK_LOG.exists():
        content = WORK_LOG.read_text()
        
        # Find latest iteration
        if "## Live Analysis - Iteration" in content:
            parts = content.split("## Live Analysis - Iteration")
            if len(parts) > 1:
                latest = parts[-1].split("---")[0]
                console.print("\n[bold]Latest Analysis:[/bold]")
                console.print(f"[dim]{latest[:500]}...[/dim]")
    
    console.print(f"\n[dim]Updated: {datetime.now().strftime('%H:%M:%S')} | Press Ctrl+C to exit[/dim]")


def main():
    """Main monitor loop"""
    args = parse_args()
    data_dir = args.data_dir or (PROJECT_ROOT / "data" / "live")
    if not data_dir.is_absolute():
        data_dir = PROJECT_ROOT / data_dir
    if not data_dir.exists():
        console.print(f"[red]Dossier inexistant: {data_dir}[/red]")
        sys.exit(1)
    is_remote = "server_data_live" in str(data_dir) or "server_logs" in str(data_dir)
    console.print(f"[cyan]Données: {data_dir}[/cyan] | [cyan]Rafraîchissement: 10s[/cyan]")
    try:
        while True:
            display_status(data_dir, is_remote=is_remote)
            time.sleep(10)
    except KeyboardInterrupt:
        console.print("\n[yellow]Monitor stopped[/yellow]")


if __name__ == "__main__":
    main()
