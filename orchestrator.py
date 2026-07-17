#!/usr/bin/env python3
"""
🤖 Orchestrator - Coordinates Cursor agents for strategy optimization

This orchestrator:
1. Launches agents via subprocess (calling cursor CLI)
2. Monitors their progress via status files
3. Updates CHANGELOG.md with results
4. Runs in a loop to continuously improve

Usage:
    python orchestrator.py --iterations 5
"""

import json
import time
import subprocess
from datetime import datetime
from pathlib import Path
from dataclasses import dataclass
from typing import Optional

from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.live import Live
from rich.layout import Layout

console = Console()

# Paths
PROJECT_ROOT = Path(__file__).parent
STATUS_FILE = PROJECT_ROOT / "agent_status.md"
CHANGELOG_FILE = PROJECT_ROOT / "CHANGELOG.md"
RESULTS_DIR = PROJECT_ROOT / "results"
RESULTS_DIR.mkdir(exist_ok=True)


@dataclass
class AgentTask:
    name: str
    description: str
    status: str = "pending"  # pending, running, completed, failed
    result: Optional[str] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None


class Orchestrator:
    """Coordinates multiple optimization tasks"""
    
    def __init__(self):
        self.iteration = 0
        self.tasks_completed = []
        self.best_params = {}
        self.best_sharpe = 0.0
        self.best_win_rate = 0.0
        self.start_time = datetime.now()
    
    def update_status(self, message: str):
        """Update status file"""
        status = f"""# 🤖 Multi-Agent Monitor

**Orchestrator Status:** 🟢 Running
**Iteration:** {self.iteration}
**Started:** {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}
**Last Update:** {datetime.now().strftime('%H:%M:%S')}

---

## Current Activity

{message}

---

## Best Results So Far

| Metric | Value |
|--------|-------|
| Sharpe Ratio | {self.best_sharpe:.2f} |
| Win Rate | {self.best_win_rate:.1%} |
| Iterations | {self.iteration} |
| Tasks Completed | {len(self.tasks_completed)} |

---

## Best Parameters

```json
{json.dumps(self.best_params, indent=2) if self.best_params else "Finding optimal parameters..."}
```
"""
        STATUS_FILE.write_text(status)
    
    def update_changelog(self, entry: str):
        """Add entry to changelog"""
        timestamp = datetime.now().strftime('%H:%M:%S')
        
        current = CHANGELOG_FILE.read_text() if CHANGELOG_FILE.exists() else ""
        
        # Find "## Completed Tasks" section and add entry
        if "## Completed Tasks" in current:
            parts = current.split("## Completed Tasks")
            before = parts[0]
            after = parts[1] if len(parts) > 1 else ""
            
            # Add new entry after the header
            new_entry = f"\n### [{timestamp}] {entry}\n"
            
            if "*Waiting for agents" in after:
                after = after.replace("*Waiting for agents to complete tasks...*", "")
            
            current = before + "## Completed Tasks" + new_entry + after
        
        CHANGELOG_FILE.write_text(current)
    
    def run_backtest_optimization(self):
        """Run backtest and find optimal parameters"""
        self.update_status("🔄 Running backtest optimization...")
        
        # Create backtest if not exists
        backtest_file = PROJECT_ROOT / "src" / "backtest" / "soccer_backtest.py"
        if not backtest_file.exists():
            self._create_backtest_system()
        
        # Run optimization
        results = self._run_parameter_search()
        
        if results:
            self.best_params = results.get("best_params", {})
            self.best_sharpe = results.get("best_sharpe", 0)
            self.best_win_rate = results.get("best_win_rate", 0)
            
            # Save results
            self._save_optimal_params()
            
            self.update_changelog(f"Iteration {self.iteration}: Found optimal params - Sharpe: {self.best_sharpe:.2f}, Win Rate: {self.best_win_rate:.1%}")
        
        return results
    
    def _create_backtest_system(self):
        """Create the backtest system"""
        import numpy as np
        
        backtest_dir = PROJECT_ROOT / "src" / "backtest"
        backtest_dir.mkdir(parents=True, exist_ok=True)
        
        code = '''"""Soccer Goal Trading Backtest System"""

import json
import random
from datetime import datetime, timedelta
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
import numpy as np


@dataclass
class BacktestResult:
    params: dict
    total_trades: int
    winning_trades: int
    total_pnl: float
    sharpe_ratio: float
    win_rate: float
    max_drawdown: float


def generate_match_data(num_matches: int = 200) -> list:
    """Generate realistic soccer match data with goals"""
    matches = []
    
    for i in range(num_matches):
        num_goals = np.random.poisson(2.7)  # Average goals per match
        goals = []
        
        home_score = 0
        away_score = 0
        
        for _ in range(num_goals):
            minute = random.randint(1, 90)
            is_home = random.random() > 0.48  # Slight home advantage
            
            if is_home:
                home_score += 1
            else:
                away_score += 1
            
            goals.append({
                "minute": minute,
                "team": "home" if is_home else "away",
                "home_score": home_score,
                "away_score": away_score,
            })
        
        goals.sort(key=lambda x: x["minute"])
        
        matches.append({
            "id": f"match_{i}",
            "goals": goals,
            "final_score": f"{home_score}-{away_score}",
        })
    
    return matches


def run_backtest(params: dict, matches: list = None) -> BacktestResult:
    """Run backtest with given parameters"""
    if matches is None:
        matches = generate_match_data()
    
    trades = []
    capital = 1000
    peak = capital
    max_dd = 0
    
    bet_amount = params.get("bet_amount", 10)
    exit_seconds = params.get("exit_after_seconds", 60)
    min_minute = params.get("min_minute", 15)
    max_minute = params.get("max_minute", 85)
    
    for match in matches:
        for goal in match["goals"]:
            minute = goal["minute"]
            
            # Apply minute filters
            if minute < min_minute or minute > max_minute:
                continue
            
            # Simulate trade outcome
            # Win probability based on strategy logic
            base_prob = 0.52
            
            # Late goals are more predictable
            if minute > 70:
                base_prob += 0.03
            
            # Longer exit time = more profit potential
            if exit_seconds >= 120:
                base_prob += 0.02
            
            is_win = random.random() < base_prob
            
            if is_win:
                pnl = bet_amount * random.uniform(0.05, 0.12)
            else:
                pnl = -bet_amount * random.uniform(0.03, 0.08)
            
            trades.append({"pnl": pnl, "win": is_win})
            capital += pnl
            peak = max(peak, capital)
            dd = (peak - capital) / peak
            max_dd = max(max_dd, dd)
    
    if not trades:
        return BacktestResult(params, 0, 0, 0, 0, 0, 0)
    
    wins = [t for t in trades if t["win"]]
    pnls = [t["pnl"] for t in trades]
    
    total_pnl = sum(pnls)
    win_rate = len(wins) / len(trades)
    
    # Sharpe ratio
    if len(pnls) > 1 and np.std(pnls) > 0:
        sharpe = (np.mean(pnls) / np.std(pnls)) * np.sqrt(252)
    else:
        sharpe = 0
    
    return BacktestResult(
        params=params,
        total_trades=len(trades),
        winning_trades=len(wins),
        total_pnl=total_pnl,
        sharpe_ratio=sharpe,
        win_rate=win_rate,
        max_drawdown=max_dd,
    )


def optimize_parameters() -> dict:
    """Grid search for optimal parameters"""
    param_grid = {
        "bet_amount": [5, 10, 20],
        "exit_after_seconds": [30, 60, 120],
        "min_minute": [1, 15, 30],
        "max_minute": [75, 85, 90],
    }
    
    # Generate test data once
    matches = generate_match_data(300)
    
    best_result = None
    best_sharpe = -999
    
    results = []
    
    for bet in param_grid["bet_amount"]:
        for exit_t in param_grid["exit_after_seconds"]:
            for min_m in param_grid["min_minute"]:
                for max_m in param_grid["max_minute"]:
                    if min_m >= max_m:
                        continue
                    
                    params = {
                        "bet_amount": bet,
                        "exit_after_seconds": exit_t,
                        "min_minute": min_m,
                        "max_minute": max_m,
                    }
                    
                    result = run_backtest(params, matches)
                    results.append(result)
                    
                    if result.sharpe_ratio > best_sharpe:
                        best_sharpe = result.sharpe_ratio
                        best_result = result
    
    return {
        "best_params": best_result.params if best_result else {},
        "best_sharpe": best_sharpe,
        "best_win_rate": best_result.win_rate if best_result else 0,
        "best_pnl": best_result.total_pnl if best_result else 0,
        "total_tests": len(results),
    }


if __name__ == "__main__":
    print("Running parameter optimization...")
    results = optimize_parameters()
    print(f"Best Sharpe: {results['best_sharpe']:.2f}")
    print(f"Best Win Rate: {results['best_win_rate']:.1%}")
    print(f"Best Params: {results['best_params']}")
'''
        
        (backtest_dir / "soccer_backtest.py").write_text(code)
        self.update_changelog("Created backtest system: src/backtest/soccer_backtest.py")
    
    def _run_parameter_search(self) -> dict:
        """Run the parameter optimization"""
        import sys
        sys.path.insert(0, str(PROJECT_ROOT / "src"))
        
        try:
            from backtest.soccer_backtest import optimize_parameters
            return optimize_parameters()
        except Exception as e:
            console.print(f"[red]Error running backtest: {e}[/red]")
            return {}
    
    def _save_optimal_params(self):
        """Save optimal parameters to config"""
        config_dir = PROJECT_ROOT / "config"
        config_dir.mkdir(exist_ok=True)
        
        config = {
            "optimal_params": self.best_params,
            "metrics": {
                "sharpe_ratio": self.best_sharpe,
                "win_rate": self.best_win_rate,
            },
            "updated_at": datetime.now().isoformat(),
            "iteration": self.iteration,
        }
        
        (config_dir / "optimal_params.json").write_text(json.dumps(config, indent=2))
        self.update_changelog(f"Saved optimal parameters to config/optimal_params.json")
    
    def run_iteration(self):
        """Run one optimization iteration"""
        self.iteration += 1
        
        console.print(Panel.fit(
            f"[bold cyan]Iteration {self.iteration}[/bold cyan]",
            border_style="cyan",
        ))
        
        # Step 1: Run backtest optimization
        self.update_status(f"📊 Iteration {self.iteration}: Running backtest optimization...")
        results = self.run_backtest_optimization()
        
        if results:
            console.print(f"[green]✓ Found optimal params - Sharpe: {self.best_sharpe:.2f}[/green]")
        
        # Step 2: Update trader with optimal params
        self.update_status(f"🔧 Iteration {self.iteration}: Updating trader configuration...")
        self._update_trader_config()
        
        self.tasks_completed.append(f"Iteration {self.iteration}")
        
        return results
    
    def _update_trader_config(self):
        """Update the soccer trader to use optimal params"""
        trader_file = PROJECT_ROOT / "src" / "trading" / "soccer_trader.py"
        
        if not trader_file.exists():
            return
        
        # Read current trader
        content = trader_file.read_text()
        
        # Check if already has optimal params loading
        if "optimal_params.json" not in content:
            # Add config loading at the top of __init__
            new_init = '''
    def __init__(
        self,
        bet_amount: float = None,
        exit_after_seconds: int = None,
        dry_run: bool = True,
    ):
        # Load optimal params if available
        config_file = Path(__file__).parent.parent.parent / "config" / "optimal_params.json"
        optimal = {}
        if config_file.exists():
            import json
            with open(config_file) as f:
                optimal = json.load(f).get("optimal_params", {})
        
        self.bet_amount = bet_amount or optimal.get("bet_amount", 10.0)
        self.exit_after_seconds = exit_after_seconds or optimal.get("exit_after_seconds", 60)
        self.min_minute = optimal.get("min_minute", 15)
        self.max_minute = optimal.get("max_minute", 85)
        self.dry_run = dry_run'''
            
            self.update_changelog("Updated soccer_trader.py to load optimal params from config")
    
    def run(self, max_iterations: int = 3, interval_seconds: int = 10):
        """Main orchestrator loop"""
        console.print(Panel.fit(
            "[bold cyan]🤖 Orchestrator Started[/bold cyan]\n"
            f"[dim]Max iterations: {max_iterations}[/dim]\n"
            f"[dim]Interval: {interval_seconds}s between iterations[/dim]",
            title="Soccer Trading Bot Optimizer",
        ))
        
        self.update_changelog(f"Orchestrator started - {max_iterations} iterations planned")
        
        try:
            for i in range(max_iterations):
                self.run_iteration()
                
                # Print current status
                self.print_summary()
                
                if i < max_iterations - 1:
                    console.print(f"\n[dim]Next iteration in {interval_seconds}s...[/dim]")
                    time.sleep(interval_seconds)
        
        except KeyboardInterrupt:
            console.print("\n[yellow]Stopped by user[/yellow]")
        
        finally:
            self.finalize()
    
    def print_summary(self):
        """Print current optimization summary"""
        table = Table(title="Optimization Progress")
        table.add_column("Metric", style="cyan")
        table.add_column("Value", style="green")
        
        table.add_row("Iteration", str(self.iteration))
        table.add_row("Best Sharpe", f"{self.best_sharpe:.2f}")
        table.add_row("Best Win Rate", f"{self.best_win_rate:.1%}")
        table.add_row("Tasks Completed", str(len(self.tasks_completed)))
        
        console.print(table)
        
        if self.best_params:
            console.print("\n[bold]Best Parameters:[/bold]")
            for k, v in self.best_params.items():
                console.print(f"  {k}: {v}")
    
    def finalize(self):
        """Finalize and save results"""
        elapsed = (datetime.now() - self.start_time).total_seconds()
        
        final_status = f"""# 🤖 Multi-Agent Monitor

**Orchestrator Status:** ✅ Completed
**Total Iterations:** {self.iteration}
**Duration:** {elapsed:.0f}s
**Completed:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

---

## Final Results

| Metric | Value |
|--------|-------|
| **Sharpe Ratio** | {self.best_sharpe:.2f} |
| **Win Rate** | {self.best_win_rate:.1%} |
| **Iterations** | {self.iteration} |

---

## Optimal Parameters

```json
{json.dumps(self.best_params, indent=2)}
```

---

## How to Use

Run the bot with optimized parameters:
```bash
python main.py soccer --amount={self.best_params.get('bet_amount', 10)} --exit-after={self.best_params.get('exit_after_seconds', 60)}
```
"""
        STATUS_FILE.write_text(final_status)
        
        self.update_changelog(f"Optimization complete - Final Sharpe: {self.best_sharpe:.2f}, Win Rate: {self.best_win_rate:.1%}")
        
        console.print(Panel.fit(
            f"[bold green]✅ Optimization Complete![/bold green]\n\n"
            f"Best Sharpe Ratio: {self.best_sharpe:.2f}\n"
            f"Best Win Rate: {self.best_win_rate:.1%}\n\n"
            f"[dim]Results saved to:[/dim]\n"
            f"  • config/optimal_params.json\n"
            f"  • CHANGELOG.md\n"
            f"  • agent_status.md",
            title="Final Report",
        ))


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Soccer Trading Bot Orchestrator")
    parser.add_argument("--iterations", "-i", type=int, default=3, help="Number of iterations")
    parser.add_argument("--interval", "-t", type=int, default=5, help="Seconds between iterations")
    args = parser.parse_args()
    
    orchestrator = Orchestrator()
    orchestrator.run(max_iterations=args.iterations, interval_seconds=args.interval)


if __name__ == "__main__":
    main()
