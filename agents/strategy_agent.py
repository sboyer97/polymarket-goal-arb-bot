"""Strategy Agent - Runs backtests and optimizes parameters"""

import asyncio
import json
import itertools
from datetime import datetime
from pathlib import Path
from dataclasses import dataclass
from typing import Optional

import numpy as np
from loguru import logger

from .base import BaseAgent


@dataclass
class BacktestResult:
    """Results from a single backtest run"""
    params: dict
    total_trades: int
    winning_trades: int
    losing_trades: int
    total_pnl: float
    max_drawdown: float
    sharpe_ratio: float
    win_rate: float
    avg_win: float
    avg_loss: float
    
    def to_dict(self) -> dict:
        return {
            "params": self.params,
            "total_trades": self.total_trades,
            "winning_trades": self.winning_trades,
            "total_pnl": self.total_pnl,
            "sharpe_ratio": self.sharpe_ratio,
            "win_rate": self.win_rate,
            "max_drawdown": self.max_drawdown,
        }


class StrategyAgent(BaseAgent):
    """
    Strategy Agent responsibilities:
    - Run backtests with different parameters
    - Grid search for optimal parameters
    - Analyze results and find best configuration
    - Report findings to orchestrator
    """
    
    def __init__(self):
        super().__init__(
            name="strategy_agent",
            description="Optimizes trading strategy parameters"
        )
        self.data_dir = Path("data")
        self.results_dir = Path("results")
        self.results_dir.mkdir(exist_ok=True)
        
        # Parameter search space
        self.param_grid = {
            "bet_amount": [5, 10, 20, 50],
            "exit_after_seconds": [30, 60, 120, 300],
            "min_minute": [1, 15, 30],  # Don't bet on goals before this minute
            "max_minute": [75, 85, 90],  # Don't bet on goals after this minute
            "odds_threshold": [0.3, 0.4, 0.5, 0.6],  # Min odds to bet
        }
        
        self.all_results: list[BacktestResult] = []
        self.best_result: Optional[BacktestResult] = None
    
    async def work_on_task(self):
        """Execute current task"""
        if not self.current_task:
            return
        
        task_name = self.current_task.name
        
        if task_name == "run_backtest":
            result = await self.run_single_backtest()
            await self.complete_task(result)
        
        elif task_name == "optimize_params":
            result = await self.optimize_parameters()
            await self.complete_task(result)
        
        elif task_name == "analyze_results":
            result = await self.analyze_results()
            await self.complete_task(result)
        
        else:
            self.status = self.status.IDLE
            self.current_task = None
    
    async def run_single_backtest(self, params: dict = None) -> BacktestResult:
        """Run a single backtest with given parameters"""
        if params is None:
            params = {
                "bet_amount": 10,
                "exit_after_seconds": 60,
                "min_minute": 15,
                "max_minute": 85,
                "odds_threshold": 0.5,
            }
        
        # Load match data
        data_file = self.data_dir / "matches.json"
        if not data_file.exists():
            return BacktestResult(params=params, total_trades=0, winning_trades=0,
                                  losing_trades=0, total_pnl=0, max_drawdown=0,
                                  sharpe_ratio=0, win_rate=0, avg_win=0, avg_loss=0)
        
        with open(data_file) as f:
            matches = json.load(f)
        
        # Simulate trades
        trades = []
        capital = 1000
        peak_capital = capital
        max_drawdown = 0
        
        for match in matches:
            for goal in match.get("goals", []):
                minute = goal["minute"]
                
                # Apply filters
                if minute < params["min_minute"]:
                    continue
                if minute > params["max_minute"]:
                    continue
                
                # Simulate the trade
                # Win probability based on minute (late goals less predictable)
                base_win_prob = 0.55
                minute_factor = 1 - (minute / 90) * 0.2
                win_prob = base_win_prob * minute_factor
                
                # Add some randomness based on parameters
                if params["exit_after_seconds"] > 120:
                    win_prob += 0.05  # Longer hold = more time for market to adjust
                
                is_win = np.random.random() < win_prob
                
                if is_win:
                    # Win: typically 5-15% profit
                    profit_pct = np.random.uniform(0.05, 0.15)
                    pnl = params["bet_amount"] * profit_pct
                else:
                    # Loss: typically 3-10% loss
                    loss_pct = np.random.uniform(0.03, 0.10)
                    pnl = -params["bet_amount"] * loss_pct
                
                trades.append({
                    "match": match["id"],
                    "minute": minute,
                    "pnl": pnl,
                    "is_win": is_win,
                })
                
                capital += pnl
                peak_capital = max(peak_capital, capital)
                drawdown = (peak_capital - capital) / peak_capital
                max_drawdown = max(max_drawdown, drawdown)
        
        # Calculate metrics
        if not trades:
            return BacktestResult(params=params, total_trades=0, winning_trades=0,
                                  losing_trades=0, total_pnl=0, max_drawdown=0,
                                  sharpe_ratio=0, win_rate=0, avg_win=0, avg_loss=0)
        
        pnls = [t["pnl"] for t in trades]
        wins = [t for t in trades if t["is_win"]]
        losses = [t for t in trades if not t["is_win"]]
        
        total_pnl = sum(pnls)
        win_rate = len(wins) / len(trades) if trades else 0
        avg_win = np.mean([t["pnl"] for t in wins]) if wins else 0
        avg_loss = np.mean([abs(t["pnl"]) for t in losses]) if losses else 0
        
        # Sharpe ratio (annualized)
        if len(pnls) > 1 and np.std(pnls) > 0:
            daily_return = np.mean(pnls) / params["bet_amount"]
            daily_std = np.std(pnls) / params["bet_amount"]
            sharpe = (daily_return / daily_std) * np.sqrt(252)
        else:
            sharpe = 0
        
        result = BacktestResult(
            params=params,
            total_trades=len(trades),
            winning_trades=len(wins),
            losing_trades=len(losses),
            total_pnl=total_pnl,
            max_drawdown=max_drawdown,
            sharpe_ratio=sharpe,
            win_rate=win_rate,
            avg_win=avg_win,
            avg_loss=avg_loss,
        )
        
        return result
    
    async def optimize_parameters(self) -> dict:
        """Run grid search to find optimal parameters"""
        await self.report("Starting parameter optimization (grid search)...")
        
        # Generate all parameter combinations
        keys = list(self.param_grid.keys())
        values = list(self.param_grid.values())
        combinations = list(itertools.product(*values))
        
        # Limit to manageable number
        max_combos = 50
        if len(combinations) > max_combos:
            combinations = list(np.random.choice(
                range(len(combinations)), 
                size=max_combos, 
                replace=False
            ))
            combinations = [list(itertools.product(*values))[i] for i in combinations]
        
        await self.report(f"Testing {len(combinations)} parameter combinations...")
        
        best_sharpe = -float("inf")
        best_params = None
        
        for i, combo in enumerate(combinations):
            params = dict(zip(keys, combo))
            result = await self.run_single_backtest(params)
            self.all_results.append(result)
            
            if result.sharpe_ratio > best_sharpe:
                best_sharpe = result.sharpe_ratio
                best_params = params
                self.best_result = result
            
            # Progress update every 10 tests
            if (i + 1) % 10 == 0:
                await self.report(f"Progress: {i + 1}/{len(combinations)} tested")
            
            await asyncio.sleep(0.05)  # Small delay
        
        # Update orchestrator state
        await self.send_message(
            "orchestrator",
            {
                "action": "update_state",
                "best_params": best_params,
                "best_sharpe": best_sharpe,
                "best_win_rate": self.best_result.win_rate if self.best_result else 0,
                "total_backtests": len(self.all_results),
            },
            "info"
        )
        
        await self.report(f"Optimization complete. Best Sharpe: {best_sharpe:.2f}")
        
        return {
            "best_params": best_params,
            "best_sharpe": best_sharpe,
            "total_tested": len(combinations),
        }
    
    async def analyze_results(self) -> dict:
        """Analyze backtest results and provide insights"""
        await self.report("Analyzing backtest results...")
        
        if not self.all_results:
            return {"error": "No results to analyze"}
        
        # Find patterns in successful configurations
        good_results = [r for r in self.all_results if r.sharpe_ratio > 1.0]
        
        insights = []
        
        if good_results:
            # Analyze which parameters tend to work best
            avg_bet = np.mean([r.params["bet_amount"] for r in good_results])
            avg_exit = np.mean([r.params["exit_after_seconds"] for r in good_results])
            avg_min = np.mean([r.params["min_minute"] for r in good_results])
            avg_max = np.mean([r.params["max_minute"] for r in good_results])
            
            insights.append(f"Optimal bet amount range: ${avg_bet:.0f}")
            insights.append(f"Optimal exit time: {avg_exit:.0f}s")
            insights.append(f"Best minute range: {avg_min:.0f}'-{avg_max:.0f}'")
            
            # Risk analysis
            avg_drawdown = np.mean([r.max_drawdown for r in good_results])
            insights.append(f"Average max drawdown: {avg_drawdown:.1%}")
        
        # Save analysis
        analysis = {
            "total_tests": len(self.all_results),
            "profitable_configs": len([r for r in self.all_results if r.total_pnl > 0]),
            "best_config": self.best_result.to_dict() if self.best_result else None,
            "insights": insights,
        }
        
        analysis_file = self.results_dir / "analysis.json"
        with open(analysis_file, "w") as f:
            json.dump(analysis, f, indent=2)
        
        await self.report(f"Analysis complete. {len(insights)} insights generated.")
        
        return analysis
