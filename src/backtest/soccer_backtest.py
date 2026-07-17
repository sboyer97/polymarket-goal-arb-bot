"""Soccer Goal Trading Backtest System"""

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
