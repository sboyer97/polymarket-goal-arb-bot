"""Code Agent - Optimizes and improves the codebase"""

import asyncio
import json
from datetime import datetime
from pathlib import Path

from loguru import logger

from .base import BaseAgent


class CodeAgent(BaseAgent):
    """
    Code Agent responsibilities:
    - Review code for improvements
    - Apply optimizations based on backtest findings
    - Update configuration files with optimal parameters
    - Track code changes
    """
    
    def __init__(self):
        super().__init__(
            name="code_agent",
            description="Optimizes code and applies improvements"
        )
        self.project_root = Path(".")
        self.improvements_made: list[dict] = []
    
    async def work_on_task(self):
        """Execute current task"""
        if not self.current_task:
            return
        
        task_name = self.current_task.name
        
        if task_name == "optimize_code":
            result = await self.optimize_code()
            await self.complete_task(result)
        else:
            self.status = self.status.IDLE
            self.current_task = None
    
    async def optimize_code(self) -> dict:
        """Apply code optimizations based on findings"""
        await self.report("Analyzing codebase for optimizations...")
        
        improvements = []
        
        # 1. Update config with optimal parameters
        config_update = await self._update_optimal_config()
        if config_update:
            improvements.append(config_update)
        
        # 2. Generate optimized strategy file
        strategy_update = await self._generate_optimized_strategy()
        if strategy_update:
            improvements.append(strategy_update)
        
        # 3. Create performance tips
        perf_tips = await self._generate_performance_tips()
        if perf_tips:
            improvements.append(perf_tips)
        
        self.improvements_made.extend(improvements)
        
        # Update orchestrator
        await self.send_message(
            "orchestrator",
            {
                "action": "update_state",
                "code_improvements": len(self.improvements_made),
            },
            "info"
        )
        
        await self.report(f"Applied {len(improvements)} code improvements")
        
        return {
            "improvements": len(improvements),
            "details": improvements,
        }
    
    async def _update_optimal_config(self) -> dict:
        """Update config with optimal parameters from analysis"""
        analysis_file = Path("results/analysis.json")
        if not analysis_file.exists():
            return None
        
        with open(analysis_file) as f:
            analysis = json.load(f)
        
        best_config = analysis.get("best_config", {})
        if not best_config:
            return None
        
        params = best_config.get("params", {})
        
        # Create optimal config file
        optimal_config = {
            "strategy": {
                "bet_amount": params.get("bet_amount", 10),
                "exit_after_seconds": params.get("exit_after_seconds", 60),
                "min_minute": params.get("min_minute", 15),
                "max_minute": params.get("max_minute", 85),
                "odds_threshold": params.get("odds_threshold", 0.5),
            },
            "metrics": {
                "expected_sharpe": best_config.get("sharpe_ratio", 0),
                "expected_win_rate": best_config.get("win_rate", 0),
                "max_drawdown": best_config.get("max_drawdown", 0),
            },
            "updated_at": datetime.utcnow().isoformat(),
        }
        
        config_file = Path("config/optimal_params.json")
        config_file.parent.mkdir(exist_ok=True)
        
        with open(config_file, "w") as f:
            json.dump(optimal_config, f, indent=2)
        
        return {
            "type": "config_update",
            "file": str(config_file),
            "description": "Updated optimal parameters config",
        }
    
    async def _generate_optimized_strategy(self) -> dict:
        """Generate an optimized strategy file"""
        analysis_file = Path("results/analysis.json")
        if not analysis_file.exists():
            return None
        
        with open(analysis_file) as f:
            analysis = json.load(f)
        
        best_config = analysis.get("best_config", {})
        params = best_config.get("params", {})
        insights = analysis.get("insights", [])
        
        # Generate optimized strategy code
        strategy_code = f'''"""
Auto-generated Optimized Soccer Strategy
Generated: {datetime.utcnow().isoformat()}

Based on {analysis.get("total_tests", 0)} backtests.
Expected Sharpe Ratio: {best_config.get("sharpe_ratio", 0):.2f}
Expected Win Rate: {best_config.get("win_rate", 0):.1%}

Insights:
{chr(10).join(f"- {i}" for i in insights)}
"""

# Optimal Parameters
OPTIMAL_PARAMS = {{
    "bet_amount": {params.get("bet_amount", 10)},
    "exit_after_seconds": {params.get("exit_after_seconds", 60)},
    "min_minute": {params.get("min_minute", 15)},
    "max_minute": {params.get("max_minute", 85)},
    "odds_threshold": {params.get("odds_threshold", 0.5)},
}}


def should_bet(minute: int, current_odds: float) -> bool:
    """
    Determine if we should place a bet on a goal.
    
    Args:
        minute: Current match minute
        current_odds: Current odds for the scoring team
    
    Returns:
        True if we should bet
    """
    # Time filter
    if minute < OPTIMAL_PARAMS["min_minute"]:
        return False
    if minute > OPTIMAL_PARAMS["max_minute"]:
        return False
    
    # Odds filter
    if current_odds < OPTIMAL_PARAMS["odds_threshold"]:
        return False
    
    return True


def get_bet_amount(capital: float, confidence: float = 0.5) -> float:
    """
    Calculate bet amount based on Kelly criterion.
    
    Args:
        capital: Current capital
        confidence: Confidence level (0-1)
    
    Returns:
        Recommended bet amount
    """
    base_bet = OPTIMAL_PARAMS["bet_amount"]
    max_bet = capital * 0.05  # Never bet more than 5% of capital
    
    adjusted_bet = base_bet * confidence
    return min(adjusted_bet, max_bet)


def get_exit_time() -> int:
    """Get optimal exit time in seconds"""
    return OPTIMAL_PARAMS["exit_after_seconds"]
'''
        
        strategy_file = Path("src/trading/optimized_strategy.py")
        with open(strategy_file, "w") as f:
            f.write(strategy_code)
        
        return {
            "type": "strategy_generation",
            "file": str(strategy_file),
            "description": "Generated optimized strategy with best parameters",
        }
    
    async def _generate_performance_tips(self) -> dict:
        """Generate performance optimization tips"""
        tips = [
            {
                "area": "WebSocket Connection",
                "tip": "Use connection pooling and automatic reconnection",
                "priority": "high",
            },
            {
                "area": "Order Execution",
                "tip": "Pre-calculate order parameters to reduce latency",
                "priority": "high",
            },
            {
                "area": "Data Processing",
                "tip": "Use asyncio.gather for parallel API calls",
                "priority": "medium",
            },
            {
                "area": "Memory",
                "tip": "Limit historical data to last 100 events",
                "priority": "low",
            },
        ]
        
        tips_file = Path("results/performance_tips.json")
        with open(tips_file, "w") as f:
            json.dump(tips, f, indent=2)
        
        return {
            "type": "performance_tips",
            "file": str(tips_file),
            "description": f"Generated {len(tips)} performance tips",
        }
