import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Optional
from dataclasses import dataclass, field

import matplotlib.pyplot as plt
from loguru import logger

from src.backtest.data_loader import HistoricalDataLoader, HistoricalMatch
from src.trading.strategy import TradingStrategy, GoalArbitrageStrategy
from src.utils.models import GoalEvent, PolymarketMatch, MarketOutcome, OrderSide
from config.settings import settings


@dataclass
class BacktestTrade:
    """Record of a simulated trade"""
    match_id: str
    goal_minute: int
    entry_time: datetime
    exit_time: datetime
    entry_price: float
    exit_price: float
    size: float
    side: OrderSide
    outcome: str
    pnl: float
    edge_captured: float


@dataclass
class BacktestResult:
    """Complete backtest results"""
    start_date: datetime
    end_date: datetime
    num_matches: int
    num_goals: int
    num_trades: int
    trades: list[BacktestTrade]
    total_pnl: float
    win_rate: float
    avg_pnl_per_trade: float
    max_drawdown: float
    sharpe_ratio: float
    equity_curve: pd.DataFrame


class BacktestSimulator:
    """Simulate trading strategy on historical data"""
    
    def __init__(
        self, 
        strategy: Optional[TradingStrategy] = None,
        initial_capital: float = 10000.0,
        latency_ms: float = 100.0
    ):
        self.strategy = strategy or GoalArbitrageStrategy()
        self.initial_capital = initial_capital
        self.latency_ms = latency_ms
        self.data_loader = HistoricalDataLoader()
    
    def run(self, start_date: datetime, end_date: datetime) -> BacktestResult:
        """Run backtest on historical data"""
        logger.info(f"Running backtest from {start_date} to {end_date}")
        
        matches = self.data_loader.load_matches(start_date, end_date)
        
        if not matches:
            logger.warning("No historical matches found")
            return self._empty_result(start_date, end_date)
        
        trades: list[BacktestTrade] = []
        equity = self.initial_capital
        equity_history = [(start_date, equity)]
        
        total_goals = 0
        
        for match in matches:
            total_goals += len(match.goals)
            
            for goal in match.goals:
                trade = self._simulate_trade(match, goal, equity)
                
                if trade:
                    trades.append(trade)
                    equity += trade.pnl
                    equity_history.append((trade.exit_time, equity))
        
        equity_df = pd.DataFrame(equity_history, columns=["timestamp", "equity"])
        equity_df.set_index("timestamp", inplace=True)
        
        return self._compile_results(
            start_date, end_date, 
            len(matches), total_goals, 
            trades, equity_df
        )
    
    def _simulate_trade(
        self, 
        match: HistoricalMatch, 
        goal: GoalEvent,
        current_equity: float
    ) -> Optional[BacktestTrade]:
        """Simulate a trade triggered by a goal"""
        
        if match.price_history.empty:
            return None
        
        entry_time = goal.timestamp + timedelta(milliseconds=self.latency_ms)
        exit_time = entry_time + timedelta(seconds=settings.trading.exit_delay_seconds)
        
        entry_prices = self._get_prices_at_time(match.price_history, entry_time)
        exit_prices = self._get_prices_at_time(match.price_history, exit_time)
        
        if not entry_prices or not exit_prices:
            return None
        
        mock_market = self._create_mock_market(match, entry_prices)
        
        signal = self.strategy.analyze_goal(goal, mock_market, entry_prices)
        
        if not signal:
            return None
        
        outcome_map = {
            "Home Win": "home_win",
            "Away Win": "away_win", 
            "Draw": "draw",
            "Yes": "home_win" if goal.team.value == "home" else "away_win",
            "No": "away_win" if goal.team.value == "home" else "home_win"
        }
        
        outcome = None
        for out in mock_market.outcomes:
            if out.token_id == signal.token_id:
                outcome = out.outcome
                break
        
        if not outcome or outcome not in outcome_map:
            return None
        
        price_key = outcome_map.get(outcome, "home_win")
        entry_price = entry_prices.get(price_key, 0.5)
        exit_price = exit_prices.get(price_key, 0.5)
        
        slippage = settings.trading.max_slippage_percent / 100
        entry_price += slippage
        exit_price -= slippage
        
        size = min(signal.recommended_size, current_equity * 0.1)
        pnl = (exit_price - entry_price) * size
        
        return BacktestTrade(
            match_id=match.match_info.match_id,
            goal_minute=goal.minute,
            entry_time=entry_time,
            exit_time=exit_time,
            entry_price=entry_price,
            exit_price=exit_price,
            size=size,
            side=signal.side,
            outcome=outcome,
            pnl=pnl,
            edge_captured=exit_price - entry_price
        )
    
    def _get_prices_at_time(self, price_df: pd.DataFrame, timestamp: datetime) -> dict:
        """Get prices at a specific timestamp"""
        try:
            ts = pd.Timestamp(timestamp)
            idx = price_df.index.get_indexer([ts], method="nearest")[0]
            row = price_df.iloc[idx]
            return {
                "home_win": row["home_win"],
                "away_win": row["away_win"],
                "draw": row["draw"]
            }
        except Exception:
            return {}
    
    def _create_mock_market(self, match: HistoricalMatch, prices: dict) -> PolymarketMatch:
        """Create a mock market for strategy analysis"""
        outcomes = [
            MarketOutcome(token_id="home_win", outcome="Home Win", price=prices.get("home_win", 0.35)),
            MarketOutcome(token_id="away_win", outcome="Away Win", price=prices.get("away_win", 0.35)),
            MarketOutcome(token_id="draw", outcome="Draw", price=prices.get("draw", 0.30)),
        ]
        
        return PolymarketMatch(
            market_id=f"backtest_{match.match_info.match_id}",
            condition_id="",
            match_id=match.match_info.match_id,
            question=f"{match.match_info.home_team} vs {match.match_info.away_team}",
            home_team=match.match_info.home_team,
            away_team=match.match_info.away_team,
            outcomes=outcomes,
            end_date=match.match_info.start_time + timedelta(hours=2)
        )
    
    def _compile_results(
        self,
        start_date: datetime,
        end_date: datetime,
        num_matches: int,
        num_goals: int,
        trades: list[BacktestTrade],
        equity_df: pd.DataFrame
    ) -> BacktestResult:
        """Compile backtest statistics"""
        
        if not trades:
            return self._empty_result(start_date, end_date)
        
        total_pnl = sum(t.pnl for t in trades)
        winning_trades = [t for t in trades if t.pnl > 0]
        win_rate = len(winning_trades) / len(trades)
        avg_pnl = total_pnl / len(trades)
        
        equity_series = equity_df["equity"]
        rolling_max = equity_series.expanding().max()
        drawdowns = (equity_series - rolling_max) / rolling_max
        max_drawdown = abs(drawdowns.min())
        
        returns = equity_series.pct_change().dropna()
        if len(returns) > 1 and returns.std() > 0:
            sharpe = returns.mean() / returns.std() * np.sqrt(252)
        else:
            sharpe = 0.0
        
        return BacktestResult(
            start_date=start_date,
            end_date=end_date,
            num_matches=num_matches,
            num_goals=num_goals,
            num_trades=len(trades),
            trades=trades,
            total_pnl=total_pnl,
            win_rate=win_rate,
            avg_pnl_per_trade=avg_pnl,
            max_drawdown=max_drawdown,
            sharpe_ratio=sharpe,
            equity_curve=equity_df
        )
    
    def _empty_result(self, start_date: datetime, end_date: datetime) -> BacktestResult:
        """Return empty result when no data"""
        return BacktestResult(
            start_date=start_date,
            end_date=end_date,
            num_matches=0,
            num_goals=0,
            num_trades=0,
            trades=[],
            total_pnl=0,
            win_rate=0,
            avg_pnl_per_trade=0,
            max_drawdown=0,
            sharpe_ratio=0,
            equity_curve=pd.DataFrame()
        )
    
    def plot_results(self, result: BacktestResult, save_path: Optional[str] = None):
        """Plot backtest results"""
        fig, axes = plt.subplots(2, 2, figsize=(14, 10))
        
        if not result.equity_curve.empty:
            axes[0, 0].plot(result.equity_curve.index, result.equity_curve["equity"])
            axes[0, 0].set_title("Equity Curve")
            axes[0, 0].set_xlabel("Date")
            axes[0, 0].set_ylabel("Equity ($)")
            axes[0, 0].grid(True)
        
        pnls = [t.pnl for t in result.trades]
        if pnls:
            colors = ["green" if p > 0 else "red" for p in pnls]
            axes[0, 1].bar(range(len(pnls)), pnls, color=colors)
            axes[0, 1].set_title("Trade PnL Distribution")
            axes[0, 1].set_xlabel("Trade #")
            axes[0, 1].set_ylabel("PnL ($)")
            axes[0, 1].axhline(y=0, color="black", linestyle="-", linewidth=0.5)
        
        if pnls:
            axes[1, 0].hist(pnls, bins=20, edgecolor="black")
            axes[1, 0].set_title("PnL Histogram")
            axes[1, 0].set_xlabel("PnL ($)")
            axes[1, 0].set_ylabel("Frequency")
            axes[1, 0].axvline(x=0, color="red", linestyle="--")
        
        edges = [t.edge_captured for t in result.trades]
        if edges:
            axes[1, 1].scatter(range(len(edges)), edges, alpha=0.6)
            axes[1, 1].set_title("Edge Captured per Trade")
            axes[1, 1].set_xlabel("Trade #")
            axes[1, 1].set_ylabel("Edge (%)")
            axes[1, 1].axhline(y=0, color="red", linestyle="--")
        
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path, dpi=150)
            logger.info(f"Results plot saved to {save_path}")
        
        plt.show()
    
    def print_summary(self, result: BacktestResult):
        """Print a summary of backtest results"""
        print("\n" + "=" * 60)
        print("BACKTEST RESULTS SUMMARY")
        print("=" * 60)
        print(f"Period: {result.start_date.date()} to {result.end_date.date()}")
        print(f"Matches analyzed: {result.num_matches}")
        print(f"Goals observed: {result.num_goals}")
        print(f"Trades executed: {result.num_trades}")
        print("-" * 60)
        print(f"Total PnL: ${result.total_pnl:,.2f}")
        print(f"Win Rate: {result.win_rate:.1%}")
        print(f"Avg PnL/Trade: ${result.avg_pnl_per_trade:,.2f}")
        print(f"Max Drawdown: {result.max_drawdown:.1%}")
        print(f"Sharpe Ratio: {result.sharpe_ratio:.2f}")
        print("=" * 60)
        
        if result.trades:
            print("\nBest Trade: ${:.2f}".format(max(t.pnl for t in result.trades)))
            print("Worst Trade: ${:.2f}".format(min(t.pnl for t in result.trades)))
            print("Avg Edge Captured: {:.2%}".format(
                sum(t.edge_captured for t in result.trades) / len(result.trades)
            ))
