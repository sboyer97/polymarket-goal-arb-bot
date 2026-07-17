from abc import ABC, abstractmethod
from typing import Optional
from datetime import datetime

from loguru import logger

from src.utils.models import (
    GoalEvent, PolymarketMatch, TradeSignal, OrderSide, MarketOutcome
)
from config.settings import settings


class TradingStrategy(ABC):
    """Base class for trading strategies"""
    
    @abstractmethod
    def analyze_goal(
        self, 
        event: GoalEvent, 
        market: PolymarketMatch,
        current_prices: dict[str, float]
    ) -> Optional[TradeSignal]:
        """Analyze a goal event and generate a trading signal"""
        pass
    
    @abstractmethod
    def should_exit(
        self,
        market: PolymarketMatch,
        entry_price: float,
        current_price: float,
        time_in_position: float
    ) -> bool:
        """Determine if we should exit a position"""
        pass


class GoalArbitrageStrategy(TradingStrategy):
    """
    Goal arbitrage strategy:
    - When a goal is scored, market odds shift rapidly
    - If we get the goal info faster than the market, we can capture the edge
    - We buy the outcome that benefits from the goal (team scoring wins/draw)
    - We exit quickly after the market adjusts
    """
    
    def __init__(self):
        self.min_edge = settings.trading.min_edge_percent / 100
        self.max_position = settings.trading.max_position_size_usdc
        self.exit_delay = settings.trading.exit_delay_seconds
    
    def analyze_goal(
        self, 
        event: GoalEvent, 
        market: PolymarketMatch,
        current_prices: dict[str, float]
    ) -> Optional[TradeSignal]:
        """
        When a goal is scored:
        1. Calculate expected new fair odds based on new score
        2. Compare with current market prices
        3. If edge exists, generate buy signal
        """
        
        new_fair_prices = self._calculate_fair_prices(event)
        
        best_edge = 0.0
        best_outcome: Optional[MarketOutcome] = None
        
        for outcome in market.outcomes:
            current_price = current_prices.get(outcome.token_id, outcome.price)
            fair_price = new_fair_prices.get(outcome.outcome, current_price)
            
            edge = fair_price - current_price
            
            if edge > best_edge and edge >= self.min_edge:
                best_edge = edge
                best_outcome = outcome
        
        if not best_outcome:
            logger.debug(f"No edge found for goal in match {event.match_id}")
            return None
        
        size = self._calculate_position_size(best_edge)
        
        signal = TradeSignal(
            match_id=event.match_id,
            market_id=market.market_id,
            signal_type="entry",
            side=OrderSide.BUY,
            token_id=best_outcome.token_id,
            recommended_size=size,
            expected_edge=best_edge,
            reason=f"Goal by {event.team.value} team at {event.minute}'. Score: {event.home_score}-{event.away_score}. Expected edge: {best_edge:.2%}"
        )
        
        logger.info(f"Signal generated: {signal.reason}")
        return signal
    
    def should_exit(
        self,
        market: PolymarketMatch,
        entry_price: float,
        current_price: float,
        time_in_position: float
    ) -> bool:
        """
        Exit conditions:
        1. Time-based: exit after market has adjusted (exit_delay seconds)
        2. Profit-based: captured expected edge
        3. Stop-loss: price moved against us significantly
        """
        
        if time_in_position >= self.exit_delay:
            logger.info(f"Exit trigger: Time limit reached ({time_in_position:.1f}s)")
            return True
        
        profit = current_price - entry_price
        if profit >= self.min_edge:
            logger.info(f"Exit trigger: Target profit reached ({profit:.2%})")
            return True
        
        loss = entry_price - current_price
        max_loss = settings.trading.max_slippage_percent / 100
        if loss > max_loss:
            logger.warning(f"Exit trigger: Stop loss ({loss:.2%})")
            return True
        
        return False
    
    def _calculate_fair_prices(self, event: GoalEvent) -> dict[str, float]:
        """
        Calculate fair prices based on current score.
        This is a simplified model - in production you'd use more sophisticated odds.
        
        The key insight: when a goal is scored, the scoring team's win probability
        increases significantly, especially late in the game.
        """
        
        score_diff = event.score_diff
        minute = event.minute
        
        time_remaining_factor = max(0, (90 - minute) / 90)
        
        if score_diff > 0:
            home_win_prob = 0.65 + (score_diff * 0.1) - (time_remaining_factor * 0.15)
            away_win_prob = 0.1 + (time_remaining_factor * 0.1)
        elif score_diff < 0:
            home_win_prob = 0.1 + (time_remaining_factor * 0.1)
            away_win_prob = 0.65 + (abs(score_diff) * 0.1) - (time_remaining_factor * 0.15)
        else:
            home_win_prob = 0.35 + (time_remaining_factor * 0.05)
            away_win_prob = 0.35 + (time_remaining_factor * 0.05)
        
        draw_prob = 1 - home_win_prob - away_win_prob
        draw_prob = max(0.05, min(0.4, draw_prob))
        
        total = home_win_prob + away_win_prob + draw_prob
        home_win_prob /= total
        away_win_prob /= total
        draw_prob /= total
        
        return {
            "Home Win": home_win_prob,
            "Away Win": away_win_prob,
            "Draw": draw_prob,
            "Yes": home_win_prob if event.team.value == "home" else away_win_prob,
            "No": 1 - (home_win_prob if event.team.value == "home" else away_win_prob)
        }
    
    def _calculate_position_size(self, edge: float) -> float:
        """
        Kelly criterion for position sizing (with fraction for safety).
        Size = (edge / odds) * bankroll * kelly_fraction
        
        We use a conservative 0.25 Kelly fraction.
        """
        kelly_fraction = 0.25
        estimated_odds = 1.0
        
        raw_size = (edge / estimated_odds) * self.max_position * kelly_fraction
        
        return min(raw_size, self.max_position)
