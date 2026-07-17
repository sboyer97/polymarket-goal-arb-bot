import asyncio
from typing import Optional
from datetime import datetime, timedelta
from dataclasses import dataclass, field

from loguru import logger

from src.polymarket.client import PolymarketClient
from src.data_provider.base import DataProvider
from src.trading.strategy import TradingStrategy, GoalArbitrageStrategy
from src.utils.models import (
    GoalEvent, PolymarketMatch, Order, OrderSide, Position, TradeSignal
)
from config.settings import settings


@dataclass
class ActiveTrade:
    """Track an active trade"""
    signal: TradeSignal
    entry_time: datetime
    entry_price: float
    size: float
    order_id: Optional[str] = None


@dataclass 
class TradeResult:
    """Result of a completed trade"""
    match_id: str
    token_id: str
    entry_price: float
    exit_price: float
    size: float
    pnl: float
    duration_seconds: float
    reason: str
    timestamp: datetime = field(default_factory=datetime.utcnow)


class TradingEngine:
    """Main trading engine that coordinates everything"""
    
    def __init__(
        self,
        polymarket_client: PolymarketClient,
        data_provider: DataProvider,
        strategy: Optional[TradingStrategy] = None
    ):
        self.polymarket = polymarket_client
        self.data_provider = data_provider
        self.strategy = strategy or GoalArbitrageStrategy()
        
        self._market_cache: dict[str, PolymarketMatch] = {}
        self._match_to_market: dict[str, str] = {}
        self._active_trades: dict[str, ActiveTrade] = {}
        self._trade_history: list[TradeResult] = []
        self._running = False
    
    async def start(self):
        """Start the trading engine"""
        logger.info("Starting trading engine...")
        
        await self.polymarket.initialize()
        await self.data_provider.connect()
        
        await self._load_soccer_markets()
        
        self._running = True
        
        asyncio.create_task(self._monitor_positions())
        
        logger.info("Trading engine started")
        
        await self._process_events()
    
    async def stop(self):
        """Stop the trading engine"""
        logger.info("Stopping trading engine...")
        self._running = False
        
        await self._close_all_positions()
        
        await self.polymarket.close()
        await self.data_provider.disconnect()
        
        logger.info("Trading engine stopped")
    
    async def _load_soccer_markets(self):
        """Load active soccer markets from Polymarket"""
        markets = await self.polymarket.search_soccer_markets()
        
        for market_data in markets:
            try:
                market = self._parse_market(market_data)
                if market:
                    self._market_cache[market.market_id] = market
                    logger.debug(f"Loaded market: {market.question}")
            except Exception as e:
                logger.warning(f"Failed to parse market: {e}")
        
        logger.info(f"Loaded {len(self._market_cache)} soccer markets")
    
    def link_match_to_market(self, match_id: str, market_id: str):
        """Link a data provider match ID to a Polymarket market"""
        self._match_to_market[match_id] = market_id
        logger.info(f"Linked match {match_id} to market {market_id}")
    
    async def _process_events(self):
        """Main event processing loop"""
        logger.info("Starting event processing loop...")
        
        async for goal_event in self.data_provider.stream_events():
            if not self._running:
                break
            
            try:
                await self._handle_goal(goal_event)
            except Exception as e:
                logger.error(f"Error handling goal event: {e}")
    
    async def _handle_goal(self, event: GoalEvent):
        """Handle a goal event"""
        logger.info(f"Goal detected: Match {event.match_id}, Score {event.home_score}-{event.away_score}")
        
        market_id = self._match_to_market.get(event.match_id)
        if not market_id:
            logger.warning(f"No market linked for match {event.match_id}")
            return
        
        market = self._market_cache.get(market_id)
        if not market:
            logger.warning(f"Market {market_id} not found in cache")
            return
        
        current_prices = {}
        for outcome in market.outcomes:
            price = await self.polymarket.get_midpoint_price(outcome.token_id)
            if price is not None:
                current_prices[outcome.token_id] = price
        
        signal = self.strategy.analyze_goal(event, market, current_prices)
        
        if signal:
            await self._execute_signal(signal, current_prices)
    
    async def _execute_signal(self, signal: TradeSignal, current_prices: dict[str, float]):
        """Execute a trading signal"""
        
        if signal.token_id in self._active_trades:
            logger.warning(f"Already have active trade for {signal.token_id}")
            return
        
        current_price = current_prices.get(signal.token_id, 0)
        
        order = Order(
            market_id=signal.market_id,
            token_id=signal.token_id,
            side=signal.side,
            size=signal.recommended_size,
            price=current_price + settings.trading.max_slippage_percent / 100
        )
        
        result = await self.polymarket.place_order(order)
        
        if result:
            trade = ActiveTrade(
                signal=signal,
                entry_time=datetime.utcnow(),
                entry_price=current_price,
                size=signal.recommended_size,
                order_id=result.get("order_id")
            )
            self._active_trades[signal.token_id] = trade
            logger.info(f"Entered trade: {signal.reason}")
    
    async def _monitor_positions(self):
        """Monitor active positions and exit when conditions are met"""
        while self._running:
            await asyncio.sleep(0.5)
            
            to_close = []
            
            for token_id, trade in self._active_trades.items():
                current_price = await self.polymarket.get_midpoint_price(token_id)
                if current_price is None:
                    continue
                
                time_in_position = (datetime.utcnow() - trade.entry_time).total_seconds()
                
                market = self._market_cache.get(trade.signal.market_id)
                if not market:
                    continue
                
                should_exit = self.strategy.should_exit(
                    market=market,
                    entry_price=trade.entry_price,
                    current_price=current_price,
                    time_in_position=time_in_position
                )
                
                if should_exit:
                    to_close.append((token_id, current_price, time_in_position))
            
            for token_id, exit_price, duration in to_close:
                await self._close_position(token_id, exit_price, duration)
    
    async def _close_position(self, token_id: str, exit_price: float, duration: float):
        """Close a position"""
        trade = self._active_trades.get(token_id)
        if not trade:
            return
        
        order = Order(
            market_id=trade.signal.market_id,
            token_id=token_id,
            side=OrderSide.SELL,
            size=trade.size,
            price=exit_price - settings.trading.max_slippage_percent / 100
        )
        
        result = await self.polymarket.place_order(order)
        
        if result:
            pnl = (exit_price - trade.entry_price) * trade.size
            
            trade_result = TradeResult(
                match_id=trade.signal.match_id,
                token_id=token_id,
                entry_price=trade.entry_price,
                exit_price=exit_price,
                size=trade.size,
                pnl=pnl,
                duration_seconds=duration,
                reason=trade.signal.reason
            )
            self._trade_history.append(trade_result)
            
            del self._active_trades[token_id]
            
            logger.info(f"Closed position: PnL ${pnl:.2f} ({duration:.1f}s)")
    
    async def _close_all_positions(self):
        """Close all active positions"""
        for token_id in list(self._active_trades.keys()):
            price = await self.polymarket.get_midpoint_price(token_id)
            if price:
                duration = (datetime.utcnow() - self._active_trades[token_id].entry_time).total_seconds()
                await self._close_position(token_id, price, duration)
    
    def _parse_market(self, data: dict) -> Optional[PolymarketMatch]:
        """Parse market data from API response"""
        try:
            outcomes = []
            for token in data.get("tokens", []):
                outcomes.append(MarketOutcome(
                    token_id=token.get("token_id", ""),
                    outcome=token.get("outcome", ""),
                    price=float(token.get("price", 0))
                ))
            
            return PolymarketMatch(
                market_id=data.get("id", ""),
                condition_id=data.get("condition_id", ""),
                match_id="",
                question=data.get("question", ""),
                home_team="",
                away_team="",
                outcomes=outcomes,
                end_date=datetime.fromisoformat(data.get("end_date_iso", datetime.utcnow().isoformat()).replace("Z", "+00:00")),
                active=data.get("active", False)
            )
        except Exception as e:
            logger.error(f"Failed to parse market: {e}")
            return None
    
    def get_trade_history(self) -> list[TradeResult]:
        """Get trade history"""
        return self._trade_history.copy()
    
    def get_stats(self) -> dict:
        """Get trading statistics"""
        if not self._trade_history:
            return {"total_trades": 0}
        
        total_pnl = sum(t.pnl for t in self._trade_history)
        winning_trades = [t for t in self._trade_history if t.pnl > 0]
        losing_trades = [t for t in self._trade_history if t.pnl <= 0]
        
        return {
            "total_trades": len(self._trade_history),
            "winning_trades": len(winning_trades),
            "losing_trades": len(losing_trades),
            "win_rate": len(winning_trades) / len(self._trade_history) if self._trade_history else 0,
            "total_pnl": total_pnl,
            "avg_pnl": total_pnl / len(self._trade_history),
            "avg_duration_seconds": sum(t.duration_seconds for t in self._trade_history) / len(self._trade_history)
        }


from src.utils.models import MarketOutcome
