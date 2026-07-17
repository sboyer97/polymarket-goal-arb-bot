import asyncio
import httpx
from loguru import logger
from typing import Optional, Tuple
from datetime import datetime

from config.settings import settings
from src.utils.models import (
    PolymarketMatch, MarketOutcome, Order, OrderSide, Position
)


class PolymarketClient:
    """Client for interacting with Polymarket CLOB API"""
    
    def __init__(self):
        self.clob_url = settings.polymarket.clob_api_url
        self.gamma_url = settings.polymarket.gamma_api_url
        self.api_key = settings.polymarket.api_key
        self.private_key = settings.polymarket.private_key
        self._http_client: Optional[httpx.AsyncClient] = None
        self._clob_client = None
    
    async def initialize(self):
        """Initialize the client. Supports:
        1) Magic + Smart Wallet: POLYMARKET_PRIVATE_KEY + POLYMARKET_SMART_WALLET → derive API creds (signature_type=1).
        2) EOA + pre-set creds: POLYMARKET_PRIVATE_KEY + API_KEY/SECRET/PASSPHRASE.
        """
        self._http_client = httpx.AsyncClient(timeout=30.0)
        
        if not self.private_key:
            logger.warning("No POLYMARKET_PRIVATE_KEY provided, running in read-only mode")
            return

        try:
            from py_clob_client.client import ClobClient
            from py_clob_client.clob_types import ApiCreds

            smart_wallet = getattr(settings.polymarket, "smart_wallet", "") or ""
            has_pre_set_creds = bool(self.api_key and settings.polymarket.api_secret)

            if smart_wallet and not has_pre_set_creds:
                # Magic + Smart Wallet: derive API credentials from private key
                POLY_PROXY_SIGNATURE = 1
                client_temp = ClobClient(
                    host=self.clob_url,
                    chain_id=settings.polymarket.chain_id,
                    key=self.private_key,
                    signature_type=POLY_PROXY_SIGNATURE,
                    funder=smart_wallet,
                )
                creds = client_temp.create_or_derive_api_creds()
                self._clob_client = ClobClient(
                    host=self.clob_url,
                    chain_id=settings.polymarket.chain_id,
                    key=self.private_key,
                    creds=creds,
                    signature_type=POLY_PROXY_SIGNATURE,
                    funder=smart_wallet,
                )
                logger.info("Polymarket CLOB initialized (Magic/Smart Wallet, derived API key)")
                await self._warmup_client()
            elif has_pre_set_creds:
                self._clob_client = ClobClient(
                    host=self.clob_url,
                    chain_id=settings.polymarket.chain_id,
                    key=self.private_key,
                    creds=ApiCreds(
                        api_key=self.api_key,
                        api_secret=settings.polymarket.api_secret,
                        api_passphrase=settings.polymarket.api_passphrase or "",
                    ),
                )
                logger.info("Polymarket CLOB client initialized with credentials")
                await self._warmup_client()
            else:
                # Private key only: try to derive (no funder = EOA mode)
                client_temp = ClobClient(
                    host=self.clob_url,
                    chain_id=settings.polymarket.chain_id,
                    key=self.private_key,
                )
                creds = client_temp.create_or_derive_api_creds()
                self._clob_client = ClobClient(
                    host=self.clob_url,
                    chain_id=settings.polymarket.chain_id,
                    key=self.private_key,
                    creds=creds,
                )
                logger.info("Polymarket CLOB initialized (derived API key)")
                await self._warmup_client()
        except Exception as e:
            logger.warning(f"Could not initialize CLOB client: {e}")
            logger.info("Running in read-only mode")
    
    async def _warmup_client(self):
        """Pre-warm internal caches and HTTP connections to reduce first-order latency (~800ms savings).
        
        The first call to create_market_order has ~800ms overhead from:
        1. HTTP connection establishment
        2. Internal library initialization
        We do a dummy order creation (not posted) to absorb this overhead at startup.
        """
        if not self._clob_client:
            return
        try:
            from py_clob_client.clob_types import MarketOrderArgs, OrderType
            from py_clob_client.order_builder.constants import BUY
            
            # Use a known liquid token - MicroStrategy BTC market
            WARMUP_TOKEN = "38397507750621893057346880033441136112987238933685677349709401910643842844855"
            
            # Create a dummy order (not posted) to warm up internal state
            order_args = MarketOrderArgs(
                token_id=WARMUP_TOKEN,
                amount=0.1,
                side=BUY,
                price=0,
                order_type=OrderType.FOK,
            )
            # Run in thread to not block async loop
            await asyncio.to_thread(self._clob_client.create_market_order, order_args)
            logger.debug("Client warm-up complete")
        except Exception as e:
            logger.debug(f"Client warm-up failed (non-critical): {e}")
    
    async def close(self):
        """Close the client"""
        if self._http_client:
            await self._http_client.aclose()
    
    async def search_soccer_markets(self, query: str = "soccer") -> list[dict]:
        """Search for soccer-related markets using Gamma API"""
        try:
            response = await self._http_client.get(
                f"{self.gamma_url}/markets",
                params={
                    "tag": "sports",
                    "active": "true",
                    "limit": 100
                }
            )
            response.raise_for_status()
            markets = response.json()
            
            soccer_keywords = ["soccer", "football", "premier league", "la liga", 
                             "bundesliga", "serie a", "champions league", "world cup",
                             "uefa", "fifa", "mls"]
            
            soccer_markets = []
            for market in markets:
                question = market.get("question", "").lower()
                if any(kw in question for kw in soccer_keywords):
                    soccer_markets.append(market)
            
            logger.info(f"Found {len(soccer_markets)} soccer markets")
            return soccer_markets
            
        except Exception as e:
            logger.error(f"Error searching markets: {e}")
            return []
    
    async def get_market(self, condition_id: str) -> Optional[dict]:
        """Get detailed market information"""
        try:
            response = await self._http_client.get(
                f"{self.clob_url}/markets/{condition_id}"
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error getting market {condition_id}: {e}")
            return None
    
    async def get_orderbook(self, token_id: str) -> dict:
        """Get the orderbook for a specific token"""
        try:
            response = await self._http_client.get(
                f"{self.clob_url}/book",
                params={"token_id": token_id}
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error getting orderbook: {e}")
            return {"bids": [], "asks": []}
    
    async def get_midpoint_price(self, token_id: str) -> Optional[float]:
        """Get the midpoint price for a token"""
        try:
            response = await self._http_client.get(
                f"{self.clob_url}/midpoint",
                params={"token_id": token_id}
            )
            response.raise_for_status()
            data = response.json()
            return float(data.get("mid", 0))
        except Exception as e:
            logger.error(f"Error getting midpoint: {e}")
            return None

    async def get_bid_price(self, token_id: str) -> Optional[float]:
        """Get best bid (price at which we can sell)."""
        try:
            response = await self._http_client.get(
                f"{self.clob_url}/price",
                params={"token_id": token_id, "side": "SELL"}
            )
            if response.status_code != 200:
                return None
            data = response.json()
            p = data.get("price")
            return float(p) if p is not None else None
        except Exception as e:
            logger.debug(f"Bid price: {e}")
            return None
    
    async def place_order(self, order: Order) -> Optional[dict]:
        """Place an order on Polymarket"""
        if settings.trading.dry_run:
            logger.info(f"[DRY RUN] Would place order: {order}")
            return {"order_id": "dry_run", "status": "simulated"}
        
        if not self._clob_client:
            logger.error("CLOB client not initialized, cannot place order")
            return None
        
        try:
            from py_clob_client.clob_types import OrderArgs, OrderType
            from py_clob_client.order_builder.constants import BUY, SELL
            
            side = BUY if order.side == OrderSide.BUY else SELL
            
            order_args = OrderArgs(
                token_id=order.token_id,
                price=order.price,
                size=order.size,
                side=side
            )
            
            signed_order = self._clob_client.create_order(order_args)
            result = self._clob_client.post_order(signed_order, OrderType.GTC)
            
            logger.info(f"Order placed: {result}")
            return result
            
        except Exception as e:
            logger.error(f"Error placing order: {e}")
            return None

    async def update_balance_allowance(self, token_id: str) -> bool:
        """Refresh balance/allowance for a token (helps avoid 'not enough balance/allowance' on SELL)."""
        if not self._clob_client or not token_id or settings.trading.dry_run:
            return False
        try:
            from py_clob_client.clob_types import BalanceAllowanceParams, AssetType
            params = BalanceAllowanceParams(
                asset_type=AssetType.CONDITIONAL,
                token_id=token_id,
                signature_type=-1,
            )
            await asyncio.to_thread(self._clob_client.update_balance_allowance, params)
            logger.debug(f"update_balance_allowance OK for {token_id[:20]}...")
            return True
        except Exception as e:
            logger.debug(f"update_balance_allowance: {e}")
            return False

    async def set_allowances(self, token_id: Optional[str] = None) -> bool:
        """Update balance allowance for trading. If no token_id, uses a default liquid market."""
        if not self._clob_client or settings.trading.dry_run:
            return False
        try:
            from py_clob_client.clob_types import BalanceAllowanceParams, AssetType
            target_token = token_id or "38397507750621893057346880033441136112987238933685677349709401910643842844855"
            params = BalanceAllowanceParams(
                asset_type=AssetType.CONDITIONAL,
                token_id=target_token,
                signature_type=-1,
            )
            await asyncio.to_thread(self._clob_client.update_balance_allowance, params)
            logger.debug(f"set_allowances OK for {target_token[:20]}...")
            return True
        except Exception as e:
            logger.debug(f"set_allowances: {e}")
            return False

    async def place_limit_order(
        self, side: str, token_id: str, price: float, size: float
    ) -> Optional[dict]:
        """Place a GTC limit order. SELL: size=shares at price. Returns result or None."""
        if settings.trading.dry_run:
            logger.info(f"[DRY RUN] Would place limit order: {side} {size} @ {price} {token_id[:20]}...")
            return {"orderID": "dry_run", "success": True}
        if not self._clob_client:
            return None
        try:
            from py_clob_client.clob_types import OrderArgs, OrderType
            from py_clob_client.order_builder.constants import BUY, SELL
            side_str = BUY if side in (OrderSide.BUY, "BUY") else SELL
            order_args = OrderArgs(token_id=token_id, price=price, size=size, side=side_str)
            signed = self._clob_client.create_order(order_args)
            result = await asyncio.to_thread(self._clob_client.post_order, signed, OrderType.GTC)
            logger.info(f"Limit order placed: {result}")
            return result
        except Exception as e:
            logger.error(f"Error placing limit order: {e}")
            return None

    async def place_sell_with_fallback(
        self, token_id: str, sell_size: float
    ) -> Optional[dict]:
        """Simple FOK SELL. On failure, retry after set_allowances."""
        # 1) FOK market SELL direct
        result = await self.place_market_order("SELL", token_id, sell_size)
        if result and (result.get("orderID") or result.get("success")):
            return result
        
        # 2) On failure, refresh allowances and retry
        logger.warning(f"FOK SELL failed, retrying after set_allowances...")
        await self.set_allowances()
        await self.update_balance_allowance(token_id)
        await asyncio.sleep(0.3)
        
        return await self.place_market_order("SELL", token_id, sell_size)

    async def place_market_order(
        self, side: str, token_id: str, amount: float
    ) -> Optional[dict]:
        """Place a FOK market order. LATENCY-OPTIMIZED. BUY: amount = USD. SELL: amount = shares."""
        if settings.trading.dry_run:
            return {"orderID": "dry_run", "success": True}
        if not self._clob_client:
            return None
        try:
            from py_clob_client.clob_types import MarketOrderArgs, OrderType
            from py_clob_client.order_builder.constants import BUY, SELL
            side_str = BUY if side == OrderSide.BUY or side == "BUY" else SELL
            order_args = MarketOrderArgs(
                token_id=token_id,
                amount=amount,
                side=side_str,
                price=0,
                order_type=OrderType.FOK,
            )
            signed = self._clob_client.create_market_order(order_args)
            result = await asyncio.to_thread(
                self._clob_client.post_order, signed, OrderType.FOK
            )
            return result
        except Exception as e:
            logger.error(f"Market order error: {e}")
            return None

    async def get_order_fill_info(
        self, order_id: str, timeout_seconds: float = 5.0
    ) -> Optional[Tuple[float, float]]:
        """Poll get_order until filled or timeout. Returns (size_matched, price) or None."""
        if not self._clob_client or not order_id or str(order_id) == "dry_run":
            return None
        deadline = asyncio.get_event_loop().time() + timeout_seconds
        poll_interval = 0.05  # 50ms for minimal latency
        while asyncio.get_event_loop().time() < deadline:
            try:
                order = await asyncio.to_thread(self._clob_client.get_order, order_id)
                if not order:
                    await asyncio.sleep(poll_interval)
                    continue
                size_matched = float(order.get("size_matched") or order.get("sizeMatched") or 0)
                if size_matched >= 1e6:
                    size_matched = size_matched / 1_000_000
                price = order.get("average_price") or order.get("avgPrice") or order.get("price")
                if price is not None:
                    price = float(price)
                if size_matched > 0 and price is not None and price > 0:
                    return (size_matched, price)
            except Exception as e:
                logger.debug(f"get_order: {e}")
            await asyncio.sleep(poll_interval)
        return None

    async def cancel_order(self, order_id: str) -> bool:
        """Cancel an existing order"""
        if settings.trading.dry_run:
            logger.info(f"[DRY RUN] Would cancel order: {order_id}")
            return True
        
        if not self._clob_client:
            logger.error("CLOB client not initialized")
            return False
        
        try:
            self._clob_client.cancel(order_id)
            logger.info(f"Order cancelled: {order_id}")
            return True
        except Exception as e:
            logger.error(f"Error cancelling order: {e}")
            return False
    
    async def get_positions(self) -> list[Position]:
        """Get current positions"""
        if not self._clob_client:
            logger.warning("CLOB client not initialized, returning empty positions")
            return []
        
        try:
            positions_data = self._clob_client.get_positions()
            positions = []
            
            for pos in positions_data:
                positions.append(Position(
                    market_id=pos.get("market", ""),
                    token_id=pos.get("asset", ""),
                    outcome=pos.get("outcome", ""),
                    size=float(pos.get("size", 0)),
                    avg_price=float(pos.get("avg_price", 0)),
                    current_price=float(pos.get("cur_price", 0)),
                    unrealized_pnl=float(pos.get("unrealized_pnl", 0))
                ))
            
            return positions
            
        except Exception as e:
            logger.error(f"Error getting positions: {e}")
            return []
    
    async def get_balance(self) -> float:
        """Get USDC balance"""
        if not self._clob_client:
            return 0.0
        
        try:
            balance = self._clob_client.get_balance()
            return float(balance)
        except Exception as e:
            logger.error(f"Error getting balance: {e}")
            return 0.0

    async def get_position_size(self, token_id: str) -> float:
        """Get actual shares held for a conditional token (on-chain balance). Use for SELL size.
        signature_type=-1 → client uses its builder.sig_type (1 for Magic/Proxy, 0 for EOA)."""
        if not self._clob_client or not token_id:
            return 0.0
        try:
            from py_clob_client.clob_types import BalanceAllowanceParams, AssetType
            import asyncio
            params = BalanceAllowanceParams(
                asset_type=AssetType.CONDITIONAL,
                token_id=token_id,
                signature_type=-1,  # -1 = use client's sig_type (proxy vs EOA)
            )
            result = await asyncio.to_thread(
                self._clob_client.get_balance_allowance, params
            )
            raw_balance = int(result.get("balance", 0))
            shares = raw_balance / 1_000_000
            if raw_balance == 0:
                logger.debug(f"get_position_size: balance=0 for token {token_id[:20]}...")
            return float(shares)
        except Exception as e:
            logger.warning(f"get_position_size failed: {e}")
            return 0.0
