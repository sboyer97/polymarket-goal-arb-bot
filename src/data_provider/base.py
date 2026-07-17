from abc import ABC, abstractmethod
from typing import AsyncIterator, Callable, Optional
from datetime import datetime

from src.utils.models import GoalEvent, MatchInfo


class DataProvider(ABC):
    """Abstract base class for soccer data providers"""
    
    def __init__(self):
        self._goal_callbacks: list[Callable[[GoalEvent], None]] = []
        self._connected = False
    
    @abstractmethod
    async def connect(self) -> bool:
        """Connect to the data provider"""
        pass
    
    @abstractmethod
    async def disconnect(self):
        """Disconnect from the data provider"""
        pass
    
    @abstractmethod
    async def subscribe_match(self, match_id: str):
        """Subscribe to live updates for a specific match"""
        pass
    
    @abstractmethod
    async def unsubscribe_match(self, match_id: str):
        """Unsubscribe from match updates"""
        pass
    
    @abstractmethod
    async def get_live_matches(self) -> list[MatchInfo]:
        """Get list of currently live matches"""
        pass
    
    @abstractmethod
    async def get_match_info(self, match_id: str) -> Optional[MatchInfo]:
        """Get information about a specific match"""
        pass
    
    @abstractmethod
    async def stream_events(self) -> AsyncIterator[GoalEvent]:
        """Stream goal events in real-time"""
        pass
    
    def on_goal(self, callback: Callable[[GoalEvent], None]):
        """Register a callback to be called when a goal is scored"""
        self._goal_callbacks.append(callback)
    
    async def _notify_goal(self, event: GoalEvent):
        """Notify all registered callbacks of a goal"""
        for callback in self._goal_callbacks:
            try:
                if asyncio.iscoroutinefunction(callback):
                    await callback(event)
                else:
                    callback(event)
            except Exception as e:
                logger.error(f"Error in goal callback: {e}")
    
    @property
    def is_connected(self) -> bool:
        return self._connected


import asyncio
from loguru import logger
