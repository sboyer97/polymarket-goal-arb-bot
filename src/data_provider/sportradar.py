import asyncio
import json
from typing import AsyncIterator, Optional
from datetime import datetime

import websockets
import httpx
from loguru import logger

from config.settings import settings
from src.utils.models import GoalEvent, MatchInfo, MatchStatus, Team
from .base import DataProvider


class SportradarProvider(DataProvider):
    """Sportradar data provider implementation"""
    
    BASE_URL = "https://api.sportradar.com/soccer/production/v4"
    
    def __init__(self):
        super().__init__()
        self.api_key = settings.data_provider.api_key
        self.ws_url = settings.data_provider.websocket_url
        self._ws_connection = None
        self._http_client: Optional[httpx.AsyncClient] = None
        self._subscribed_matches: set[str] = set()
        self._event_queue: asyncio.Queue[GoalEvent] = asyncio.Queue()
    
    async def connect(self) -> bool:
        """Connect to Sportradar API"""
        try:
            self._http_client = httpx.AsyncClient(timeout=30.0)
            
            if self.ws_url:
                self._ws_connection = await websockets.connect(
                    f"{self.ws_url}?api_key={self.api_key}"
                )
                asyncio.create_task(self._listen_websocket())
            
            self._connected = True
            logger.info("Connected to Sportradar")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to Sportradar: {e}")
            return False
    
    async def disconnect(self):
        """Disconnect from Sportradar"""
        if self._ws_connection:
            await self._ws_connection.close()
        if self._http_client:
            await self._http_client.aclose()
        self._connected = False
        logger.info("Disconnected from Sportradar")
    
    async def subscribe_match(self, match_id: str):
        """Subscribe to live updates for a match"""
        self._subscribed_matches.add(match_id)
        
        if self._ws_connection:
            await self._ws_connection.send(json.dumps({
                "type": "subscribe",
                "match_id": match_id
            }))
        
        logger.info(f"Subscribed to match: {match_id}")
    
    async def unsubscribe_match(self, match_id: str):
        """Unsubscribe from match updates"""
        self._subscribed_matches.discard(match_id)
        
        if self._ws_connection:
            await self._ws_connection.send(json.dumps({
                "type": "unsubscribe",
                "match_id": match_id
            }))
        
        logger.info(f"Unsubscribed from match: {match_id}")
    
    async def get_live_matches(self) -> list[MatchInfo]:
        """Get currently live soccer matches"""
        try:
            response = await self._http_client.get(
                f"{self.BASE_URL}/en/schedules/live/schedule.json",
                params={"api_key": self.api_key}
            )
            response.raise_for_status()
            data = response.json()
            
            matches = []
            for event in data.get("sport_events", []):
                status = self._parse_status(event.get("sport_event_status", {}))
                scores = event.get("sport_event_status", {})
                
                matches.append(MatchInfo(
                    match_id=event.get("id", ""),
                    home_team=event.get("competitors", [{}])[0].get("name", "Unknown"),
                    away_team=event.get("competitors", [{}])[1].get("name", "Unknown") if len(event.get("competitors", [])) > 1 else "Unknown",
                    start_time=datetime.fromisoformat(event.get("scheduled", datetime.utcnow().isoformat()).replace("Z", "+00:00")),
                    league=event.get("tournament", {}).get("name", "Unknown"),
                    status=status,
                    home_score=scores.get("home_score", 0),
                    away_score=scores.get("away_score", 0),
                    current_minute=scores.get("match_time", 0)
                ))
            
            return matches
            
        except Exception as e:
            logger.error(f"Error fetching live matches: {e}")
            return []
    
    async def get_match_info(self, match_id: str) -> Optional[MatchInfo]:
        """Get information about a specific match"""
        try:
            response = await self._http_client.get(
                f"{self.BASE_URL}/en/sport_events/{match_id}/summary.json",
                params={"api_key": self.api_key}
            )
            response.raise_for_status()
            data = response.json()
            
            event = data.get("sport_event", {})
            status_data = data.get("sport_event_status", {})
            
            return MatchInfo(
                match_id=match_id,
                home_team=event.get("competitors", [{}])[0].get("name", "Unknown"),
                away_team=event.get("competitors", [{}])[1].get("name", "Unknown") if len(event.get("competitors", [])) > 1 else "Unknown",
                start_time=datetime.fromisoformat(event.get("scheduled", datetime.utcnow().isoformat()).replace("Z", "+00:00")),
                league=event.get("tournament", {}).get("name", "Unknown"),
                status=self._parse_status(status_data),
                home_score=status_data.get("home_score", 0),
                away_score=status_data.get("away_score", 0),
                current_minute=status_data.get("match_time", 0)
            )
            
        except Exception as e:
            logger.error(f"Error getting match info: {e}")
            return None
    
    async def stream_events(self) -> AsyncIterator[GoalEvent]:
        """Stream goal events"""
        while self._connected:
            try:
                event = await asyncio.wait_for(
                    self._event_queue.get(),
                    timeout=1.0
                )
                yield event
            except asyncio.TimeoutError:
                continue
    
    async def _listen_websocket(self):
        """Listen for WebSocket messages"""
        try:
            async for message in self._ws_connection:
                data = json.loads(message)
                
                if data.get("type") == "goal":
                    event = self._parse_goal_event(data)
                    if event and event.match_id in self._subscribed_matches:
                        await self._event_queue.put(event)
                        await self._notify_goal(event)
                        
        except websockets.ConnectionClosed:
            logger.warning("WebSocket connection closed")
        except Exception as e:
            logger.error(f"WebSocket error: {e}")
    
    def _parse_goal_event(self, data: dict) -> Optional[GoalEvent]:
        """Parse a goal event from WebSocket data"""
        try:
            return GoalEvent(
                match_id=data.get("match_id", ""),
                timestamp=datetime.fromisoformat(data.get("timestamp", datetime.utcnow().isoformat())),
                minute=data.get("minute", 0),
                team=Team.HOME if data.get("team") == "home" else Team.AWAY,
                scorer=data.get("scorer"),
                home_score=data.get("home_score", 0),
                away_score=data.get("away_score", 0)
            )
        except Exception as e:
            logger.error(f"Error parsing goal event: {e}")
            return None
    
    def _parse_status(self, status_data: dict) -> MatchStatus:
        """Parse match status from API response"""
        status_map = {
            "not_started": MatchStatus.NOT_STARTED,
            "1st_half": MatchStatus.FIRST_HALF,
            "halftime": MatchStatus.HALFTIME,
            "2nd_half": MatchStatus.SECOND_HALF,
            "ended": MatchStatus.FINISHED,
            "closed": MatchStatus.FINISHED
        }
        return status_map.get(status_data.get("status", ""), MatchStatus.NOT_STARTED)
