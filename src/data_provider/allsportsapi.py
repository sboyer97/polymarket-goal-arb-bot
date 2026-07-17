import asyncio
import json
from typing import AsyncIterator, Optional
from datetime import datetime
from dataclasses import dataclass

import websockets
import httpx
from loguru import logger

from config.settings import settings
from src.utils.models import GoalEvent, MatchInfo, MatchStatus, Team
from .base import DataProvider


@dataclass
class MatchState:
    """Track the state of a match to detect new goals"""
    match_id: str
    home_score: int
    away_score: int
    goalscorers: list[dict]


class AllSportsAPIProvider(DataProvider):
    """
    AllSportsAPI data provider with WebSocket support
    
    WebSocket endpoint: wss://wss.allsportsapi.com/live_events
    Docs: https://allsportsapi.com/soccer-football-socket-documentation
    """
    
    REST_BASE_URL = "https://apiv2.allsportsapi.com/football/"
    WS_URL = "wss://wss.allsportsapi.com/live_events"
    
    def __init__(self):
        super().__init__()
        self.api_key = settings.data_provider.api_key
        self._ws_connection = None
        self._http_client: Optional[httpx.AsyncClient] = None
        self._event_queue: asyncio.Queue[GoalEvent] = asyncio.Queue()
        self._match_states: dict[str, MatchState] = {}
        self._subscribed_leagues: set[str] = set()
        self._subscribed_matches: set[str] = set()
        self._ws_task: Optional[asyncio.Task] = None
    
    async def connect(self) -> bool:
        """Connect to AllSportsAPI"""
        try:
            self._http_client = httpx.AsyncClient(timeout=30.0)
            
            if not self.api_key:
                logger.error("AllSportsAPI key not configured")
                return False
            
            test_response = await self._http_client.get(
                self.REST_BASE_URL,
                params={"met": "Leagues", "APIkey": self.api_key},
                follow_redirects=True
            )
            
            if test_response.status_code != 200:
                logger.error(f"AllSportsAPI authentication failed: {test_response.text}")
                return False
            
            data = test_response.json()
            if data.get("success") == 0:
                logger.error(f"AllSportsAPI error: {data.get('result', 'Unknown error')}")
                return False
            
            logger.info(f"API key valid. Found {len(data.get('result', []))} leagues")
            
            ws_url = f"{self.WS_URL}?APIkey={self.api_key}"
            self._ws_connection = await websockets.connect(ws_url)
            
            self._ws_task = asyncio.create_task(self._listen_websocket())
            
            self._connected = True
            logger.info("Connected to AllSportsAPI (WebSocket)")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to AllSportsAPI: {e}")
            return False
    
    async def disconnect(self):
        """Disconnect from AllSportsAPI"""
        self._connected = False
        
        if self._ws_task:
            self._ws_task.cancel()
            try:
                await self._ws_task
            except asyncio.CancelledError:
                pass
        
        if self._ws_connection:
            await self._ws_connection.close()
        
        if self._http_client:
            await self._http_client.aclose()
        
        logger.info("Disconnected from AllSportsAPI")
    
    async def subscribe_match(self, match_id: str):
        """Subscribe to a specific match"""
        self._subscribed_matches.add(match_id)
        
        match_info = await self.get_match_info(match_id)
        if match_info:
            self._match_states[match_id] = MatchState(
                match_id=match_id,
                home_score=match_info.home_score,
                away_score=match_info.away_score,
                goalscorers=[]
            )
        
        logger.info(f"Subscribed to match: {match_id}")
    
    async def subscribe_league(self, league_id: str):
        """Subscribe to all matches in a league"""
        self._subscribed_leagues.add(league_id)
        logger.info(f"Subscribed to league: {league_id}")
    
    async def unsubscribe_match(self, match_id: str):
        """Unsubscribe from a match"""
        self._subscribed_matches.discard(match_id)
        self._match_states.pop(match_id, None)
        logger.info(f"Unsubscribed from match: {match_id}")
    
    async def get_live_matches(self) -> list[MatchInfo]:
        """Get all currently live matches"""
        try:
            response = await self._http_client.get(
                self.REST_BASE_URL,
                params={
                    "met": "Livescore",
                    "APIkey": self.api_key
                }
            )
            response.raise_for_status()
            data = response.json()
            
            if data.get("success") == 0:
                logger.warning(f"AllSportsAPI error: {data.get('result')}")
                return []
            
            matches = []
            for match in data.get("result", []):
                matches.append(self._parse_match(match))
            
            return matches
            
        except Exception as e:
            logger.error(f"Error fetching live matches: {e}")
            return []
    
    async def get_match_info(self, match_id: str) -> Optional[MatchInfo]:
        """Get information about a specific match"""
        try:
            response = await self._http_client.get(
                self.REST_BASE_URL,
                params={
                    "met": "Livescore",
                    "APIkey": self.api_key,
                    "matchId": match_id
                }
            )
            response.raise_for_status()
            data = response.json()
            
            if data.get("success") == 0 or not data.get("result"):
                return None
            
            return self._parse_match(data["result"][0])
            
        except Exception as e:
            logger.error(f"Error fetching match {match_id}: {e}")
            return None
    
    async def get_leagues(self) -> list[dict]:
        """Get list of available leagues"""
        try:
            response = await self._http_client.get(
                self.REST_BASE_URL,
                params={
                    "met": "Leagues",
                    "APIkey": self.api_key
                }
            )
            response.raise_for_status()
            data = response.json()
            
            return data.get("result", [])
            
        except Exception as e:
            logger.error(f"Error fetching leagues: {e}")
            return []
    
    async def stream_events(self) -> AsyncIterator[GoalEvent]:
        """Stream goal events from WebSocket"""
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
        """Listen to WebSocket messages and detect goals"""
        logger.info("WebSocket listener started")
        
        try:
            async for message in self._ws_connection:
                if not self._connected:
                    break
                
                try:
                    data = json.loads(message)
                    await self._process_ws_message(data)
                except json.JSONDecodeError:
                    logger.warning(f"Invalid JSON from WebSocket: {message[:100]}")
                except Exception as e:
                    logger.error(f"Error processing WebSocket message: {e}")
                    
        except websockets.ConnectionClosed:
            logger.warning("WebSocket connection closed")
            if self._connected:
                await self._reconnect_websocket()
        except Exception as e:
            logger.error(f"WebSocket error: {e}")
    
    async def _reconnect_websocket(self):
        """Attempt to reconnect WebSocket"""
        logger.info("Attempting WebSocket reconnection...")
        
        for attempt in range(5):
            try:
                await asyncio.sleep(2 ** attempt)
                ws_url = f"{self.WS_URL}?APIkey={self.api_key}"
                self._ws_connection = await websockets.connect(ws_url)
                self._ws_task = asyncio.create_task(self._listen_websocket())
                logger.info("WebSocket reconnected")
                return
            except Exception as e:
                logger.warning(f"Reconnection attempt {attempt + 1} failed: {e}")
        
        logger.error("Failed to reconnect WebSocket after 5 attempts")
        self._connected = False
    
    async def _process_ws_message(self, matches: list):
        """Process incoming WebSocket data and detect goals"""
        if not isinstance(matches, list):
            return
        
        for match_data in matches:
            match_id = str(match_data.get("event_key", ""))
            league_id = str(match_data.get("league_key", ""))
            
            if not self._should_process_match(match_id, league_id):
                continue
            
            current_home = self._parse_score(match_data.get("event_final_result", "0 - 0"), "home")
            current_away = self._parse_score(match_data.get("event_final_result", "0 - 0"), "away")
            goalscorers = match_data.get("goalscorers", [])
            
            if match_id in self._match_states:
                state = self._match_states[match_id]
                
                if current_home > state.home_score:
                    goal_event = self._create_goal_event(
                        match_data, Team.HOME, current_home, current_away, goalscorers
                    )
                    if goal_event:
                        await self._event_queue.put(goal_event)
                        await self._notify_goal(goal_event)
                        logger.info(f"GOAL! {match_data.get('event_home_team')} scores! {current_home}-{current_away}")
                
                if current_away > state.away_score:
                    goal_event = self._create_goal_event(
                        match_data, Team.AWAY, current_home, current_away, goalscorers
                    )
                    if goal_event:
                        await self._event_queue.put(goal_event)
                        await self._notify_goal(goal_event)
                        logger.info(f"GOAL! {match_data.get('event_away_team')} scores! {current_home}-{current_away}")
                
                state.home_score = current_home
                state.away_score = current_away
                state.goalscorers = goalscorers
            else:
                self._match_states[match_id] = MatchState(
                    match_id=match_id,
                    home_score=current_home,
                    away_score=current_away,
                    goalscorers=goalscorers
                )
    
    def _should_process_match(self, match_id: str, league_id: str) -> bool:
        """Check if we should process this match"""
        if not self._subscribed_matches and not self._subscribed_leagues:
            return True
        
        if match_id in self._subscribed_matches:
            return True
        
        if league_id in self._subscribed_leagues:
            return True
        
        return False
    
    def _create_goal_event(
        self, 
        match_data: dict, 
        team: Team, 
        home_score: int, 
        away_score: int,
        goalscorers: list
    ) -> Optional[GoalEvent]:
        """Create a GoalEvent from match data"""
        try:
            scorer = None
            minute = 0
            
            if goalscorers:
                latest_goal = goalscorers[-1]
                minute = int(latest_goal.get("time", "0").replace("'", "").replace("+", ""))
                
                if team == Team.HOME and latest_goal.get("home_scorer"):
                    scorer = latest_goal["home_scorer"]
                elif team == Team.AWAY and latest_goal.get("away_scorer"):
                    scorer = latest_goal["away_scorer"]
            
            return GoalEvent(
                match_id=str(match_data.get("event_key", "")),
                timestamp=datetime.utcnow(),
                minute=minute,
                team=team,
                scorer=scorer,
                home_score=home_score,
                away_score=away_score
            )
        except Exception as e:
            logger.error(f"Error creating goal event: {e}")
            return None
    
    def _parse_match(self, data: dict) -> MatchInfo:
        """Parse match data from API response"""
        status_map = {
            "": MatchStatus.NOT_STARTED,
            "Finished": MatchStatus.FINISHED,
            "Half Time": MatchStatus.HALFTIME,
            "Not Started": MatchStatus.NOT_STARTED,
        }
        
        event_status = data.get("event_status", "")
        if event_status.isdigit():
            minute = int(event_status)
            status = MatchStatus.FIRST_HALF if minute <= 45 else MatchStatus.SECOND_HALF
        else:
            status = status_map.get(event_status, MatchStatus.FIRST_HALF)
        
        return MatchInfo(
            match_id=str(data.get("event_key", "")),
            home_team=data.get("event_home_team", "Unknown"),
            away_team=data.get("event_away_team", "Unknown"),
            start_time=self._parse_datetime(data.get("event_date"), data.get("event_time")),
            league=data.get("league_name", "Unknown"),
            status=status,
            home_score=self._parse_score(data.get("event_final_result", "0 - 0"), "home"),
            away_score=self._parse_score(data.get("event_final_result", "0 - 0"), "away"),
            current_minute=int(event_status) if event_status.isdigit() else 0
        )
    
    def _parse_score(self, score_str: str, team: str) -> int:
        """Parse score from string like '2 - 1'"""
        try:
            parts = score_str.split("-")
            if len(parts) == 2:
                if team == "home":
                    return int(parts[0].strip())
                else:
                    return int(parts[1].strip())
        except (ValueError, IndexError):
            pass
        return 0
    
    def _parse_datetime(self, date_str: Optional[str], time_str: Optional[str]) -> datetime:
        """Parse date and time strings"""
        try:
            if date_str and time_str:
                return datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M")
        except ValueError:
            pass
        return datetime.utcnow()
