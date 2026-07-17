import asyncio
import random
from typing import AsyncIterator, Optional
from datetime import datetime, timedelta

from loguru import logger

from src.utils.models import GoalEvent, MatchInfo, MatchStatus, Team
from .base import DataProvider


class MockDataProvider(DataProvider):
    """Mock data provider for testing and backtesting"""
    
    def __init__(self, simulate_goals: bool = True, goal_frequency_seconds: float = 30.0):
        super().__init__()
        self.simulate_goals = simulate_goals
        self.goal_frequency = goal_frequency_seconds
        self._mock_matches: dict[str, MatchInfo] = {}
        self._event_queue: asyncio.Queue[GoalEvent] = asyncio.Queue()
        self._simulation_task: Optional[asyncio.Task] = None
    
    async def connect(self) -> bool:
        """Connect (initialize mock data)"""
        self._create_mock_matches()
        self._connected = True
        
        if self.simulate_goals:
            self._simulation_task = asyncio.create_task(self._simulate_goals())
        
        logger.info("Mock data provider connected")
        return True
    
    async def disconnect(self):
        """Disconnect"""
        if self._simulation_task:
            self._simulation_task.cancel()
        self._connected = False
        logger.info("Mock data provider disconnected")
    
    async def subscribe_match(self, match_id: str):
        """Subscribe to a mock match"""
        logger.info(f"[MOCK] Subscribed to match: {match_id}")
    
    async def unsubscribe_match(self, match_id: str):
        """Unsubscribe from a mock match"""
        logger.info(f"[MOCK] Unsubscribed from match: {match_id}")
    
    async def get_live_matches(self) -> list[MatchInfo]:
        """Get mock live matches"""
        return list(self._mock_matches.values())
    
    async def get_match_info(self, match_id: str) -> Optional[MatchInfo]:
        """Get mock match info"""
        return self._mock_matches.get(match_id)
    
    async def stream_events(self) -> AsyncIterator[GoalEvent]:
        """Stream mock goal events"""
        while self._connected:
            try:
                event = await asyncio.wait_for(
                    self._event_queue.get(),
                    timeout=1.0
                )
                yield event
            except asyncio.TimeoutError:
                continue
    
    def inject_goal(self, match_id: str, team: Team, scorer: str = "Test Player"):
        """Manually inject a goal event (for testing)"""
        if match_id not in self._mock_matches:
            logger.warning(f"Match {match_id} not found")
            return
        
        match = self._mock_matches[match_id]
        
        if team == Team.HOME:
            match.home_score += 1
        else:
            match.away_score += 1
        
        match.current_minute += random.randint(1, 5)
        
        event = GoalEvent(
            match_id=match_id,
            timestamp=datetime.utcnow(),
            minute=match.current_minute,
            team=team,
            scorer=scorer,
            home_score=match.home_score,
            away_score=match.away_score
        )
        
        self._event_queue.put_nowait(event)
        logger.info(f"[MOCK] Goal injected: {match.home_team} {match.home_score} - {match.away_score} {match.away_team}")
    
    def _create_mock_matches(self):
        """Create some mock matches"""
        mock_data = [
            ("match_001", "Manchester United", "Liverpool", "Premier League"),
            ("match_002", "Real Madrid", "Barcelona", "La Liga"),
            ("match_003", "Bayern Munich", "Borussia Dortmund", "Bundesliga"),
            ("match_004", "PSG", "Marseille", "Ligue 1"),
            ("match_005", "Juventus", "AC Milan", "Serie A"),
        ]
        
        for match_id, home, away, league in mock_data:
            self._mock_matches[match_id] = MatchInfo(
                match_id=match_id,
                home_team=home,
                away_team=away,
                start_time=datetime.utcnow() - timedelta(minutes=random.randint(10, 60)),
                league=league,
                status=MatchStatus.FIRST_HALF if random.random() > 0.5 else MatchStatus.SECOND_HALF,
                home_score=random.randint(0, 2),
                away_score=random.randint(0, 2),
                current_minute=random.randint(15, 75)
            )
    
    async def _simulate_goals(self):
        """Simulate random goals"""
        while self._connected:
            await asyncio.sleep(self.goal_frequency)
            
            if not self._mock_matches:
                continue
            
            match_id = random.choice(list(self._mock_matches.keys()))
            team = random.choice([Team.HOME, Team.AWAY])
            
            self.inject_goal(match_id, team, f"Player {random.randint(1, 11)}")
