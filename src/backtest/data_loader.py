import json
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional
from dataclasses import dataclass

from loguru import logger

from src.utils.models import GoalEvent, MatchInfo, MatchStatus, Team


@dataclass
class HistoricalMatch:
    """Historical match data with price snapshots"""
    match_info: MatchInfo
    goals: list[GoalEvent]
    price_history: pd.DataFrame  # timestamp, home_win, away_win, draw


class HistoricalDataLoader:
    """Load historical match and market data for backtesting"""
    
    def __init__(self, data_dir: str = "data/historical"):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
    
    def load_matches(self, start_date: datetime, end_date: datetime) -> list[HistoricalMatch]:
        """Load historical matches from files"""
        matches = []
        
        for file_path in self.data_dir.glob("*.json"):
            try:
                match = self._load_match_file(file_path)
                if match and start_date <= match.match_info.start_time <= end_date:
                    matches.append(match)
            except Exception as e:
                logger.warning(f"Failed to load {file_path}: {e}")
        
        matches.sort(key=lambda m: m.match_info.start_time)
        logger.info(f"Loaded {len(matches)} historical matches")
        return matches
    
    def _load_match_file(self, file_path: Path) -> Optional[HistoricalMatch]:
        """Load a single match from JSON file"""
        with open(file_path) as f:
            data = json.load(f)
        
        match_info = MatchInfo(
            match_id=data["match_id"],
            home_team=data["home_team"],
            away_team=data["away_team"],
            start_time=datetime.fromisoformat(data["start_time"]),
            league=data["league"],
            status=MatchStatus.FINISHED,
            home_score=data["final_home_score"],
            away_score=data["final_away_score"]
        )
        
        goals = []
        for g in data.get("goals", []):
            goals.append(GoalEvent(
                match_id=data["match_id"],
                timestamp=datetime.fromisoformat(g["timestamp"]),
                minute=g["minute"],
                team=Team.HOME if g["team"] == "home" else Team.AWAY,
                scorer=g.get("scorer"),
                home_score=g["home_score"],
                away_score=g["away_score"]
            ))
        
        price_df = pd.DataFrame(data.get("price_history", []))
        if not price_df.empty:
            price_df["timestamp"] = pd.to_datetime(price_df["timestamp"])
            price_df.set_index("timestamp", inplace=True)
        
        return HistoricalMatch(
            match_info=match_info,
            goals=goals,
            price_history=price_df
        )
    
    def save_match(self, match: HistoricalMatch):
        """Save a match to JSON file"""
        data = {
            "match_id": match.match_info.match_id,
            "home_team": match.match_info.home_team,
            "away_team": match.match_info.away_team,
            "start_time": match.match_info.start_time.isoformat(),
            "league": match.match_info.league,
            "final_home_score": match.match_info.home_score,
            "final_away_score": match.match_info.away_score,
            "goals": [
                {
                    "timestamp": g.timestamp.isoformat(),
                    "minute": g.minute,
                    "team": g.team.value,
                    "scorer": g.scorer,
                    "home_score": g.home_score,
                    "away_score": g.away_score
                }
                for g in match.goals
            ],
            "price_history": match.price_history.reset_index().to_dict(orient="records") if not match.price_history.empty else []
        }
        
        file_path = self.data_dir / f"{match.match_info.match_id}.json"
        with open(file_path, "w") as f:
            json.dump(data, f, indent=2, default=str)
        
        logger.info(f"Saved match to {file_path}")
    
    def generate_sample_data(self, num_matches: int = 50):
        """Generate sample historical data for testing"""
        import random
        
        teams = [
            ("Manchester United", "Liverpool", "Premier League"),
            ("Real Madrid", "Barcelona", "La Liga"),
            ("Bayern Munich", "Borussia Dortmund", "Bundesliga"),
            ("PSG", "Marseille", "Ligue 1"),
            ("Juventus", "AC Milan", "Serie A"),
            ("Arsenal", "Chelsea", "Premier League"),
            ("Atletico Madrid", "Sevilla", "La Liga"),
            ("Inter Milan", "Roma", "Serie A"),
        ]
        
        base_time = datetime.utcnow() - timedelta(days=30)
        
        for i in range(num_matches):
            home, away, league = random.choice(teams)
            start_time = base_time + timedelta(days=random.randint(0, 30), hours=random.randint(14, 21))
            
            home_score = 0
            away_score = 0
            goals = []
            
            num_goals = random.choices([0, 1, 2, 3, 4, 5], weights=[10, 25, 30, 20, 10, 5])[0]
            
            goal_minutes = sorted(random.sample(range(1, 91), min(num_goals, 90)))
            
            for minute in goal_minutes:
                if random.random() < 0.55:
                    home_score += 1
                    team = Team.HOME
                else:
                    away_score += 1
                    team = Team.AWAY
                
                goals.append(GoalEvent(
                    match_id=f"sample_{i:04d}",
                    timestamp=start_time + timedelta(minutes=minute),
                    minute=minute,
                    team=team,
                    home_score=home_score,
                    away_score=away_score
                ))
            
            price_history = self._generate_price_history(start_time, goals)
            
            match = HistoricalMatch(
                match_info=MatchInfo(
                    match_id=f"sample_{i:04d}",
                    home_team=home,
                    away_team=away,
                    start_time=start_time,
                    league=league,
                    status=MatchStatus.FINISHED,
                    home_score=home_score,
                    away_score=away_score
                ),
                goals=goals,
                price_history=price_history
            )
            
            self.save_match(match)
        
        logger.info(f"Generated {num_matches} sample matches")
    
    def _generate_price_history(self, start_time: datetime, goals: list[GoalEvent]) -> pd.DataFrame:
        """Generate realistic price history with goal impact"""
        import numpy as np
        
        timestamps = pd.date_range(start_time, start_time + timedelta(minutes=95), freq="30s")
        
        home_win = [0.35]
        away_win = [0.35]
        draw = [0.30]
        
        goal_times = {g.timestamp: g for g in goals}
        
        for ts in timestamps[1:]:
            noise = np.random.normal(0, 0.005)
            
            nearest_goal = None
            for gt, g in goal_times.items():
                if abs((ts - gt).total_seconds()) < 10:
                    nearest_goal = g
                    break
            
            if nearest_goal:
                score_diff = nearest_goal.score_diff
                if score_diff > 0:
                    home_win.append(min(0.95, home_win[-1] + 0.12))
                    away_win.append(max(0.02, away_win[-1] - 0.08))
                    draw.append(max(0.02, 1 - home_win[-1] - away_win[-1]))
                elif score_diff < 0:
                    away_win.append(min(0.95, away_win[-1] + 0.12))
                    home_win.append(max(0.02, home_win[-1] - 0.08))
                    draw.append(max(0.02, 1 - home_win[-1] - away_win[-1]))
                else:
                    draw.append(min(0.5, draw[-1] + 0.08))
                    home_win.append(max(0.15, (1 - draw[-1]) / 2))
                    away_win.append(max(0.15, (1 - draw[-1]) / 2))
            else:
                home_win.append(max(0.02, min(0.95, home_win[-1] + noise)))
                away_win.append(max(0.02, min(0.95, away_win[-1] + noise)))
                draw.append(max(0.02, min(0.95, 1 - home_win[-1] - away_win[-1])))
        
        df = pd.DataFrame({
            "timestamp": timestamps,
            "home_win": home_win,
            "away_win": away_win,
            "draw": draw
        })
        df.set_index("timestamp", inplace=True)
        
        return df
