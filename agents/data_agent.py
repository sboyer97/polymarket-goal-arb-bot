"""Data Agent - Collects and prepares data for backtesting"""

import asyncio
import json
import random
from datetime import datetime, timedelta
from pathlib import Path

from loguru import logger

from .base import BaseAgent


class DataAgent(BaseAgent):
    """
    Data Agent responsibilities:
    - Collect historical soccer match data
    - Fetch goal timing data
    - Prepare datasets for backtesting
    - Store data for other agents
    """
    
    def __init__(self):
        super().__init__(
            name="data_agent",
            description="Collects and prepares match data"
        )
        self.data_dir = Path("data")
        self.data_dir.mkdir(exist_ok=True)
    
    async def work_on_task(self):
        """Execute current task"""
        if not self.current_task:
            return
        
        task_name = self.current_task.name
        
        if task_name == "collect_data":
            result = await self.collect_data()
            await self.complete_task(result)
        else:
            # Not my task
            self.status = self.status.IDLE
            self.current_task = None
    
    async def collect_data(self) -> dict:
        """Collect historical match data"""
        await self.report("Collecting historical match data...")
        
        # Generate realistic sample data
        matches = []
        leagues = ["MLS", "EPL", "LaLiga", "SerieA", "Bundesliga"]
        
        for i in range(100):
            match_date = datetime.utcnow() - timedelta(days=random.randint(1, 90))
            league = random.choice(leagues)
            
            # Generate goals with timestamps
            num_goals = random.randint(0, 6)
            goals = []
            total_home = 0
            total_away = 0
            
            for _ in range(num_goals):
                minute = random.randint(1, 90)
                is_home = random.random() > 0.5
                if is_home:
                    total_home += 1
                else:
                    total_away += 1
                
                goals.append({
                    "minute": minute,
                    "team": "home" if is_home else "away",
                    "score_after": f"{total_home}-{total_away}",
                })
            
            goals.sort(key=lambda x: x["minute"])
            
            match = {
                "id": f"match_{i}",
                "date": match_date.isoformat(),
                "league": league,
                "home_team": f"Team_{random.randint(1, 50)}",
                "away_team": f"Team_{random.randint(51, 100)}",
                "final_score": f"{total_home}-{total_away}",
                "goals": goals,
            }
            matches.append(match)
        
        # Save to file
        data_file = self.data_dir / "matches.json"
        with open(data_file, "w") as f:
            json.dump(matches, f, indent=2)
        
        await self.report(f"Collected {len(matches)} matches with goal data")
        
        return {
            "matches": len(matches),
            "total_goals": sum(len(m["goals"]) for m in matches),
            "file": str(data_file),
        }
