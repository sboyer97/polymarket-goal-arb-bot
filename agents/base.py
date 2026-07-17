"""Base Agent class for the multi-agent framework"""

import asyncio
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Optional
import uuid

from loguru import logger


class AgentStatus(Enum):
    IDLE = "idle"
    WORKING = "working"
    WAITING = "waiting"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class Task:
    """A task to be executed by an agent"""
    id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
    name: str = ""
    description: str = ""
    priority: int = 5  # 1-10, higher = more urgent
    assigned_to: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.utcnow)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    status: str = "pending"
    result: Any = None
    error: Optional[str] = None


@dataclass
class AgentMessage:
    """Message between agents"""
    from_agent: str
    to_agent: str
    content: Any
    msg_type: str = "info"  # info, request, response, alert
    timestamp: datetime = field(default_factory=datetime.utcnow)


class BaseAgent(ABC):
    """Base class for all agents"""
    
    def __init__(self, name: str, description: str = ""):
        self.name = name
        self.description = description
        self.status = AgentStatus.IDLE
        self.current_task: Optional[Task] = None
        self.completed_tasks: list[Task] = []
        self.inbox: asyncio.Queue[AgentMessage] = asyncio.Queue()
        self._running = False
        self._orchestrator = None
    
    def set_orchestrator(self, orchestrator):
        """Set reference to orchestrator for communication"""
        self._orchestrator = orchestrator
    
    async def send_message(self, to_agent: str, content: Any, msg_type: str = "info"):
        """Send message to another agent via orchestrator"""
        if self._orchestrator:
            msg = AgentMessage(
                from_agent=self.name,
                to_agent=to_agent,
                content=content,
                msg_type=msg_type,
            )
            await self._orchestrator.route_message(msg)
    
    async def report(self, content: str):
        """Report to orchestrator"""
        await self.send_message("orchestrator", content, "info")
    
    async def request_task(self):
        """Request a new task from orchestrator"""
        await self.send_message("orchestrator", {"action": "request_task"}, "request")
    
    async def start(self):
        """Start the agent loop"""
        self._running = True
        logger.info(f"[{self.name}] Started")
        
        while self._running:
            try:
                # Check inbox
                try:
                    msg = self.inbox.get_nowait()
                    await self.handle_message(msg)
                except asyncio.QueueEmpty:
                    pass
                
                # Work on current task
                if self.current_task and self.status == AgentStatus.WORKING:
                    await self.work_on_task()
                elif self.status == AgentStatus.IDLE:
                    await self.request_task()
                    self.status = AgentStatus.WAITING
                
                await asyncio.sleep(0.5)
                
            except Exception as e:
                logger.error(f"[{self.name}] Error: {e}")
                await asyncio.sleep(1)
    
    async def stop(self):
        """Stop the agent"""
        self._running = False
        logger.info(f"[{self.name}] Stopped")
    
    async def assign_task(self, task: Task):
        """Receive a task assignment"""
        self.current_task = task
        task.assigned_to = self.name
        task.started_at = datetime.utcnow()
        task.status = "in_progress"
        self.status = AgentStatus.WORKING
        logger.info(f"[{self.name}] Assigned task: {task.name}")
    
    async def complete_task(self, result: Any = None):
        """Mark current task as completed"""
        if self.current_task:
            self.current_task.completed_at = datetime.utcnow()
            self.current_task.status = "completed"
            self.current_task.result = result
            self.completed_tasks.append(self.current_task)
            
            await self.send_message(
                "orchestrator",
                {"action": "task_completed", "task": self.current_task, "result": result},
                "response"
            )
            
            logger.info(f"[{self.name}] Completed task: {self.current_task.name}")
            self.current_task = None
            self.status = AgentStatus.IDLE
    
    async def fail_task(self, error: str):
        """Mark current task as failed"""
        if self.current_task:
            self.current_task.status = "failed"
            self.current_task.error = error
            
            await self.send_message(
                "orchestrator",
                {"action": "task_failed", "task": self.current_task, "error": error},
                "alert"
            )
            
            logger.error(f"[{self.name}] Failed task: {self.current_task.name} - {error}")
            self.current_task = None
            self.status = AgentStatus.IDLE
    
    async def handle_message(self, msg: AgentMessage):
        """Handle incoming message"""
        if msg.msg_type == "task" and isinstance(msg.content, Task):
            await self.assign_task(msg.content)
    
    @abstractmethod
    async def work_on_task(self):
        """Work on the current task - implemented by subclasses"""
        pass
    
    def get_status(self) -> dict:
        """Get agent status"""
        return {
            "name": self.name,
            "status": self.status.value,
            "current_task": self.current_task.name if self.current_task else None,
            "completed_tasks": len(self.completed_tasks),
        }
