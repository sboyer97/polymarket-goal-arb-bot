"""Orchestrator - Coordinates all agents and manages task queue"""

import asyncio
from datetime import datetime
from typing import Optional
from dataclasses import dataclass, field

from loguru import logger
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.live import Live

from .base import BaseAgent, Task, AgentMessage, AgentStatus

console = Console()


@dataclass
class ProjectState:
    """Current state of the project optimization"""
    best_params: dict = field(default_factory=dict)
    best_sharpe: float = 0.0
    best_win_rate: float = 0.0
    total_backtests: int = 0
    code_improvements: int = 0
    iteration: int = 0
    start_time: datetime = field(default_factory=datetime.utcnow)


class Orchestrator:
    """
    Orchestrator manages all agents and coordinates their work.
    
    Workflow:
    1. Data Agent collects historical data
    2. Strategy Agent runs backtests with different params
    3. Code Agent optimizes based on findings
    4. Reporter Agent summarizes results
    5. Repeat to continuously improve
    """
    
    def __init__(self):
        self.agents: dict[str, BaseAgent] = {}
        self.task_queue: asyncio.Queue[Task] = asyncio.Queue()
        self.completed_tasks: list[Task] = []
        self.messages: list[AgentMessage] = []
        self.state = ProjectState()
        self._running = False
        self._iteration_complete = asyncio.Event()
    
    def register_agent(self, agent: BaseAgent):
        """Register an agent with the orchestrator"""
        agent.set_orchestrator(self)
        self.agents[agent.name] = agent
        logger.info(f"[Orchestrator] Registered agent: {agent.name}")
    
    async def route_message(self, msg: AgentMessage):
        """Route message to target agent"""
        self.messages.append(msg)
        
        if msg.to_agent == "orchestrator":
            await self.handle_message(msg)
        elif msg.to_agent in self.agents:
            await self.agents[msg.to_agent].inbox.put(msg)
    
    async def handle_message(self, msg: AgentMessage):
        """Handle messages sent to orchestrator"""
        content = msg.content
        
        if isinstance(content, dict):
            action = content.get("action")
            
            if action == "request_task":
                # Assign next task to requesting agent
                agent = self.agents.get(msg.from_agent)
                if agent and not self.task_queue.empty():
                    task = await self.task_queue.get()
                    await self.assign_task_to_agent(agent, task)
            
            elif action == "task_completed":
                task = content.get("task")
                result = content.get("result")
                if task:
                    self.completed_tasks.append(task)
                    await self.on_task_completed(task, result, msg.from_agent)
            
            elif action == "task_failed":
                task = content.get("task")
                error = content.get("error")
                logger.warning(f"[Orchestrator] Task failed: {task.name if task else '?'} - {error}")
            
            elif action == "update_state":
                # Update project state
                for key, value in content.items():
                    if key != "action" and hasattr(self.state, key):
                        setattr(self.state, key, value)
        
        elif isinstance(content, str):
            # Log info messages
            logger.info(f"[{msg.from_agent}] {content}")
    
    async def assign_task_to_agent(self, agent: BaseAgent, task: Task):
        """Assign a task to an agent"""
        msg = AgentMessage(
            from_agent="orchestrator",
            to_agent=agent.name,
            content=task,
            msg_type="task",
        )
        await agent.inbox.put(msg)
    
    async def on_task_completed(self, task: Task, result: any, agent_name: str):
        """Handle task completion and decide next steps"""
        logger.info(f"[Orchestrator] Task completed by {agent_name}: {task.name}")
        
        # Check if iteration is complete
        if task.name == "generate_report":
            self.state.iteration += 1
            self._iteration_complete.set()
    
    async def create_iteration_tasks(self):
        """Create tasks for one optimization iteration"""
        tasks = [
            Task(
                name="collect_data",
                description="Collect and prepare historical match data for backtesting",
                priority=10,
            ),
            Task(
                name="run_backtest",
                description="Run backtests with current parameters",
                priority=9,
            ),
            Task(
                name="optimize_params",
                description="Find optimal parameters using grid search",
                priority=8,
            ),
            Task(
                name="analyze_results",
                description="Analyze backtest results and identify improvements",
                priority=7,
            ),
            Task(
                name="optimize_code",
                description="Review and optimize code based on findings",
                priority=6,
            ),
            Task(
                name="generate_report",
                description="Generate summary report of this iteration",
                priority=5,
            ),
        ]
        
        for task in tasks:
            await self.task_queue.put(task)
        
        return len(tasks)
    
    async def run_iteration(self) -> bool:
        """Run one optimization iteration"""
        self._iteration_complete.clear()
        
        console.print(f"\n[bold cyan]═══ ITERATION {self.state.iteration + 1} ═══[/bold cyan]")
        
        # Create tasks for this iteration
        num_tasks = await self.create_iteration_tasks()
        console.print(f"[dim]Created {num_tasks} tasks[/dim]\n")
        
        # Wait for iteration to complete (with timeout)
        try:
            await asyncio.wait_for(
                self._iteration_complete.wait(),
                timeout=300  # 5 minute timeout
            )
            return True
        except asyncio.TimeoutError:
            logger.warning("[Orchestrator] Iteration timed out")
            return False
    
    async def start(self, max_iterations: int = 3):
        """Start the orchestrator and all agents"""
        self._running = True
        
        console.print(Panel.fit(
            "[bold cyan]🤖 Multi-Agent Strategy Optimizer[/bold cyan]\n"
            f"[dim]Agents: {', '.join(self.agents.keys())}[/dim]\n"
            f"[dim]Max iterations: {max_iterations}[/dim]",
            title="Starting",
        ))
        
        # Start all agents
        agent_tasks = []
        for agent in self.agents.values():
            task = asyncio.create_task(agent.start())
            agent_tasks.append(task)
        
        # Run iterations
        try:
            for i in range(max_iterations):
                if not self._running:
                    break
                
                success = await self.run_iteration()
                
                if success:
                    self.print_status()
                    
                    # Check if we've reached good enough results
                    if self.state.best_sharpe > 2.0 and self.state.best_win_rate > 0.65:
                        console.print("\n[green]✓ Reached target metrics![/green]")
                        break
                
                # Small pause between iterations
                await asyncio.sleep(2)
        
        except KeyboardInterrupt:
            console.print("\n[yellow]Stopping...[/yellow]")
        
        finally:
            await self.stop()
            self.print_final_report()
    
    async def stop(self):
        """Stop all agents"""
        self._running = False
        for agent in self.agents.values():
            await agent.stop()
    
    def print_status(self):
        """Print current status"""
        table = Table(title="Agent Status")
        table.add_column("Agent", style="cyan")
        table.add_column("Status", style="green")
        table.add_column("Current Task")
        table.add_column("Completed")
        
        for agent in self.agents.values():
            status = agent.get_status()
            table.add_row(
                status["name"],
                status["status"],
                status["current_task"] or "-",
                str(status["completed_tasks"]),
            )
        
        console.print(table)
        
        console.print(f"\n[bold]Project State:[/bold]")
        console.print(f"  Iteration: {self.state.iteration}")
        console.print(f"  Best Sharpe: {self.state.best_sharpe:.2f}")
        console.print(f"  Best Win Rate: {self.state.best_win_rate:.1%}")
        console.print(f"  Total Backtests: {self.state.total_backtests}")
        console.print(f"  Best Params: {self.state.best_params}")
    
    def print_final_report(self):
        """Print final report"""
        elapsed = (datetime.utcnow() - self.state.start_time).total_seconds()
        
        console.print("\n" + "=" * 60)
        console.print(Panel.fit(
            f"[bold green]Optimization Complete[/bold green]\n\n"
            f"Total iterations: {self.state.iteration}\n"
            f"Total backtests: {self.state.total_backtests}\n"
            f"Code improvements: {self.state.code_improvements}\n"
            f"Time elapsed: {elapsed:.0f}s\n\n"
            f"[bold]Best Results:[/bold]\n"
            f"  Sharpe Ratio: {self.state.best_sharpe:.2f}\n"
            f"  Win Rate: {self.state.best_win_rate:.1%}\n"
            f"  Parameters: {self.state.best_params}",
            title="Final Report",
        ))
