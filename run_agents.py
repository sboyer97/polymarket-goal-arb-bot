#!/usr/bin/env python3
"""
Multi-Agent Strategy Optimizer

Launches all agents to autonomously optimize the soccer trading strategy.

Usage:
    python run_agents.py [--iterations N]
"""

import asyncio
import sys
from pathlib import Path

import click
from loguru import logger
from rich.console import Console

# Setup logging
logger.remove()
logger.add(
    sys.stderr,
    format="<dim>{time:HH:mm:ss}</dim> | <level>{level: <7}</level> | <cyan>{name}</cyan> - <level>{message}</level>",
    level="INFO",
)
logger.add(
    "logs/agents_{time}.log",
    rotation="1 day",
    level="DEBUG",
)

# Create directories
Path("logs").mkdir(exist_ok=True)
Path("data").mkdir(exist_ok=True)
Path("results").mkdir(exist_ok=True)
Path("reports").mkdir(exist_ok=True)

console = Console()


@click.command()
@click.option("--iterations", "-i", default=3, help="Number of optimization iterations")
@click.option("--verbose", "-v", is_flag=True, help="Verbose output")
def main(iterations: int, verbose: bool):
    """Run the multi-agent strategy optimizer"""
    
    if verbose:
        logger.remove()
        logger.add(sys.stderr, level="DEBUG")
    
    console.print("""
[bold cyan]
╔═══════════════════════════════════════════════════════════╗
║     🤖 Multi-Agent Strategy Optimizer                     ║
║                                                           ║
║     Agents:                                               ║
║     • Data Agent      - Collects match data               ║
║     • Strategy Agent  - Optimizes parameters              ║
║     • Code Agent      - Improves codebase                 ║
║     • Reporter Agent  - Generates reports                 ║
║                                                           ║
╚═══════════════════════════════════════════════════════════╝
[/bold cyan]
""")
    
    asyncio.run(run_optimization(iterations))


async def run_optimization(iterations: int):
    """Run the optimization loop"""
    from agents import (
        Orchestrator,
        DataAgent,
        StrategyAgent,
        CodeAgent,
        ReporterAgent,
    )
    
    # Create orchestrator and agents
    orchestrator = Orchestrator()
    
    agents = [
        DataAgent(),
        StrategyAgent(),
        CodeAgent(),
        ReporterAgent(),
    ]
    
    for agent in agents:
        orchestrator.register_agent(agent)
    
    # Start optimization
    try:
        await orchestrator.start(max_iterations=iterations)
    except KeyboardInterrupt:
        console.print("\n[yellow]Interrupted by user[/yellow]")
        await orchestrator.stop()
    
    console.print("\n[green]✓ Optimization complete![/green]")
    console.print("[dim]Check the 'reports' folder for detailed results.[/dim]\n")


if __name__ == "__main__":
    main()
