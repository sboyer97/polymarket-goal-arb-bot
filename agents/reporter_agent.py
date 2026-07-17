"""Reporter Agent - Generates reports and summaries"""

import json
from datetime import datetime
from pathlib import Path

from loguru import logger
from rich.console import Console
from rich.table import Table
from rich.panel import Panel

from .base import BaseAgent

console = Console()


class ReporterAgent(BaseAgent):
    """
    Reporter Agent responsibilities:
    - Generate iteration reports
    - Summarize findings from other agents
    - Create visualizations
    - Save reports to files
    """
    
    def __init__(self):
        super().__init__(
            name="reporter_agent",
            description="Generates reports and summaries"
        )
        self.reports_dir = Path("reports")
        self.reports_dir.mkdir(exist_ok=True)
        self.report_count = 0
    
    async def work_on_task(self):
        """Execute current task"""
        if not self.current_task:
            return
        
        task_name = self.current_task.name
        
        if task_name == "generate_report":
            result = await self.generate_report()
            await self.complete_task(result)
        else:
            self.status = self.status.IDLE
            self.current_task = None
    
    async def generate_report(self) -> dict:
        """Generate a comprehensive report"""
        await self.report("Generating iteration report...")
        
        self.report_count += 1
        
        # Gather data from other agents' outputs
        report_data = await self._gather_data()
        
        # Generate console report
        self._print_console_report(report_data)
        
        # Save to file
        report_file = await self._save_report(report_data)
        
        await self.report(f"Report #{self.report_count} generated: {report_file}")
        
        return {
            "report_number": self.report_count,
            "file": report_file,
        }
    
    async def _gather_data(self) -> dict:
        """Gather data from various sources"""
        data = {
            "timestamp": datetime.utcnow().isoformat(),
            "iteration": self.report_count,
        }
        
        # Load analysis results
        analysis_file = Path("results/analysis.json")
        if analysis_file.exists():
            with open(analysis_file) as f:
                data["analysis"] = json.load(f)
        
        # Load optimal config
        config_file = Path("config/optimal_params.json")
        if config_file.exists():
            with open(config_file) as f:
                data["optimal_config"] = json.load(f)
        
        # Load performance tips
        tips_file = Path("results/performance_tips.json")
        if tips_file.exists():
            with open(tips_file) as f:
                data["performance_tips"] = json.load(f)
        
        return data
    
    def _print_console_report(self, data: dict):
        """Print report to console"""
        console.print("\n")
        console.print(Panel.fit(
            f"[bold cyan]📊 ITERATION {data['iteration']} REPORT[/bold cyan]",
            border_style="cyan",
        ))
        
        # Analysis summary
        analysis = data.get("analysis", {})
        if analysis:
            console.print("\n[bold]Backtest Analysis:[/bold]")
            console.print(f"  Total configurations tested: {analysis.get('total_tests', 0)}")
            console.print(f"  Profitable configurations: {analysis.get('profitable_configs', 0)}")
            
            best = analysis.get("best_config", {})
            if best:
                console.print(f"\n[bold green]Best Configuration:[/bold green]")
                console.print(f"  Sharpe Ratio: {best.get('sharpe_ratio', 0):.2f}")
                console.print(f"  Win Rate: {best.get('win_rate', 0):.1%}")
                console.print(f"  Total PnL: ${best.get('total_pnl', 0):.2f}")
                console.print(f"  Max Drawdown: {best.get('max_drawdown', 0):.1%}")
                
                params = best.get("params", {})
                if params:
                    table = Table(title="Optimal Parameters")
                    table.add_column("Parameter", style="cyan")
                    table.add_column("Value", style="green")
                    
                    for key, value in params.items():
                        table.add_row(key, str(value))
                    
                    console.print(table)
        
        # Insights
        insights = analysis.get("insights", [])
        if insights:
            console.print("\n[bold]Key Insights:[/bold]")
            for insight in insights:
                console.print(f"  • {insight}")
        
        # Performance tips
        tips = data.get("performance_tips", [])
        if tips:
            console.print("\n[bold]Performance Tips:[/bold]")
            for tip in tips[:3]:
                priority_color = "red" if tip["priority"] == "high" else "yellow" if tip["priority"] == "medium" else "dim"
                console.print(f"  [{priority_color}][{tip['priority'].upper()}][/{priority_color}] {tip['area']}: {tip['tip']}")
        
        console.print("\n" + "─" * 50)
    
    async def _save_report(self, data: dict) -> str:
        """Save report to file"""
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        report_file = self.reports_dir / f"report_{timestamp}.json"
        
        with open(report_file, "w") as f:
            json.dump(data, f, indent=2, default=str)
        
        # Also save markdown version
        md_file = self.reports_dir / f"report_{timestamp}.md"
        
        analysis = data.get("analysis", {})
        best = analysis.get("best_config", {})
        params = best.get("params", {})
        
        md_content = f"""# Strategy Optimization Report

**Generated:** {data['timestamp']}  
**Iteration:** {data['iteration']}

## Summary

- **Configurations Tested:** {analysis.get('total_tests', 0)}
- **Profitable Configs:** {analysis.get('profitable_configs', 0)}

## Best Configuration

| Metric | Value |
|--------|-------|
| Sharpe Ratio | {best.get('sharpe_ratio', 0):.2f} |
| Win Rate | {best.get('win_rate', 0):.1%} |
| Total PnL | ${best.get('total_pnl', 0):.2f} |
| Max Drawdown | {best.get('max_drawdown', 0):.1%} |

## Optimal Parameters

| Parameter | Value |
|-----------|-------|
"""
        for key, value in params.items():
            md_content += f"| {key} | {value} |\n"
        
        md_content += "\n## Insights\n\n"
        for insight in analysis.get("insights", []):
            md_content += f"- {insight}\n"
        
        with open(md_file, "w") as f:
            f.write(md_content)
        
        return str(report_file)
