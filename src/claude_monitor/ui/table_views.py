"""Table views for daily and monthly statistics display.

This module provides UI components for displaying aggregated usage data
in table format using Rich library.
"""

import logging
from typing import Any, Dict, List, Optional, Union

from rich.align import Align
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

# Removed theme import - using direct styles
from claude_monitor.utils.formatting import format_currency, format_number

logger = logging.getLogger(__name__)


class TableViewsController:
    """Controller for table-based views (daily, monthly)."""

    def __init__(self, console: Optional[Console] = None):
        """Initialize the table views controller.

        Args:
            console: Optional Console instance for rich output
        """
        self.console = console
        # Define simple styles
        self.key_style = "cyan"
        self.value_style = "white"
        self.accent_style = "yellow"
        self.success_style = "green"
        self.warning_style = "yellow"
        self.header_style = "bold cyan"
        self.table_header_style = "bold"
        self.border_style = "bright_blue"

    def _create_base_table(
        self, title: str, period_column_name: str, period_column_width: int
    ) -> Table:
        """Create a base table with common structure.

        Args:
            title: Table title
            period_column_name: Name for the period column ('Date' or 'Month')
            period_column_width: Width for the period column

        Returns:
            Rich Table object with columns added
        """
        table = Table(
            title=title,
            title_style="bold cyan",
            show_header=True,
            header_style="bold",
            border_style="bright_blue",
            expand=True,
            show_lines=True,
        )

        # Add columns
        table.add_column(
            period_column_name, style=self.key_style, width=period_column_width
        )
        table.add_column("Models", style=self.value_style, width=20)
        table.add_column(
            "Messages", style=self.value_style, justify="right", width=12
        )
        table.add_column("Input", style=self.value_style, justify="right", width=12)
        table.add_column("Output", style=self.value_style, justify="right", width=12)
        table.add_column(
            "Cache Create", style=self.value_style, justify="right", width=12
        )
        table.add_column(
            "Cache Read", style=self.value_style, justify="right", width=12
        )
        table.add_column(
            "Total Tokens", style=self.accent_style, justify="right", width=12
        )
        table.add_column(
            "Cost (USD)", style=self.success_style, justify="right", width=10
        )

        return table

    def _add_data_rows(
        self,
        table: Table,
        data_list: List[Dict[str, Any]],
        period_key: str,
        include_model_analysis: bool = False,
    ) -> None:
        """Add data rows to the table.

        Args:
            table: Table to add rows to
            data_list: List of data dictionaries
            period_key: Key to use for period column ('date' or 'month')
        """
        for data in data_list:
            total_tokens = (
                data["input_tokens"]
                + data["output_tokens"]
                + data["cache_creation_tokens"]
                + data["cache_read_tokens"]
            )
            models_text = self._format_models(data["models_used"])
            messages_text = format_number(data.get("entries_count", 0))
            input_text = format_number(data["input_tokens"])
            output_text = format_number(data["output_tokens"])
            cache_create_text = format_number(data["cache_creation_tokens"])
            cache_read_text = format_number(data["cache_read_tokens"])
            total_tokens_text = format_number(total_tokens)
            cost_text = format_currency(data["total_cost"])

            if include_model_analysis:
                model_analysis = self._format_model_analysis(
                    data.get("models_used", []),
                    data.get("model_breakdowns", {}),
                    data,
                    total_tokens,
                    data.get("total_cost", 0.0),
                )
                models_text = model_analysis["models"]
                messages_text = model_analysis["messages"]
                input_text = model_analysis["input"]
                output_text = model_analysis["output"]
                cache_create_text = model_analysis["cache_create"]
                cache_read_text = model_analysis["cache_read"]
                total_tokens_text = model_analysis["total_tokens"]
                cost_text = model_analysis["cost"]

            table.add_row(
                data[period_key],
                models_text,
                messages_text,
                input_text,
                output_text,
                cache_create_text,
                cache_read_text,
                total_tokens_text,
                cost_text,
            )

    def _add_totals_row(self, table: Table, totals: Dict[str, Any]) -> None:
        """Add totals row to the table.

        Args:
            table: Table to add totals to
            totals: Dictionary with total statistics
        """
        # Add separator
        table.add_row("", "", "", "", "", "", "", "", "")

        # Add totals row
        table.add_row(
            Text("Total", style=self.accent_style),
            "",
            Text(format_number(totals.get("entries_count", 0)), style=self.accent_style),
            Text(format_number(totals["input_tokens"]), style=self.accent_style),
            Text(format_number(totals["output_tokens"]), style=self.accent_style),
            Text(
                format_number(totals["cache_creation_tokens"]), style=self.accent_style
            ),
            Text(format_number(totals["cache_read_tokens"]), style=self.accent_style),
            Text(format_number(totals["total_tokens"]), style=self.accent_style),
            Text(format_currency(totals["total_cost"]), style=self.success_style),
        )

    def create_daily_table(
        self,
        daily_data: List[Dict[str, Any]],
        totals: Dict[str, Any],
        timezone: str = "UTC",
    ) -> Table:
        """Create a daily statistics table.

        Args:
            daily_data: List of daily aggregated data
            totals: Total statistics
            timezone: Timezone for display

        Returns:
            Rich Table object
        """
        # Create base table
        table = self._create_base_table(
            title=f"Claude Code Token Usage Report - Daily ({timezone})",
            period_column_name="Date",
            period_column_width=12,
        )

        # Add data rows
        self._add_data_rows(
            table,
            daily_data,
            "date",
            include_model_analysis=True,
        )

        # Add totals
        self._add_totals_row(table, totals)

        return table

    def create_monthly_table(
        self,
        monthly_data: List[Dict[str, Any]],
        totals: Dict[str, Any],
        timezone: str = "UTC",
    ) -> Table:
        """Create a monthly statistics table.

        Args:
            monthly_data: List of monthly aggregated data
            totals: Total statistics
            timezone: Timezone for display

        Returns:
            Rich Table object
        """
        # Create base table
        table = self._create_base_table(
            title=f"Claude Code Token Usage Report - Monthly ({timezone})",
            period_column_name="Month",
            period_column_width=10,
        )

        # Add data rows
        self._add_data_rows(table, monthly_data, "month", include_model_analysis=True)

        # Add totals
        self._add_totals_row(table, totals)

        return table

    def create_summary_panel(
        self, view_type: str, totals: Dict[str, Any], period: str
    ) -> Panel:
        """Create a summary panel for the table view.

        Args:
            view_type: Type of view ('daily' or 'monthly')
            totals: Total statistics
            period: Period description

        Returns:
            Rich Panel object
        """
        # Create summary text
        summary_lines = [
            f"📊 {view_type.capitalize()} Usage Summary - {period}",
            "",
            f"Total Tokens: {format_number(totals['total_tokens'])}",
            f"Total Cost: {format_currency(totals['total_cost'])}",
            f"Messages: {format_number(totals['entries_count'])}",
        ]

        summary_text = Text("\n".join(summary_lines), style=self.value_style)

        # Create panel
        panel = Panel(
            Align.center(summary_text),
            title="Summary",
            title_align="center",
            border_style=self.border_style,
            expand=False,
            padding=(1, 2),
        )

        return panel

    def _format_models(self, models: List[str]) -> str:
        """Format model names for display.

        Args:
            models: List of model names

        Returns:
            Formatted string of model names
        """
        if not models:
            return "No models"

        # Create bullet list
        if len(models) == 1:
            return models[0]
        elif len(models) <= 3:
            return "\n".join([f"• {model}" for model in models])
        else:
            # Truncate long lists
            first_two = models[:2]
            remaining_count = len(models) - 2
            formatted = "\n".join([f"• {model}" for model in first_two])
            formatted += f"\n• ...and {remaining_count} more"
            return formatted

    def _format_model_analysis(
        self,
        models: List[str],
        model_breakdowns: Dict[str, Dict[str, Any]],
        period_data: Dict[str, Any],
        period_total_tokens: int,
        period_total_cost: float,
    ) -> Dict[str, str]:
        """Format per-model per-column details for daily rows."""
        if not models:
            return {
                "models": "No models",
                "messages": format_number(period_data.get("entries_count", 0)),
                "input": format_number(period_data.get("input_tokens", 0)),
                "output": format_number(period_data.get("output_tokens", 0)),
                "cache_create": format_number(
                    period_data.get("cache_creation_tokens", 0)
                ),
                "cache_read": format_number(period_data.get("cache_read_tokens", 0)),
                "total_tokens": format_number(period_total_tokens),
                "cost": format_currency(period_total_cost),
            }

        model_totals: List[Dict[str, Any]] = []
        for model in models:
            breakdown = model_breakdowns.get(model, {})
            input_tokens = breakdown.get("input_tokens", 0)
            output_tokens = breakdown.get("output_tokens", 0)
            cache_creation_tokens = breakdown.get("cache_creation_tokens", 0)
            cache_read_tokens = breakdown.get("cache_read_tokens", 0)
            model_tokens = (
                input_tokens + output_tokens + cache_creation_tokens + cache_read_tokens
            )
            model_cost = breakdown.get("cost", 0.0)
            model_count = breakdown.get("count", 0)
            model_totals.append(
                {
                    "model": model,
                    "input_tokens": input_tokens,
                    "output_tokens": output_tokens,
                    "cache_creation_tokens": cache_creation_tokens,
                    "cache_read_tokens": cache_read_tokens,
                    "tokens": model_tokens,
                    "cost": model_cost,
                    "count": model_count,
                }
            )

        model_totals.sort(key=lambda item: (-item["tokens"], item["model"]))

        models_lines = []
        input_lines = []
        output_lines = []
        cache_create_lines = []
        cache_read_lines = []
        total_tokens_lines = []
        cost_lines = []
        messages_lines = []
        for item in model_totals:
            total_token_pct = (
                (item["tokens"] / period_total_tokens * 100.0)
                if period_total_tokens > 0
                else 0.0
            )
            cost_pct = (
                (item["cost"] / period_total_cost * 100.0)
                if period_total_cost > 0
                else 0.0
            )
            input_pct = (
                (item["input_tokens"] / period_data.get("input_tokens", 0) * 100.0)
                if period_data.get("input_tokens", 0) > 0
                else 0.0
            )
            output_pct = (
                (item["output_tokens"] / period_data.get("output_tokens", 0) * 100.0)
                if period_data.get("output_tokens", 0) > 0
                else 0.0
            )
            cache_create_pct = (
                (
                    item["cache_creation_tokens"]
                    / period_data.get("cache_creation_tokens", 0)
                    * 100.0
                )
                if period_data.get("cache_creation_tokens", 0) > 0
                else 0.0
            )
            cache_read_pct = (
                (
                    item["cache_read_tokens"]
                    / period_data.get("cache_read_tokens", 0)
                    * 100.0
                )
                if period_data.get("cache_read_tokens", 0) > 0
                else 0.0
            )
            messages_pct = (
                (item["count"] / period_data.get("entries_count", 0) * 100.0)
                if period_data.get("entries_count", 0) > 0
                else 0.0
            )

            models_lines.append(f"• {item['model']}")
            input_lines.append(
                f"{format_number(item['input_tokens'])} ({input_pct:.1f}%)"
            )
            output_lines.append(
                f"{format_number(item['output_tokens'])} ({output_pct:.1f}%)"
            )
            cache_create_lines.append(
                f"{format_number(item['cache_creation_tokens'])} ({cache_create_pct:.1f}%)"
            )
            cache_read_lines.append(
                f"{format_number(item['cache_read_tokens'])} ({cache_read_pct:.1f}%)"
            )
            total_tokens_lines.append(
                f"{format_number(item['tokens'])} ({total_token_pct:.1f}%)"
            )
            cost_lines.append(f"{format_currency(item['cost'])} ({cost_pct:.1f}%)")
            messages_lines.append(
                f"{format_number(item['count'])} ({messages_pct:.1f}%)"
            )

        return {
            "models": "\n".join(models_lines),
            "messages": "\n".join(messages_lines),
            "input": "\n".join(input_lines),
            "output": "\n".join(output_lines),
            "cache_create": "\n".join(cache_create_lines),
            "cache_read": "\n".join(cache_read_lines),
            "total_tokens": "\n".join(total_tokens_lines),
            "cost": "\n".join(cost_lines),
        }

    def create_no_data_display(self, view_type: str) -> Panel:
        """Create a display for when no data is available.

        Args:
            view_type: Type of view ('daily' or 'monthly')

        Returns:
            Rich Panel object
        """
        message = Text(
            f"No {view_type} data found.\n\nTry using Claude Code to generate some usage data.",
            style=self.warning_style,
            justify="center",
        )

        panel = Panel(
            Align.center(message, vertical="middle"),
            title=f"No {view_type.capitalize()} Data",
            title_align="center",
            border_style=self.warning_style,
            expand=True,
            height=10,
        )

        return panel

    def create_aggregate_table(
        self,
        aggregate_data: Union[List[Dict[str, Any]], List[Dict[str, Any]]],
        totals: Dict[str, Any],
        view_type: str,
        timezone: str = "UTC",
    ) -> Table:
        """Create a table for either daily or monthly aggregated data.

        Args:
            aggregate_data: List of aggregated data (daily or monthly)
            totals: Total statistics
            view_type: Type of view ('daily' or 'monthly')
            timezone: Timezone for display

        Returns:
            Rich Table object

        Raises:
            ValueError: If view_type is not 'daily' or 'monthly'
        """
        if view_type == "daily":
            return self.create_daily_table(aggregate_data, totals, timezone)
        elif view_type == "monthly":
            return self.create_monthly_table(aggregate_data, totals, timezone)
        else:
            raise ValueError(f"Invalid view type: {view_type}")

    def display_aggregated_view(
        self,
        data: List[Dict[str, Any]],
        view_mode: str,
        timezone: str,
        plan: str,
        token_limit: int,
        console: Optional[Console] = None,
    ) -> None:
        """Display aggregated view with table and summary.

        Args:
            data: Aggregated data
            view_mode: View type ('daily' or 'monthly')
            timezone: Timezone string
            plan: Plan type
            token_limit: Token limit for the plan
            console: Optional Console instance
        """
        if not data:
            no_data_display = self.create_no_data_display(view_mode)
            if console:
                console.print(no_data_display)
            else:
                print(no_data_display)
            return

        # Calculate totals
        totals = {
            "input_tokens": sum(d["input_tokens"] for d in data),
            "output_tokens": sum(d["output_tokens"] for d in data),
            "cache_creation_tokens": sum(d["cache_creation_tokens"] for d in data),
            "cache_read_tokens": sum(d["cache_read_tokens"] for d in data),
            "total_tokens": sum(
                d["input_tokens"]
                + d["output_tokens"]
                + d["cache_creation_tokens"]
                + d["cache_read_tokens"]
                for d in data
            ),
            "total_cost": sum(d["total_cost"] for d in data),
            "entries_count": sum(d.get("entries_count", 0) for d in data),
        }

        # Determine period for summary
        if view_mode == "daily":
            period = f"{data[0]['date']} to {data[-1]['date']}" if data else "No data"
        else:  # monthly
            period = f"{data[0]['month']} to {data[-1]['month']}" if data else "No data"

        # Create and display summary panel
        summary_panel = self.create_summary_panel(view_mode, totals, period)

        # Create and display table
        table = self.create_aggregate_table(data, totals, view_mode, timezone)

        # Display using console if provided
        if console:
            console.print(summary_panel)
            console.print()
            console.print(table)
        else:
            from rich import print as rprint

            rprint(summary_panel)
            rprint()
            rprint(table)
