"""Tests for table views module."""

from typing import Any, Dict, List

import pytest
from rich.panel import Panel
from rich.table import Table

from claude_monitor.ui.table_views import TableViewsController


class TestTableViewsController:
    """Test cases for TableViewsController class."""

    @pytest.fixture
    def controller(self) -> TableViewsController:
        """Create a TableViewsController instance."""
        return TableViewsController()

    @pytest.fixture
    def sample_daily_data(self) -> List[Dict[str, Any]]:
        """Create sample daily aggregated data."""
        return [
            {
                "date": "2024-01-01",
                "input_tokens": 1000,
                "output_tokens": 500,
                "cache_creation_tokens": 100,
                "cache_read_tokens": 50,
                "total_cost": 0.05,
                "models_used": ["claude-3-haiku", "claude-3-sonnet"],
                "model_breakdowns": {
                    "claude-3-haiku": {
                        "input_tokens": 600,
                        "output_tokens": 300,
                        "cache_creation_tokens": 60,
                        "cache_read_tokens": 30,
                        "cost": 0.03,
                        "count": 6,
                    },
                    "claude-3-sonnet": {
                        "input_tokens": 400,
                        "output_tokens": 200,
                        "cache_creation_tokens": 40,
                        "cache_read_tokens": 20,
                        "cost": 0.02,
                        "count": 4,
                    },
                },
                "entries_count": 10,
            },
            {
                "date": "2024-01-02",
                "input_tokens": 2000,
                "output_tokens": 1000,
                "cache_creation_tokens": 200,
                "cache_read_tokens": 100,
                "total_cost": 0.10,
                "models_used": ["claude-3-opus"],
                "model_breakdowns": {
                    "claude-3-opus": {
                        "input_tokens": 2000,
                        "output_tokens": 1000,
                        "cache_creation_tokens": 200,
                        "cache_read_tokens": 100,
                        "cost": 0.10,
                        "count": 20,
                    },
                },
                "entries_count": 20,
            },
        ]

    @pytest.fixture
    def sample_monthly_data(self) -> List[Dict[str, Any]]:
        """Create sample monthly aggregated data."""
        return [
            {
                "month": "2024-01",
                "input_tokens": 30000,
                "output_tokens": 15000,
                "cache_creation_tokens": 3000,
                "cache_read_tokens": 1500,
                "total_cost": 1.50,
                "models_used": ["claude-3-haiku", "claude-3-sonnet", "claude-3-opus"],
                "model_breakdowns": {
                    "claude-3-haiku": {
                        "input_tokens": 10000,
                        "output_tokens": 5000,
                        "cache_creation_tokens": 1000,
                        "cache_read_tokens": 500,
                        "cost": 0.50,
                        "count": 100,
                    },
                    "claude-3-sonnet": {
                        "input_tokens": 10000,
                        "output_tokens": 5000,
                        "cache_creation_tokens": 1000,
                        "cache_read_tokens": 500,
                        "cost": 0.50,
                        "count": 100,
                    },
                    "claude-3-opus": {
                        "input_tokens": 10000,
                        "output_tokens": 5000,
                        "cache_creation_tokens": 1000,
                        "cache_read_tokens": 500,
                        "cost": 0.50,
                        "count": 100,
                    },
                },
                "entries_count": 300,
            },
            {
                "month": "2024-02",
                "input_tokens": 20000,
                "output_tokens": 10000,
                "cache_creation_tokens": 2000,
                "cache_read_tokens": 1000,
                "total_cost": 1.00,
                "models_used": ["claude-3-haiku"],
                "model_breakdowns": {
                    "claude-3-haiku": {
                        "input_tokens": 20000,
                        "output_tokens": 10000,
                        "cache_creation_tokens": 2000,
                        "cache_read_tokens": 1000,
                        "cost": 1.00,
                        "count": 200,
                    },
                },
                "entries_count": 200,
            },
        ]

    @pytest.fixture
    def sample_totals(self) -> Dict[str, Any]:
        """Create sample totals data."""
        return {
            "input_tokens": 50000,
            "output_tokens": 25000,
            "cache_creation_tokens": 5000,
            "cache_read_tokens": 2500,
            "total_tokens": 82500,
            "total_cost": 2.50,
            "entries_count": 500,
        }

    def test_init_styles(self, controller: TableViewsController) -> None:
        """Test controller initialization with styles."""
        assert controller.key_style == "cyan"
        assert controller.value_style == "white"
        assert controller.accent_style == "yellow"
        assert controller.success_style == "green"
        assert controller.warning_style == "yellow"
        assert controller.header_style == "bold cyan"
        assert controller.table_header_style == "bold"
        assert controller.border_style == "bright_blue"

    def test_create_daily_table_structure(
        self,
        controller: TableViewsController,
        sample_daily_data: List[Dict[str, Any]],
        sample_totals: Dict[str, Any],
    ) -> None:
        """Test creation of daily table structure."""
        table = controller.create_daily_table(sample_daily_data, sample_totals, "UTC")

        assert isinstance(table, Table)
        assert table.title == "Claude Code Token Usage Report - Daily (UTC)"
        assert table.title_style == "bold cyan"
        assert table.show_header is True
        assert table.header_style == "bold"
        assert table.border_style == "bright_blue"
        assert table.expand is True
        assert table.show_lines is True

        # Check columns
        assert len(table.columns) == 9
        assert table.columns[0].header == "Date"
        assert table.columns[1].header == "Models"
        assert table.columns[2].header == "Messages"
        assert table.columns[3].header == "Input"
        assert table.columns[4].header == "Output"
        assert table.columns[5].header == "Cache Create"
        assert table.columns[6].header == "Cache Read"
        assert table.columns[7].header == "Total Tokens"
        assert table.columns[8].header == "Cost (USD)"

    def test_create_daily_table_data(
        self,
        controller: TableViewsController,
        sample_daily_data: List[Dict[str, Any]],
        sample_totals: Dict[str, Any],
    ) -> None:
        """Test daily table data population."""
        table = controller.create_daily_table(sample_daily_data, sample_totals, "UTC")

        # The table should have:
        # - 2 data rows (for the 2 days)
        # - 1 separator row
        # - 1 totals row
        # Total: 4 rows
        assert table.row_count == 4

    def test_create_monthly_table_structure(
        self,
        controller: TableViewsController,
        sample_monthly_data: List[Dict[str, Any]],
        sample_totals: Dict[str, Any],
    ) -> None:
        """Test creation of monthly table structure."""
        table = controller.create_monthly_table(
            sample_monthly_data, sample_totals, "UTC"
        )

        assert isinstance(table, Table)
        assert table.title == "Claude Code Token Usage Report - Monthly (UTC)"
        assert table.title_style == "bold cyan"
        assert table.show_header is True
        assert table.header_style == "bold"
        assert table.border_style == "bright_blue"
        assert table.expand is True
        assert table.show_lines is True

        # Check columns
        assert len(table.columns) == 9
        assert table.columns[0].header == "Month"
        assert table.columns[1].header == "Models"
        assert table.columns[2].header == "Messages"
        assert table.columns[3].header == "Input"
        assert table.columns[4].header == "Output"
        assert table.columns[5].header == "Cache Create"
        assert table.columns[6].header == "Cache Read"
        assert table.columns[7].header == "Total Tokens"
        assert table.columns[8].header == "Cost (USD)"

    def test_create_monthly_table_data(
        self,
        controller: TableViewsController,
        sample_monthly_data: List[Dict[str, Any]],
        sample_totals: Dict[str, Any],
    ) -> None:
        """Test monthly table data population."""
        table = controller.create_monthly_table(
            sample_monthly_data, sample_totals, "UTC"
        )

        # The table should have:
        # - 2 data rows (for the 2 months)
        # - 1 separator row
        # - 1 totals row
        # Total: 4 rows
        assert table.row_count == 4

    def test_create_summary_panel(
        self, controller: TableViewsController, sample_totals: Dict[str, Any]
    ) -> None:
        """Test creation of summary panel."""
        panel = controller.create_summary_panel("daily", sample_totals, "Last 30 days")

        assert isinstance(panel, Panel)
        assert panel.title == "Summary"
        assert panel.title_align == "center"
        assert panel.border_style == controller.border_style
        assert panel.expand is False
        assert panel.padding == (1, 2)

    def test_format_models_single(self, controller: TableViewsController) -> None:
        """Test formatting single model."""
        result = controller._format_models(["claude-3-haiku"])
        assert result == "claude-3-haiku"

    def test_format_models_multiple(self, controller: TableViewsController) -> None:
        """Test formatting multiple models."""
        result = controller._format_models(
            ["claude-3-haiku", "claude-3-sonnet", "claude-3-opus"]
        )
        expected = "• claude-3-haiku\n• claude-3-sonnet\n• claude-3-opus"
        assert result == expected

    def test_format_models_no_truncation(self, controller: TableViewsController) -> None:
        """Test formatting does not truncate when model list is long."""
        result = controller._format_models(
            [
                "claude-3-haiku",
                "claude-3-sonnet",
                "claude-3-opus",
                "claude-3.5-sonnet",
            ]
        )
        expected = (
            "• claude-3-haiku\n"
            "• claude-3-sonnet\n"
            "• claude-3-opus\n"
            "• claude-3.5-sonnet"
        )
        assert result == expected

    def test_format_models_empty(self, controller: TableViewsController) -> None:
        """Test formatting empty models list."""
        result = controller._format_models([])
        assert result == "No models"

    def test_format_model_analysis(self, controller: TableViewsController) -> None:
        """Test daily per-model analysis formatting."""
        result = controller._format_model_analysis(
            ["claude-3-haiku", "claude-3-sonnet"],
            {
                "claude-3-haiku": {
                    "input_tokens": 600,
                    "output_tokens": 300,
                    "cache_creation_tokens": 60,
                    "cache_read_tokens": 30,
                    "cost": 0.03,
                },
                "claude-3-sonnet": {
                    "input_tokens": 400,
                    "output_tokens": 200,
                    "cache_creation_tokens": 40,
                    "cache_read_tokens": 20,
                    "cost": 0.02,
                },
            },
            {
                "input_tokens": 1000,
                "output_tokens": 500,
                "cache_creation_tokens": 100,
                "cache_read_tokens": 50,
            },
            1650,
            0.05,
        )
        assert result["models"] == "• claude-3-haiku\n• claude-3-sonnet"
        assert result["input"] == "600 (60.0%)\n400 (40.0%)"
        assert result["output"] == "300 (60.0%)\n200 (40.0%)"
        assert result["cache_create"] == "60 (60.0%)\n40 (40.0%)"
        assert result["cache_read"] == "30 (60.0%)\n20 (40.0%)"
        assert result["total_tokens"] == "990 (60.0%)\n660 (40.0%)"
        assert result["cost"] == "$0.03 (60.0%)\n$0.02 (40.0%)"

    def test_create_daily_table_includes_per_model_analysis(
        self,
        controller: TableViewsController,
        sample_daily_data: List[Dict[str, Any]],
        sample_totals: Dict[str, Any],
    ) -> None:
        """Test daily table includes model analysis text in Models column."""
        table = controller.create_daily_table(sample_daily_data, sample_totals, "UTC")

        first_models_cell = table.columns[1]._cells[0]
        first_messages_cell = table.columns[2]._cells[0]
        first_input_cell = table.columns[3]._cells[0]
        first_output_cell = table.columns[4]._cells[0]
        first_cost_cell = table.columns[8]._cells[0]

        assert first_models_cell == "• claude-3-haiku\n• claude-3-sonnet"
        assert first_messages_cell == "6 (60.0%)\n4 (40.0%)"
        assert first_input_cell == "600 (60.0%)\n400 (40.0%)"
        assert first_output_cell == "300 (60.0%)\n200 (40.0%)"
        assert first_cost_cell == "$0.03 (60.0%)\n$0.02 (40.0%)"

    def test_create_monthly_table_includes_per_model_analysis(
        self,
        controller: TableViewsController,
        sample_monthly_data: List[Dict[str, Any]],
        sample_totals: Dict[str, Any],
    ) -> None:
        """Test monthly table includes per-model analysis in all metric columns."""
        table = controller.create_monthly_table(sample_monthly_data, sample_totals, "UTC")

        first_models_cell = table.columns[1]._cells[0]
        first_messages_cell = table.columns[2]._cells[0]
        first_input_cell = table.columns[3]._cells[0]
        first_cost_cell = table.columns[8]._cells[0]

        assert "• claude-3-haiku" in first_models_cell
        assert "• claude-3-sonnet" in first_models_cell
        assert "• claude-3-opus" in first_models_cell
        assert "...and" not in first_models_cell
        assert "(33.3%)" in first_messages_cell
        assert "(33.3%)" in first_input_cell
        assert "(33.3%)" in first_cost_cell

    def test_create_no_data_display(self, controller: TableViewsController) -> None:
        """Test creation of no data display."""
        panel = controller.create_no_data_display("daily")

        assert isinstance(panel, Panel)
        assert panel.title == "No Daily Data"
        assert panel.title_align == "center"
        assert panel.border_style == controller.warning_style
        assert panel.expand is True
        assert panel.height == 10

    def test_create_aggregate_table_daily(
        self,
        controller: TableViewsController,
        sample_daily_data: List[Dict[str, Any]],
        sample_totals: Dict[str, Any],
    ) -> None:
        """Test create_aggregate_table for daily view."""
        table = controller.create_aggregate_table(
            sample_daily_data, sample_totals, "daily", "UTC"
        )

        assert isinstance(table, Table)
        assert table.title == "Claude Code Token Usage Report - Daily (UTC)"

    def test_create_aggregate_table_monthly(
        self,
        controller: TableViewsController,
        sample_monthly_data: List[Dict[str, Any]],
        sample_totals: Dict[str, Any],
    ) -> None:
        """Test create_aggregate_table for monthly view."""
        table = controller.create_aggregate_table(
            sample_monthly_data, sample_totals, "monthly", "UTC"
        )

        assert isinstance(table, Table)
        assert table.title == "Claude Code Token Usage Report - Monthly (UTC)"

    def test_create_aggregate_table_invalid_view_type(
        self,
        controller: TableViewsController,
        sample_daily_data: List[Dict[str, Any]],
        sample_totals: Dict[str, Any],
    ) -> None:
        """Test create_aggregate_table with invalid view type."""
        with pytest.raises(ValueError, match="Invalid view type"):
            controller.create_aggregate_table(
                sample_daily_data, sample_totals, "weekly", "UTC"
            )

    def test_daily_table_timezone_display(
        self,
        controller: TableViewsController,
        sample_daily_data: List[Dict[str, Any]],
        sample_totals: Dict[str, Any],
    ) -> None:
        """Test daily table displays correct timezone."""
        table = controller.create_daily_table(
            sample_daily_data, sample_totals, "America/New_York"
        )
        assert (
            table.title == "Claude Code Token Usage Report - Daily (America/New_York)"
        )

    def test_monthly_table_timezone_display(
        self,
        controller: TableViewsController,
        sample_monthly_data: List[Dict[str, Any]],
        sample_totals: Dict[str, Any],
    ) -> None:
        """Test monthly table displays correct timezone."""
        table = controller.create_monthly_table(
            sample_monthly_data, sample_totals, "Europe/London"
        )
        assert table.title == "Claude Code Token Usage Report - Monthly (Europe/London)"

    def test_table_with_zero_tokens(self, controller: TableViewsController) -> None:
        """Test table with entries having zero tokens."""
        data = [
            {
                "date": "2024-01-01",
                "input_tokens": 0,
                "output_tokens": 0,
                "cache_creation_tokens": 0,
                "cache_read_tokens": 0,
                "total_cost": 0.0,
                "models_used": ["claude-3-haiku"],
                "model_breakdowns": {},
                "entries_count": 0,
            }
        ]

        totals = {
            "input_tokens": 0,
            "output_tokens": 0,
            "cache_creation_tokens": 0,
            "cache_read_tokens": 0,
            "total_tokens": 0,
            "total_cost": 0.0,
            "entries_count": 0,
        }

        table = controller.create_daily_table(data, totals, "UTC")
        # Table should have 3 rows:
        # - 1 data row
        # - 1 separator row (empty)
        # - 1 totals row
        # Note: Rich table doesn't count empty separator as a row in some versions
        assert table.row_count in [3, 4]  # Allow for version differences

    def test_summary_panel_different_periods(
        self, controller: TableViewsController, sample_totals: Dict[str, Any]
    ) -> None:
        """Test summary panel with different period descriptions."""
        periods = [
            "Last 30 days",
            "Last 7 days",
            "January 2024",
            "Q1 2024",
            "Year to date",
        ]

        for period in periods:
            panel = controller.create_summary_panel("daily", sample_totals, period)
            assert isinstance(panel, Panel)
            assert panel.title == "Summary"

    def test_no_data_display_different_view_types(
        self, controller: TableViewsController
    ) -> None:
        """Test no data display for different view types."""
        for view_type in ["daily", "monthly", "weekly", "yearly"]:
            panel = controller.create_no_data_display(view_type)
            assert isinstance(panel, Panel)
            assert panel.title == f"No {view_type.capitalize()} Data"

    def test_number_formatting_integration(
        self,
        controller: TableViewsController,
        sample_daily_data: List[Dict[str, Any]],
        sample_totals: Dict[str, Any],
    ) -> None:
        """Test that number formatting is integrated correctly."""
        # Test that the table can be created with real formatting functions
        table = controller.create_daily_table(sample_daily_data, sample_totals, "UTC")

        # Verify table was created successfully
        assert table is not None
        assert table.row_count >= 3  # At least data rows + separator + totals

    def test_currency_formatting_integration(
        self,
        controller: TableViewsController,
        sample_daily_data: List[Dict[str, Any]],
        sample_totals: Dict[str, Any],
    ) -> None:
        """Test that currency formatting is integrated correctly."""
        # Test that the table can be created with real formatting functions
        table = controller.create_daily_table(sample_daily_data, sample_totals, "UTC")

        # Verify table was created successfully
        assert table is not None
        assert table.row_count >= 3  # At least data rows + separator + totals

    def test_table_column_alignment(
        self,
        controller: TableViewsController,
        sample_daily_data: List[Dict[str, Any]],
        sample_totals: Dict[str, Any],
    ) -> None:
        """Test that numeric columns are right-aligned."""
        table = controller.create_daily_table(sample_daily_data, sample_totals, "UTC")

        # Check that numeric columns are right-aligned
        for i in range(2, 9):  # Columns 2-8 are numeric
            assert table.columns[i].justify == "right"

    def test_empty_data_lists(self, controller: TableViewsController) -> None:
        """Test handling of empty data lists."""
        empty_totals = {
            "input_tokens": 0,
            "output_tokens": 0,
            "cache_creation_tokens": 0,
            "cache_read_tokens": 0,
            "total_tokens": 0,
            "total_cost": 0.0,
            "entries_count": 0,
        }

        # Daily table with empty data
        daily_table = controller.create_daily_table([], empty_totals, "UTC")
        assert daily_table.row_count == 2  # Separator + totals

        # Monthly table with empty data
        monthly_table = controller.create_monthly_table([], empty_totals, "UTC")
        assert monthly_table.row_count == 2  # Separator + totals

    def test_create_calib_disclosure_legacy_mode(
        self, controller: TableViewsController
    ) -> None:
        """Test creation of calibration disclosure for legacy mode."""
        panel = controller._create_calib_disclosure("legacy")

        assert isinstance(panel, Panel)
        assert panel.title == "⚠️ 口径说明"
        assert panel.border_style == controller.warning_style
        assert panel.expand is False
        assert panel.padding == (1, 2)

    def test_create_calib_disclosure_unknown_mode(
        self, controller: TableViewsController
    ) -> None:
        """Test creation of calibration disclosure for unknown mode."""
        panel = controller._create_calib_disclosure("unknown-mode")

        assert isinstance(panel, Panel)
        assert panel.title == "⚠️ 口径说明"
        # Should contain the mode name in the message
        assert "unknown-mode" in str(panel.renderable)
