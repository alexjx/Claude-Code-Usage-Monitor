"""Tests for SessionDisplayComponent."""

from unittest.mock import patch

from claude_monitor.ui.session_display import SessionDisplayComponent


class TestSessionDisplayComponent:
    """Test cases for realtime session screen formatting."""

    def test_format_active_session_screen_uses_tokens_per_second(self) -> None:
        """Burn rate should render in tokens/sec with matching velocity indicator."""
        component = SessionDisplayComponent()

        with (
            patch(
                "claude_monitor.ui.session_display.HeaderManager.create_header",
                return_value=[],
            ),
            patch(
                "claude_monitor.ui.session_display.TokenProgressBar.render",
                return_value="token-bar",
            ),
            patch(
                "claude_monitor.ui.session_display.TimeProgressBar.render",
                return_value="time-bar",
            ),
            patch(
                "claude_monitor.ui.session_display.ModelUsageBar.render",
                return_value="model-bar",
            ),
            patch.object(
                SessionDisplayComponent,
                "_render_wide_progress_bar",
                return_value="wide-bar",
            ),
        ):
            screen = component.format_active_session_screen(
                plan="pro",
                timezone="UTC",
                tokens_used=15000,
                token_limit=200000,
                usage_percentage=7.5,
                tokens_left=185000,
                elapsed_session_minutes=90,
                total_session_minutes=120,
                burn_rate=60.0,
                session_cost=0.45,
                per_model_stats={},
                sent_messages=12,
                entries=[],
                predicted_end_str="14:00",
                reset_time_str="00:00",
                current_time_str="12:30",
            )

        assert any(
            "Burn Rate:" in line
            and "1.0" in line
            and "tokens/sec" in line
            and "➡️" in line
            for line in screen
        )

    def test_format_no_active_session_screen_uses_tokens_per_second(self) -> None:
        """The empty realtime screen should use the same burn-rate unit."""
        component = SessionDisplayComponent()

        with patch(
            "claude_monitor.ui.session_display.HeaderManager.create_header",
            return_value=[],
        ):
            screen = component.format_no_active_session_screen(
                plan="pro",
                timezone="UTC",
                token_limit=200000,
            )

        assert any("Burn Rate:" in line and "tokens/sec" in line for line in screen)
        assert any("Burn Rate:" in line and "0.0" in line for line in screen)
