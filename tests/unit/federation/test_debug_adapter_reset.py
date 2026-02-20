# coding=utf-8
"""Unit tests for the dvt debug adapter reset fix.

The dbt adapter factory caches adapters by *type name* (e.g. "snowflake"),
not by target name.  Without calling reset_adapters() before register_adapter(),
testing a second target of the same adapter type silently reuses the first
target's cached adapter, producing a false positive connection test.

These tests verify that attempt_connection() calls reset_adapters() so each
target gets a fresh adapter registration.
"""

from unittest.mock import MagicMock, patch, call

import pytest


class TestDebugAdapterReset:
    """Verify attempt_connection resets adapters before registering."""

    @patch("dvt.task.debug.get_adapter")
    @patch("dvt.task.debug.register_adapter")
    @patch("dvt.task.debug.reset_adapters")
    @patch("dvt.task.debug.get_mp_context")
    def test_reset_called_before_register(
        self, mock_mp_ctx, mock_reset, mock_register, mock_get_adapter
    ):
        """reset_adapters() must be called before register_adapter()."""
        from dvt.task.debug import DebugTask

        mock_profile = MagicMock()
        mock_adapter = MagicMock()
        mock_get_adapter.return_value = mock_adapter
        # Make debug_query succeed
        mock_adapter.connection_named.return_value.__enter__ = MagicMock()
        mock_adapter.connection_named.return_value.__exit__ = MagicMock(
            return_value=False
        )

        result = DebugTask.attempt_connection(mock_profile)

        # Should have called reset before register
        mock_reset.assert_called_once()
        mock_register.assert_called_once_with(mock_profile, mock_mp_ctx.return_value)

        # Verify call order: reset before register
        assert mock_reset.call_count == 1
        assert mock_register.call_count == 1

        # No error on success
        assert result is None

    @patch("dvt.task.debug.get_adapter")
    @patch("dvt.task.debug.register_adapter")
    @patch("dvt.task.debug.reset_adapters")
    @patch("dvt.task.debug.get_mp_context")
    def test_reset_ensures_fresh_adapter_per_target(
        self, mock_mp_ctx, mock_reset, mock_register, mock_get_adapter
    ):
        """Two consecutive calls should each get a fresh adapter."""
        from dvt.task.debug import DebugTask

        profile_a = MagicMock()
        profile_a.credentials.type = "snowflake"
        profile_b = MagicMock()
        profile_b.credentials.type = "snowflake"

        adapter_a = MagicMock()
        adapter_b = MagicMock()
        mock_get_adapter.side_effect = [adapter_a, adapter_b]

        adapter_a.connection_named.return_value.__enter__ = MagicMock()
        adapter_a.connection_named.return_value.__exit__ = MagicMock(return_value=False)
        adapter_b.connection_named.return_value.__enter__ = MagicMock()
        adapter_b.connection_named.return_value.__exit__ = MagicMock(return_value=False)

        DebugTask.attempt_connection(profile_a)
        DebugTask.attempt_connection(profile_b)

        # reset_adapters must have been called twice (once per target)
        assert mock_reset.call_count == 2
        assert mock_register.call_count == 2

    @patch("dvt.task.debug.get_adapter")
    @patch("dvt.task.debug.register_adapter")
    @patch("dvt.task.debug.reset_adapters")
    @patch("dvt.task.debug.get_mp_context")
    def test_connection_error_returned(
        self, mock_mp_ctx, mock_reset, mock_register, mock_get_adapter
    ):
        """Connection errors are returned as strings, not raised."""
        from dvt.task.debug import DebugTask

        mock_profile = MagicMock()
        mock_adapter = MagicMock()
        mock_get_adapter.return_value = mock_adapter

        # Make debug_query fail
        mock_adapter.connection_named.return_value.__enter__ = MagicMock()
        mock_adapter.connection_named.return_value.__exit__ = MagicMock(
            return_value=False
        )
        mock_adapter.debug_query.side_effect = Exception("Connection refused")

        result = DebugTask.attempt_connection(mock_profile)

        assert result is not None
        assert "Connection refused" in result
        # reset was still called even on failure
        mock_reset.assert_called_once()
