from __future__ import annotations

import asyncio
from unittest.mock import patch

import pytest

from ingen_fab.python_libs.common.workflow_utils import random_delay


class TestWorkflowUtils:
    """Test the workflow_utils module."""

    @pytest.mark.asyncio
    async def test_random_delay_completes(self):
        """Test that random_delay completes without error."""
        # Should not raise any exceptions
        await random_delay()

    @pytest.mark.asyncio
    async def test_random_delay_is_async(self):
        """Test that random_delay is an async function."""
        assert asyncio.iscoroutinefunction(random_delay)

    @pytest.mark.asyncio
    async def test_random_delay_calls_asyncio_sleep(self):
        """Test that random_delay calls asyncio.sleep."""
        with patch("asyncio.sleep") as mock_sleep:
            await random_delay()
            mock_sleep.assert_called_once()

    @pytest.mark.asyncio
    async def test_random_delay_sleep_duration_range(self):
        """Test that random_delay sleeps for correct duration range."""
        with patch("asyncio.sleep") as mock_sleep:
            await random_delay()

            # Get the delay value passed to sleep
            sleep_duration = mock_sleep.call_args[0][0]

            # Should be between 0.1 and 1.5 seconds
            assert 0.1 <= sleep_duration <= 1.5

    @pytest.mark.asyncio
    async def test_random_delay_uses_numpy_random(self):
        """Test that random_delay uses numpy random."""
        with patch("ingen_fab.python_libs.common.workflow_utils.np.random.random") as mock_random:
            mock_random.return_value = 0.5  # Fixed random value

            with patch("asyncio.sleep") as mock_sleep:
                await random_delay()

                mock_random.assert_called_once()
                # Expected delay: 0.1 + 0.5 * 1.4 = 0.1 + 0.7 = 0.8
                expected_delay = 0.1 + 0.5 * 1.4
                mock_sleep.assert_called_once_with(expected_delay)

    @pytest.mark.asyncio
    async def test_random_delay_minimum_value(self):
        """Test random_delay with minimum random value."""
        with patch("ingen_fab.python_libs.common.workflow_utils.np.random.random") as mock_random:
            mock_random.return_value = 0.0  # Minimum random value

            with patch("asyncio.sleep") as mock_sleep:
                await random_delay()

                # Expected delay: 0.1 + 0.0 * 1.4 = 0.1
                expected_delay = 0.1
                mock_sleep.assert_called_once_with(expected_delay)

    @pytest.mark.asyncio
    async def test_random_delay_maximum_value(self):
        """Test random_delay with maximum random value."""
        with patch("ingen_fab.python_libs.common.workflow_utils.np.random.random") as mock_random:
            mock_random.return_value = 1.0  # Maximum random value

            with patch("asyncio.sleep") as mock_sleep:
                await random_delay()

                # Expected delay: 0.1 + 1.0 * 1.4 = 0.1 + 1.4 = 1.5
                expected_delay = 0.1 + 1.0 * 1.4
                mock_sleep.assert_called_once_with(expected_delay)

    @pytest.mark.asyncio
    async def test_random_delay_multiple_calls_different_values(self):
        """Test that multiple calls to random_delay produce different delay values."""
        sleep_durations = []

        with patch("asyncio.sleep") as mock_sleep:
            # Call random_delay multiple times
            for _ in range(10):
                await random_delay()
                sleep_durations.append(mock_sleep.call_args[0][0])
                mock_sleep.reset_mock()

        # Check that we got different values (with high probability)
        unique_values = set(sleep_durations)
        # Allow for some duplicates due to randomness, but expect mostly unique values
        assert len(unique_values) > 1, "Expected some variation in delay values"

    @pytest.mark.asyncio
    async def test_random_delay_docstring(self):
        """Test that random_delay has appropriate docstring."""
        assert random_delay.__doc__ is not None
        docstring = random_delay.__doc__
        assert "random delay" in docstring.lower()
        assert "contention" in docstring.lower()
        assert "delta table" in docstring.lower()

    @pytest.mark.asyncio
    async def test_random_delay_return_value(self):
        """Test that random_delay returns None."""
        result = await random_delay()
        assert result is None

    @pytest.mark.asyncio
    async def test_random_delay_calculation_formula(self):
        """Test the exact delay calculation formula."""
        test_random_value = 0.75

        with patch("ingen_fab.python_libs.common.workflow_utils.np.random.random") as mock_random:
            mock_random.return_value = test_random_value

            with patch("asyncio.sleep") as mock_sleep:
                await random_delay()

                # Formula: 0.1 + random_value * 1.4
                expected_delay = 0.1 + test_random_value * 1.4
                mock_sleep.assert_called_once_with(expected_delay)

                # Verify the calculation
                assert expected_delay == 0.1 + 0.75 * 1.4
                assert expected_delay == 0.1 + 1.05
                assert expected_delay == 1.15
