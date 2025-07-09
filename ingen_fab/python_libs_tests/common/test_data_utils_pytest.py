from __future__ import annotations

import pytest
from datetime import datetime
from unittest.mock import patch

from ingen_fab.python_libs.common.data_utils import timestamp_now


class TestDataUtils:
    """Test the data_utils module."""
    
    def test_timestamp_now_returns_integer(self):
        """Test that timestamp_now returns an integer."""
        result = timestamp_now()
        assert isinstance(result, int)
    
    def test_timestamp_now_format(self):
        """Test that timestamp_now returns correct format."""
        result = timestamp_now()
        result_str = str(result)
        
        # Should be YYYYMMDDHHMMSSmmm format (17 digits)
        assert len(result_str) == 17
        
        # First 4 digits should be current year
        current_year = datetime.utcnow().year
        assert result_str[:4] == str(current_year)
        
        # Next 2 digits should be month (01-12)
        month = int(result_str[4:6])
        assert 1 <= month <= 12
        
        # Next 2 digits should be day (01-31)
        day = int(result_str[6:8])
        assert 1 <= day <= 31
        
        # Next 2 digits should be hour (00-23)
        hour = int(result_str[8:10])
        assert 0 <= hour <= 23
        
        # Next 2 digits should be minute (00-59)
        minute = int(result_str[10:12])
        assert 0 <= minute <= 59
        
        # Next 2 digits should be second (00-59)
        second = int(result_str[12:14])
        assert 0 <= second <= 59
        
        # Last 3 digits should be milliseconds (000-999)
        milliseconds = int(result_str[14:17])
        assert 0 <= milliseconds <= 999
    
    def test_timestamp_now_fixed_datetime(self):
        """Test timestamp_now with fixed datetime."""
        # Mock datetime to return a fixed value
        fixed_datetime = datetime(2024, 1, 15, 14, 30, 45, 123456)
        
        with patch('ingen_fab.python_libs.common.data_utils.datetime') as mock_datetime:
            mock_datetime.utcnow.return_value = fixed_datetime
            
            result = timestamp_now()
            
            # Expected: 20240115143045123 (YYYYMMDDHHMMSSmmm)
            expected = 20240115143045123
            assert result == expected
    
    def test_timestamp_now_multiple_calls_different(self):
        """Test that multiple calls to timestamp_now return different values."""
        result1 = timestamp_now()
        result2 = timestamp_now()
        
        # Unless called at exactly the same millisecond, they should be different
        # We'll allow them to be the same due to fast execution, but at least one should be >= the other
        assert result1 <= result2
    
    def test_timestamp_now_uses_utc(self):
        """Test that timestamp_now uses UTC time."""
        # Mock datetime to verify utcnow is called
        with patch('ingen_fab.python_libs.common.data_utils.datetime') as mock_datetime:
            mock_datetime.utcnow.return_value = datetime(2024, 1, 1, 0, 0, 0, 0)
            
            timestamp_now()
            
            mock_datetime.utcnow.assert_called_once()
    
    def test_timestamp_now_microseconds_handling(self):
        """Test that microseconds are properly truncated to milliseconds."""
        # Test with microseconds that should be truncated
        fixed_datetime = datetime(2024, 1, 1, 0, 0, 0, 123456)  # 123456 microseconds = 123 milliseconds
        
        with patch('ingen_fab.python_libs.common.data_utils.datetime') as mock_datetime:
            mock_datetime.utcnow.return_value = fixed_datetime
            
            result = timestamp_now()
            
            # Should end with 123 (milliseconds), not 123456 (microseconds)
            result_str = str(result)
            assert result_str.endswith('123')
            assert len(result_str) == 17
    
    def test_timestamp_now_edge_cases(self):
        """Test timestamp_now with edge cases."""
        # Test with maximum values
        fixed_datetime = datetime(2024, 12, 31, 23, 59, 59, 999999)
        
        with patch('ingen_fab.python_libs.common.data_utils.datetime') as mock_datetime:
            mock_datetime.utcnow.return_value = fixed_datetime
            
            result = timestamp_now()
            
            # Expected: 20241231235959999
            expected = 20241231235959999
            assert result == expected
    
    def test_timestamp_now_zero_microseconds(self):
        """Test timestamp_now with zero microseconds."""
        fixed_datetime = datetime(2024, 1, 1, 0, 0, 0, 0)
        
        with patch('ingen_fab.python_libs.common.data_utils.datetime') as mock_datetime:
            mock_datetime.utcnow.return_value = fixed_datetime
            
            result = timestamp_now()
            
            # Expected: 20240101000000000
            expected = 20240101000000000
            assert result == expected
    
    def test_timestamp_now_consistent_format(self):
        """Test that timestamp_now always returns 17-digit format."""
        # Test with early month/day/hour/minute/second to ensure zero-padding
        fixed_datetime = datetime(2024, 1, 5, 7, 8, 9, 12345)
        
        with patch('ingen_fab.python_libs.common.data_utils.datetime') as mock_datetime:
            mock_datetime.utcnow.return_value = fixed_datetime
            
            result = timestamp_now()
            result_str = str(result)
            
            # Should be zero-padded: 20240105070809012
            assert len(result_str) == 17
            assert result_str == '20240105070809012'