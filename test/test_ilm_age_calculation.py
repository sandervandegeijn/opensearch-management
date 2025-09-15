#!/usr/bin/env python3

import unittest
import time
from unittest.mock import Mock, MagicMock, patch
import sys
import os

# Add parent directory to path to import modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ilm import Ilm
from settings import Settings


class TestIlmAgeCalculation(unittest.TestCase):
    """Unit tests for ILM age calculation logic with edge cases"""

    def setUp(self):
        """Set up test fixtures before each test method."""
        self.mock_settings = Mock(spec=Settings)
        self.mock_settings.url = "https://test-opensearch:9200"
        self.mock_settings.number_of_days_on_hot_storage = 7
        self.mock_settings.number_of_days_total_retention = 90
        self.mock_settings.rollover_size_gb = 50
        self.mock_settings.rollover_age_days = 30
        self.mock_settings.managed_index_patterns = ("log-", "alert-")

        self.mock_requests = Mock()
        self.mock_settings.get_requests_object.return_value = self.mock_requests

        self.ilm = Ilm(self.mock_settings)

    def test_age_calculation_normal_case(self):
        """Test normal age calculation for indices"""
        # Create a timestamp for 45 days ago
        days_ago = 45
        creation_timestamp_ms = int((time.time() - (days_ago * 24 * 60 * 60)) * 1000)

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "log-normal-000001": {
                "settings": {
                    "index": {
                        "creation_date": str(creation_timestamp_ms)
                    }
                }
            }
        }
        self.mock_requests.get.return_value = mock_response

        age_days = self.ilm._get_index_age_days("log-normal-000001")

        # Allow for small timing differences in test execution
        self.assertAlmostEqual(age_days, days_ago, delta=0.1,
                              msg=f"Age should be approximately {days_ago} days")

    def test_age_calculation_future_timestamp_case(self):
        """Test age calculation for index with actual future timestamp"""
        # Create a timestamp 10 days in the future
        future_timestamp_ms = int((time.time() + (10 * 24 * 60 * 60)) * 1000)

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "log-future-000001": {
                "settings": {
                    "index": {
                        "creation_date": str(future_timestamp_ms)
                    }
                }
            }
        }
        self.mock_requests.get.return_value = mock_response

        with patch('ilm.logger') as mock_logger:
            age_days = self.ilm._get_index_age_days("log-future-000001")

        # Should return 0 for future timestamps
        self.assertEqual(age_days, 0, "Future timestamps should return 0 age")

        # Should log a warning
        mock_logger.warning.assert_called_once()
        warning_call = mock_logger.warning.call_args[0][0]
        self.assertIn("future creation timestamp", warning_call)

    def test_age_calculation_your_actual_timestamps(self):
        """Test age calculation for your actual problematic timestamps (now in the past)"""
        test_cases = [
            # (timestamp_ms, description, expected_should_snapshot)
            (1756834419619, "Sept 2, 2025 timestamp (13 days old)", True),   # Old enough for 7-day threshold
            (1757535300048, "Sept 10, 2025 timestamp (5 days old)", False),   # Too young for 7-day threshold
        ]

        for timestamp_ms, description, expected_should_snapshot in test_cases:
            with self.subTest(timestamp=timestamp_ms):
                mock_response = Mock()
                mock_response.status_code = 200
                mock_response.json.return_value = {
                    "test-index": {
                        "settings": {
                            "index": {
                                "creation_date": str(timestamp_ms)
                            }
                        }
                    }
                }
                self.mock_requests.get.return_value = mock_response

                age_days = self.ilm._get_index_age_days("test-index")

                # Should return actual age (not 0)
                self.assertGreater(age_days, 0, f"{description} should have positive age")
                # Age assertions based on actual timestamps
                if "13 days" in description:
                    self.assertGreater(age_days, 7, f"{description} should be greater than 7 days old")
                elif "5 days" in description:
                    self.assertLess(age_days, 7, f"{description} should be less than 7 days old")

                # Test readiness for snapshot
                self.ilm._is_write_index = Mock(return_value=False)
                self.ilm._is_searchable_snapshot = Mock(return_value=False)

                is_ready = self.ilm._is_ready_for_snapshot("test-index")
                self.assertEqual(is_ready, expected_should_snapshot,
                    f"{description} snapshot readiness should be {expected_should_snapshot}")

    def test_age_calculation_api_error(self):
        """Test age calculation when settings API fails"""
        mock_response = Mock()
        mock_response.status_code = 404
        mock_response.text = "Index not found"
        self.mock_requests.get.return_value = mock_response

        with patch('ilm.logger') as mock_logger:
            age_days = self.ilm._get_index_age_days("log-nonexistent-000001")

        # Should return 0 for failed API calls
        self.assertEqual(age_days, 0, "Failed API calls should return 0 age")

        # Should log an error
        mock_logger.error.assert_called_once()

    def test_age_calculation_malformed_timestamp(self):
        """Test age calculation with malformed creation_date"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "log-malformed-000001": {
                "settings": {
                    "index": {
                        "creation_date": "not-a-number"
                    }
                }
            }
        }
        self.mock_requests.get.return_value = mock_response

        with patch('ilm.logger') as mock_logger:
            age_days = self.ilm._get_index_age_days("log-malformed-000001")

        # Should return 0 for malformed timestamps
        self.assertEqual(age_days, 0, "Malformed timestamps should return 0 age")

        # Should log a debug message
        mock_logger.debug.assert_called()

    def test_transition_logic_with_future_timestamps(self):
        """Test that indices with future timestamps don't get transitioned"""
        # Mock index list with future timestamp index
        mock_indices = [
            {"index": "log-infoblox-dns-000001"},
            {"index": "log-infoblox-dns-000005"}
        ]

        self.ilm.get_indices = Mock(return_value=mock_indices)

        # Mock _should_manage_index to return True (they match patterns)
        self.ilm._should_manage_index = Mock(return_value=True)

        # Mock _is_ready_for_snapshot to simulate the age check
        def mock_is_ready(index_name):
            # Simulate future timestamp returning False due to 0 age
            if index_name in ["log-infoblox-dns-000001", "log-infoblox-dns-000005"]:
                return False  # Too young due to future timestamp -> 0 age
            return True

        self.ilm._is_ready_for_snapshot = Mock(side_effect=mock_is_ready)
        self.ilm._snapshot_and_replace_index = Mock()

        # Run the transition logic
        self.ilm.transition_old_indices_to_snapshots()

        # Verify that no indices were processed for snapshot due to future timestamps
        self.ilm._snapshot_and_replace_index.assert_not_called()

    def test_ready_for_snapshot_with_correct_ages(self):
        """Test snapshot readiness with time-resilient test logic"""
        current_time = time.time()

        test_cases = [
            # (days_ago, expected_ready, description)
            (15, True, "15-day-old index should be ready (> 7 days)"),
            (10, True, "10-day-old index should be ready (> 7 days)"),
            (7, True, "7-day-old index should be ready (= 7 days)"),
            (5, False, "5-day-old index should not be ready (< 7 days)"),
            (1, False, "1-day-old index should not be ready (< 7 days)"),
        ]

        for days_ago, expected_ready, description in test_cases:
            with self.subTest(days_ago=days_ago):
                # Create timestamp for specified days ago
                creation_timestamp_ms = int((current_time - (days_ago * 24 * 60 * 60)) * 1000)

                # Mock the settings response
                mock_response = Mock()
                mock_response.status_code = 200
                mock_response.json.return_value = {
                    f"log-test-{days_ago:03d}": {
                        "settings": {
                            "index": {
                                "creation_date": str(creation_timestamp_ms)
                            }
                        }
                    }
                }
                self.mock_requests.get.return_value = mock_response

                # Mock other conditions to isolate age testing
                self.ilm._is_write_index = Mock(return_value=False)
                self.ilm._is_searchable_snapshot = Mock(return_value=False)

                result = self.ilm._is_ready_for_snapshot(f"log-test-{days_ago:03d}")
                self.assertEqual(result, expected_ready, description)

    @patch('time.time')
    def test_age_calculation_deterministic(self, mock_time):
        """Test age calculation with fixed time for deterministic results"""
        # Fix current time to a known value (Sept 15, 2025 for example)
        fixed_current_time = 1757949600  # Sept 15, 2025 12:00:00 GMT
        mock_time.return_value = fixed_current_time

        # Test cases with known timestamps
        test_cases = [
            # (creation_timestamp_ms, expected_age_days, description)
            (1756834419619, 12.9, "Sept 2, 2025 timestamp should be ~13 days old"),  # Your actual timestamp
            (1757535300048, 4.8, "Sept 10, 2025 timestamp should be ~5 days old"),   # Your second timestamp
            (1755086400000, 33.1, "Aug 13, 2025 timestamp should be ~33 days old"),  # 33 days earlier
            (1752577200000, 62.2, "July 15, 2025 timestamp should be ~62 days old"), # 62 days earlier
            # Test actual future timestamp
            (int(fixed_current_time * 1000) + 864000000, 0, "Future timestamp should return 0"),  # 10 days future
        ]

        for creation_ms, expected_age, description in test_cases:
            with self.subTest(creation_ms=creation_ms):
                mock_response = Mock()
                mock_response.status_code = 200
                mock_response.json.return_value = {
                    "test-index": {
                        "settings": {
                            "index": {
                                "creation_date": str(creation_ms)
                            }
                        }
                    }
                }
                self.mock_requests.get.return_value = mock_response

                age_days = self.ilm._get_index_age_days("test-index")

                if expected_age == 0:
                    self.assertEqual(age_days, 0, description)
                else:
                    self.assertAlmostEqual(age_days, expected_age, delta=0.2, msg=description)


if __name__ == '__main__':
    unittest.main()