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


class TestIlmWriteIndexDetection(unittest.TestCase):
    """Unit tests for ILM write index detection and management logic"""

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

    def test_should_manage_index_log_patterns(self):
        """Test that indices matching log- and alert- patterns are managed"""
        # Mock _is_write_index to return False (not a write index)
        self.ilm._is_write_index = Mock(return_value=False)

        test_cases = [
            ("log-infoblox-dns-000001", True),
            ("log-suricata-alert-000001", True),
            ("alert-security-000001", True),
            ("log-apache-access-000005", True),
            ("alert-intrusion-detection-000003", True),
        ]

        for index_name, expected in test_cases:
            with self.subTest(index_name=index_name):
                result = self.ilm._should_manage_index(index_name)
                self.assertEqual(result, expected, f"Index {index_name} should be managed: {expected}")

    def test_should_not_manage_non_matching_patterns(self):
        """Test that indices not matching patterns are not managed"""
        # Mock _is_write_index to return False
        self.ilm._is_write_index = Mock(return_value=False)

        test_cases = [
            ("system-logs-000001", False),
            ("kibana-000001", False),
            (".opensearch-dashboards", False),
            ("metrics-cpu-000001", False),
            ("traces-application-000001", False),
        ]

        for index_name, expected in test_cases:
            with self.subTest(index_name=index_name):
                result = self.ilm._should_manage_index(index_name)
                self.assertEqual(result, expected, f"Index {index_name} should not be managed")

    def test_should_not_manage_write_indices(self):
        """Test that write indices are not managed even if they match patterns"""
        test_cases = [
            # (index_name, is_write_index, expected_managed)
            ("log-infoblox-dns-000006", True, False),   # Write index - should not manage
            ("log-infoblox-dns-000005", False, True),   # Non-write index - should manage
            ("alert-security-000010", True, False),     # Write index - should not manage
            ("alert-security-000009", False, True),     # Non-write index - should manage
        ]

        for index_name, is_write, expected_managed in test_cases:
            with self.subTest(index_name=index_name, is_write=is_write):
                # Mock _is_write_index to return specific value
                self.ilm._is_write_index = Mock(return_value=is_write)

                result = self.ilm._should_manage_index(index_name)
                self.assertEqual(result, expected_managed,
                    f"Index {index_name} (write={is_write}) should be managed: {expected_managed}")

    def test_is_write_index_detection(self):
        """Test robust write index detection using alias API"""
        # Mock alias data from your example
        alias_data = {
            "log-infoblox-dns-000001": {
                "aliases": {
                    "log-infoblox-dns-write": {
                        "is_write_index": False
                    }
                }
            },
            "log-infoblox-dns-000006": {
                "aliases": {
                    "log-infoblox-dns-write": {
                        "is_write_index": True
                    }
                }
            }
        }

        test_cases = [
            ("log-infoblox-dns-000001", False),  # Not a write index
            ("log-infoblox-dns-000006", True),   # Is a write index
        ]

        for index_name, expected_is_write in test_cases:
            with self.subTest(index_name=index_name):
                # Mock the API response
                mock_response = Mock()
                mock_response.status_code = 200
                mock_response.json.return_value = {index_name: alias_data[index_name]}
                self.mock_requests.get.return_value = mock_response

                result = self.ilm._is_write_index(index_name)
                self.assertEqual(result, expected_is_write,
                    f"Index {index_name} write status should be {expected_is_write}")

                # Verify API call
                self.mock_requests.get.assert_called_with(f"https://test-opensearch:9200/{index_name}/_alias")

    def test_is_write_index_no_aliases(self):
        """Test write index detection for index with no aliases"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "log-standalone-000001": {
                "aliases": {}
            }
        }
        self.mock_requests.get.return_value = mock_response

        result = self.ilm._is_write_index("log-standalone-000001")
        self.assertFalse(result, "Index with no aliases should not be considered a write index")

    def test_is_write_index_api_error(self):
        """Test write index detection when API call fails"""
        mock_response = Mock()
        mock_response.status_code = 404
        self.mock_requests.get.return_value = mock_response

        result = self.ilm._is_write_index("log-nonexistent-000001")
        self.assertFalse(result, "Failed API call should return False for write index check")

    def test_is_ready_for_snapshot_write_index(self):
        """Test that write indices are never ready for snapshot regardless of age"""
        # Mock an old index that is a write index
        self.ilm._is_write_index = Mock(return_value=True)
        self.ilm._is_searchable_snapshot = Mock(return_value=False)
        self.ilm._get_index_age_days = Mock(return_value=15)  # Older than hot_storage_days (7)

        result = self.ilm._is_ready_for_snapshot("log-infoblox-dns-000006")
        self.assertFalse(result, "Write indices should never be ready for snapshot")

    def test_is_ready_for_snapshot_old_non_write(self):
        """Test that old non-write indices are ready for snapshot"""
        # Mock an old index that is not a write index
        self.ilm._is_write_index = Mock(return_value=False)
        self.ilm._is_searchable_snapshot = Mock(return_value=False)
        self.ilm._get_index_age_days = Mock(return_value=15)  # Older than hot_storage_days (7)

        result = self.ilm._is_ready_for_snapshot("log-infoblox-dns-000001")
        self.assertTrue(result, "Old non-write indices should be ready for snapshot")

    def test_is_ready_for_snapshot_young_non_write(self):
        """Test that young non-write indices are not ready for snapshot"""
        # Mock a young index that is not a write index
        self.ilm._is_write_index = Mock(return_value=False)
        self.ilm._is_searchable_snapshot = Mock(return_value=False)
        self.ilm._get_index_age_days = Mock(return_value=3)   # Younger than hot_storage_days (7)

        result = self.ilm._is_ready_for_snapshot("log-infoblox-dns-000005")
        self.assertFalse(result, "Young indices should not be ready for snapshot")

    def test_is_ready_for_snapshot_already_searchable(self):
        """Test that searchable snapshots are not ready for snapshot again"""
        # Mock an index that is already a searchable snapshot
        self.ilm._is_write_index = Mock(return_value=False)
        self.ilm._is_searchable_snapshot = Mock(return_value=True)
        self.ilm._get_index_age_days = Mock(return_value=45)

        result = self.ilm._is_ready_for_snapshot("log-infoblox-dns-000001-snapshot")
        self.assertFalse(result, "Searchable snapshots should not be processed again")

    def test_specific_log_infoblox_dns_scenarios(self):
        """Test specific scenarios for log-infoblox-dns indices with dynamic timestamps"""
        import time

        current_time = time.time()

        # Test cases based on your requirements
        test_cases = [
            # (index_name, days_old, is_write_index, expected_ready_for_snapshot, description)
            ("log-infoblox-dns-000001", 13, False, True, "13-day-old index should be snapshot-ready (> 7 days)"),
            ("log-infoblox-dns-000005", 5, False, False, "5-day-old index should not be snapshot-ready (< 7 days)"),
            ("log-infoblox-dns-000006", 10, True, False, "Write index should never be snapshot-ready regardless of age"),
        ]

        for index_name, days_old, is_write, expected_ready, description in test_cases:
            with self.subTest(index_name=index_name):
                # Create dynamic timestamp for specified days ago
                creation_timestamp_ms = int((current_time - (days_old * 24 * 60 * 60)) * 1000)

                # Mock the settings response for age calculation
                mock_settings_response = Mock()
                mock_settings_response.status_code = 200
                mock_settings_response.json.return_value = {
                    index_name: {
                        "settings": {
                            "index": {
                                "creation_date": str(creation_timestamp_ms)
                            }
                        }
                    }
                }

                # Mock alias response for write index detection
                if is_write:
                    alias_data = {
                        index_name: {
                            "aliases": {
                                "log-infoblox-dns-write": {
                                    "is_write_index": True
                                }
                            }
                        }
                    }
                else:
                    alias_data = {
                        index_name: {
                            "aliases": {
                                "log-infoblox-dns-write": {
                                    "is_write_index": False
                                }
                            }
                        }
                    }

                mock_alias_response = Mock()
                mock_alias_response.status_code = 200
                mock_alias_response.json.return_value = alias_data

                # Configure mock to return appropriate response based on URL
                def mock_get_response(url):
                    if "_settings" in url:
                        return mock_settings_response
                    elif "_alias" in url:
                        return mock_alias_response
                    else:
                        return Mock(status_code=404)

                self.mock_requests.get.side_effect = mock_get_response

                # Test the full pipeline: should_manage -> is_ready_for_snapshot
                should_manage = self.ilm._should_manage_index(index_name)

                if is_write:
                    # Write indices should not be managed (excluded by _should_manage_index)
                    self.assertFalse(should_manage, f"Write index {index_name} should not be managed")
                else:
                    # Non-write indices should be managed (matches log- pattern)
                    self.assertTrue(should_manage, f"Non-write index {index_name} should be managed (matches log- pattern)")

                is_ready = self.ilm._is_ready_for_snapshot(index_name)
                self.assertEqual(is_ready, expected_ready, description)

                # Also verify age calculation is working correctly
                age_days = self.ilm._get_index_age_days(index_name)
                self.assertAlmostEqual(age_days, days_old, delta=0.1,
                    msg=f"Age calculation for {index_name} should be approximately {days_old} days")

    def test_transition_logic_with_specific_indices(self):
        """Test the complete transition logic with your specific indices"""
        import time

        current_time = time.time()

        # Mock index list with your specific indices
        mock_indices = [
            {"index": "log-infoblox-dns-000001"},  # 13 days old, should transition
            {"index": "log-infoblox-dns-000005"},  # 5 days old, should not transition
            {"index": "log-infoblox-dns-000006"},  # 10 days old but write index, should not transition
        ]

        self.ilm.get_indices = Mock(return_value=mock_indices)

        # Create dynamic timestamps
        timestamps = {
            "log-infoblox-dns-000001": int((current_time - (13 * 24 * 60 * 60)) * 1000),
            "log-infoblox-dns-000005": int((current_time - (5 * 24 * 60 * 60)) * 1000),
            "log-infoblox-dns-000006": int((current_time - (10 * 24 * 60 * 60)) * 1000),
        }

        # Mock responses for each index
        def mock_get_response(url):
            if "_settings" in url:
                for index_name in timestamps:
                    if index_name in url:
                        mock_response = Mock()
                        mock_response.status_code = 200
                        mock_response.json.return_value = {
                            index_name: {
                                "settings": {
                                    "index": {
                                        "creation_date": str(timestamps[index_name])
                                    }
                                }
                            }
                        }
                        return mock_response
            elif "_alias" in url:
                # Only 000006 is write index
                for index_name in ["log-infoblox-dns-000001", "log-infoblox-dns-000005", "log-infoblox-dns-000006"]:
                    if index_name in url:
                        is_write = (index_name == "log-infoblox-dns-000006")
                        mock_response = Mock()
                        mock_response.status_code = 200
                        mock_response.json.return_value = {
                            index_name: {
                                "aliases": {
                                    "log-infoblox-dns-write": {
                                        "is_write_index": is_write
                                    }
                                }
                            }
                        }
                        return mock_response

            return Mock(status_code=404)

        self.mock_requests.get.side_effect = mock_get_response
        self.ilm._snapshot_and_replace_index = Mock()
        self.ilm._is_searchable_snapshot = Mock(return_value=False)

        # Run the transition logic
        self.ilm.transition_old_indices_to_snapshots()

        # Verify that only log-infoblox-dns-000001 was processed for snapshot
        self.ilm._snapshot_and_replace_index.assert_called_once_with("log-infoblox-dns-000001")


if __name__ == '__main__':
    unittest.main()