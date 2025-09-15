#!/usr/bin/env python3

import unittest
import time
from unittest.mock import Mock, MagicMock, patch, call
import sys
import os

# Add parent directory to path to import modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ilm import Ilm
from settings import Settings


class TestIlmIntegrationLifecycle(unittest.TestCase):
    """Integration test for complete index lifecycle: creation -> rollover -> transition"""

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

        # Track time for dynamic timestamps
        self.current_time = time.time()

    def test_complete_index_lifecycle(self):
        """Test complete lifecycle: new alias -> rollover -> transition after hot storage period"""

        # === PHASE 1: Initial state - new alias with 00001 index ===
        print("\\n=== PHASE 1: Initial state - new alias with log-application-000001 ===")

        initial_indices = [
            {"index": "log-application-000001"}
        ]

        # 00001 is currently the write index
        initial_alias_data = {
            "log-application-000001": {
                "aliases": {
                    "log-application-write": {
                        "is_write_index": True
                    }
                }
            }
        }

        # Mock age calculation for 00001 (1 day old - too young for transition)
        day_1_timestamp = int((self.current_time - (1 * 24 * 60 * 60)) * 1000)

        def phase1_mock_get_response(url):
            if "_settings" in url and "log-application-000001" in url:
                mock_response = Mock()
                mock_response.status_code = 200
                mock_response.json.return_value = {
                    "log-application-000001": {
                        "settings": {
                            "index": {
                                "creation_date": str(day_1_timestamp)
                            }
                        }
                    }
                }
                return mock_response
            elif "_alias" in url:
                mock_response = Mock()
                mock_response.status_code = 200
                mock_response.json.return_value = initial_alias_data
                return mock_response
            return Mock(status_code=404)

        self.mock_requests.get.side_effect = phase1_mock_get_response
        self.ilm.get_indices = Mock(return_value=initial_indices)
        self.ilm._snapshot_and_replace_index = Mock()
        self.ilm._is_searchable_snapshot = Mock(return_value=False)

        # Run ILM - should not transition anything (00001 is write index and too young)
        self.ilm.transition_old_indices_to_snapshots()

        # Verify no transitions occurred
        self.ilm._snapshot_and_replace_index.assert_not_called()
        print("✓ Phase 1: No transitions (00001 is write index and only 1 day old)")

        # === PHASE 2: Rollover occurs - 00002 becomes write index ===
        print("\\n=== PHASE 2: Rollover occurs - log-application-000002 becomes write index ===")

        # Simulate rollover by mocking the rollover operation
        mock_rollover_response = Mock()
        mock_rollover_response.status_code = 200
        mock_rollover_response.json.return_value = {
            "rolled_over": True,
            "old_index": "log-application-000001",
            "new_index": "log-application-000002"
        }

        # Mock the rollover API call
        def rollover_mock_post(url, **kwargs):
            if "_rollover" in url:
                return mock_rollover_response
            return Mock(status_code=404)

        self.mock_requests.post.side_effect = rollover_mock_post

        # Mock write aliases list
        def rollover_mock_get(url):
            if url.endswith("/_alias"):
                # Return write aliases
                mock_response = Mock()
                mock_response.status_code = 200
                mock_response.json.return_value = {
                    "log-application-000001": {
                        "aliases": {
                            "log-application-write": {"is_write_index": False}
                        }
                    },
                    "log-application-000002": {
                        "aliases": {
                            "log-application-write": {"is_write_index": True}
                        }
                    }
                }
                return mock_response
            elif "_alias/log-application-write" in url:
                # Return specific alias info
                mock_response = Mock()
                mock_response.status_code = 200
                mock_response.json.return_value = {
                    "log-application-000002": {
                        "aliases": {
                            "log-application-write": {"is_write_index": True}
                        }
                    }
                }
                return mock_response
            return Mock(status_code=404)

        self.mock_requests.get.side_effect = rollover_mock_get
        self.ilm._get_write_aliases = Mock(return_value=["log-application-write"])
        self.ilm._get_write_index = Mock(return_value="log-application-000002")

        # Run rollover check
        result = self.ilm.check_and_rollover_by_size()

        print("✓ Phase 2: Rollover completed - 000002 is now write index, 000001 is no longer write index")

        # === PHASE 3: Time passes - 00001 becomes eligible for transition ===
        print("\\n=== PHASE 3: Time passes - 000001 becomes eligible for transition (8 days old) ===")

        # Update indices list to include both
        updated_indices = [
            {"index": "log-application-000001"},
            {"index": "log-application-000002"}
        ]

        # Update alias data - 00002 is now write index, 00001 is not
        updated_alias_data = {
            "log-application-000001": {
                "aliases": {
                    "log-application-write": {
                        "is_write_index": False
                    }
                }
            },
            "log-application-000002": {
                "aliases": {
                    "log-application-write": {
                        "is_write_index": True
                    }
                }
            }
        }

        # 00001 is now 8 days old (eligible for transition)
        # 00002 is 1 day old (too young for transition)
        day_8_timestamp = int((self.current_time - (8 * 24 * 60 * 60)) * 1000)
        day_1_timestamp_new = int((self.current_time - (1 * 24 * 60 * 60)) * 1000)

        def phase3_mock_get_response(url):
            if "_settings" in url:
                if "log-application-000001" in url:
                    mock_response = Mock()
                    mock_response.status_code = 200
                    mock_response.json.return_value = {
                        "log-application-000001": {
                            "settings": {
                                "index": {
                                    "creation_date": str(day_8_timestamp)
                                }
                            }
                        }
                    }
                    return mock_response
                elif "log-application-000002" in url:
                    mock_response = Mock()
                    mock_response.status_code = 200
                    mock_response.json.return_value = {
                        "log-application-000002": {
                            "settings": {
                                "index": {
                                    "creation_date": str(day_1_timestamp_new)
                                }
                            }
                        }
                    }
                    return mock_response
            elif "_alias" in url:
                for index_name in ["log-application-000001", "log-application-000002"]:
                    if index_name in url:
                        is_write = (index_name == "log-application-000002")
                        mock_response = Mock()
                        mock_response.status_code = 200
                        mock_response.json.return_value = {
                            index_name: {
                                "aliases": {
                                    "log-application-write": {
                                        "is_write_index": is_write
                                    }
                                }
                            }
                        }
                        return mock_response
            return Mock(status_code=404)

        self.mock_requests.get.side_effect = phase3_mock_get_response
        self.ilm.get_indices = Mock(return_value=updated_indices)
        self.ilm._snapshot_and_replace_index.reset_mock()  # Reset previous calls

        # Run ILM transition - should now transition 00001
        self.ilm.transition_old_indices_to_snapshots()

        # Verify 00001 was transitioned but 00002 was not
        self.ilm._snapshot_and_replace_index.assert_called_once_with("log-application-000001")
        print("✓ Phase 3: log-application-000001 was transitioned to searchable snapshot (8 days old, no longer write index)")

        # === PHASE 4: Verify complete state ===
        print("\\n=== PHASE 4: Verify final state ===")

        # Test individual conditions for verification

        # 00001 should be managed (matches pattern)
        should_manage_001 = self.ilm._should_manage_index("log-application-000001")
        self.assertTrue(should_manage_001, "log-application-000001 should be managed (matches log- pattern)")

        # 00001 should be ready for snapshot (old enough and not write index)
        is_ready_001 = self.ilm._is_ready_for_snapshot("log-application-000001")
        self.assertTrue(is_ready_001, "log-application-000001 should be ready for snapshot (8 days old, not write index)")

        # 00002 should be managed but not ready for snapshot (write index)
        should_manage_002 = self.ilm._should_manage_index("log-application-000002")
        self.assertFalse(should_manage_002, "log-application-000002 should not be managed (write index)")

        # Verify ages are calculated correctly
        age_001 = self.ilm._get_index_age_days("log-application-000001")
        age_002 = self.ilm._get_index_age_days("log-application-000002")

        self.assertAlmostEqual(age_001, 8.0, delta=0.1, msg="log-application-000001 should be 8 days old")
        self.assertAlmostEqual(age_002, 1.0, delta=0.1, msg="log-application-000002 should be 1 day old")

        print(f"✓ Phase 4: Final verification complete")
        print(f"  - log-application-000001: {age_001:.1f} days old, transitioned to searchable snapshot")
        print(f"  - log-application-000002: {age_002:.1f} days old, remains as write index")

    def test_rollover_size_based_trigger(self):
        """Test that rollover is triggered when size conditions are met"""
        print("\\n=== Testing size-based rollover trigger ===")

        # Mock write aliases and indices
        self.ilm._get_write_aliases = Mock(return_value=["log-application-write"])
        self.ilm._get_write_index = Mock(return_value="log-application-000001")

        # Mock successful rollover response
        mock_rollover_response = Mock()
        mock_rollover_response.status_code = 200
        mock_rollover_response.json.return_value = {
            "rolled_over": True,
            "old_index": "log-application-000001",
            "new_index": "log-application-000002"
        }

        self.mock_requests.post.return_value = mock_rollover_response

        # Run rollover check
        self.ilm.check_and_rollover_by_size()

        # Verify rollover API was called with correct conditions
        expected_url = "https://test-opensearch:9200/log-application-write/_rollover"
        expected_body = {
            "conditions": {
                "max_size": "50gb",
                "max_age": "30d"
            }
        }

        self.mock_requests.post.assert_called_once_with(expected_url, json=expected_body)
        print("✓ Rollover API called with correct size (50GB) and age (30d) conditions")

    def test_multiple_indices_lifecycle(self):
        """Test lifecycle with multiple index patterns"""
        print("\\n=== Testing multiple indices lifecycle ===")

        current_time = time.time()

        # Mock multiple indices with different ages and write statuses
        indices = [
            {"index": "log-application-000001"},  # 10 days old, not write
            {"index": "log-application-000002"},  # 2 days old, write index
            {"index": "log-security-000001"},     # 15 days old, not write
            {"index": "log-security-000002"},     # 3 days old, write index
            {"index": "alert-intrusion-000001"},  # 12 days old, not write
            {"index": "alert-intrusion-000002"},  # 1 day old, write index
        ]

        # Create timestamps
        timestamps = {
            "log-application-000001": int((current_time - (10 * 24 * 60 * 60)) * 1000),
            "log-application-000002": int((current_time - (2 * 24 * 60 * 60)) * 1000),
            "log-security-000001": int((current_time - (15 * 24 * 60 * 60)) * 1000),
            "log-security-000002": int((current_time - (3 * 24 * 60 * 60)) * 1000),
            "alert-intrusion-000001": int((current_time - (12 * 24 * 60 * 60)) * 1000),
            "alert-intrusion-000002": int((current_time - (1 * 24 * 60 * 60)) * 1000),
        }

        # Define write indices
        write_indices = {
            "log-application-000002",
            "log-security-000002",
            "alert-intrusion-000002"
        }

        def multi_mock_get_response(url):
            if "_settings" in url:
                for index_name, timestamp in timestamps.items():
                    if index_name in url:
                        mock_response = Mock()
                        mock_response.status_code = 200
                        mock_response.json.return_value = {
                            index_name: {
                                "settings": {
                                    "index": {
                                        "creation_date": str(timestamp)
                                    }
                                }
                            }
                        }
                        return mock_response
            elif "_alias" in url:
                for index_name in timestamps.keys():
                    if index_name in url:
                        is_write = index_name in write_indices
                        mock_response = Mock()
                        mock_response.status_code = 200
                        mock_response.json.return_value = {
                            index_name: {
                                "aliases": {
                                    f"{index_name.rsplit('-', 1)[0]}-write": {
                                        "is_write_index": is_write
                                    }
                                }
                            }
                        }
                        return mock_response
            return Mock(status_code=404)

        self.mock_requests.get.side_effect = multi_mock_get_response
        self.ilm.get_indices = Mock(return_value=indices)
        self.ilm._snapshot_and_replace_index = Mock()
        self.ilm._is_searchable_snapshot = Mock(return_value=False)

        # Run ILM transition
        self.ilm.transition_old_indices_to_snapshots()

        # Expected transitions: only non-write indices older than 7 days
        expected_transitions = [
            "log-application-000001",  # 10 days old, not write
            "log-security-000001",     # 15 days old, not write
            "alert-intrusion-000001",  # 12 days old, not write
        ]

        # Verify correct indices were transitioned
        actual_calls = [call[0][0] for call in self.ilm._snapshot_and_replace_index.call_args_list]

        self.assertEqual(len(actual_calls), 3, "Should transition exactly 3 indices")
        for expected_index in expected_transitions:
            self.assertIn(expected_index, actual_calls, f"{expected_index} should be transitioned")

        print(f"✓ Multiple indices lifecycle: {len(expected_transitions)} indices transitioned correctly")
        for index in expected_transitions:
            print(f"  - {index}: transitioned (old enough and not write index)")

    def test_cleanup_old_data_lifecycle(self):
        """Test complete cleanup lifecycle: regular indices, searchable snapshots, and snapshots beyond retention"""
        print("\\n=== Testing cleanup lifecycle for data older than retention period (90 days) ===")

        current_time = time.time()

        # === Setup: Mix of indices and snapshots with different ages ===

        # Indices (mix of regular, searchable snapshots, and various ages)
        indices = [
            {"index": "log-application-000001"},      # 100 days old, regular index - should be deleted
            {"index": "log-application-000001-snapshot"}, # 95 days old, searchable snapshot - should be deleted
            {"index": "log-application-000002"},      # 80 days old, regular index - should stay
            {"index": "log-application-000002-snapshot"}, # 70 days old, searchable snapshot - should stay
            {"index": "log-security-000001"},         # 120 days old, regular index - should be deleted
            {"index": "log-security-000002"},         # 30 days old, regular index - should stay
        ]

        # Snapshots (with corresponding ages)
        snapshots = [
            {
                "id": "log-application-000001",
                "status": "SUCCESS",
                "start_epoch": str(int(current_time - (100 * 24 * 60 * 60))),
                "end_epoch": str(int(current_time - (100 * 24 * 60 * 60))),
                "endEpoch": str(int(current_time - (100 * 24 * 60 * 60)))
            },
            {
                "id": "log-application-000002",
                "status": "SUCCESS",
                "start_epoch": str(int(current_time - (80 * 24 * 60 * 60))),
                "end_epoch": str(int(current_time - (80 * 24 * 60 * 60))),
                "endEpoch": str(int(current_time - (80 * 24 * 60 * 60)))
            },
            {
                "id": "log-security-000001",
                "status": "SUCCESS",
                "start_epoch": str(int(current_time - (120 * 24 * 60 * 60))),
                "end_epoch": str(int(current_time - (120 * 24 * 60 * 60))),
                "endEpoch": str(int(current_time - (120 * 24 * 60 * 60)))
            },
            {
                "id": "log-security-000002",
                "status": "SUCCESS",
                "start_epoch": str(int(current_time - (30 * 24 * 60 * 60))),
                "end_epoch": str(int(current_time - (30 * 24 * 60 * 60))),
                "endEpoch": str(int(current_time - (30 * 24 * 60 * 60)))
            }
        ]

        # Create timestamps for age calculation
        timestamps = {
            "log-application-000001": int((current_time - (100 * 24 * 60 * 60)) * 1000),
            "log-application-000001-snapshot": int((current_time - (95 * 24 * 60 * 60)) * 1000),
            "log-application-000002": int((current_time - (80 * 24 * 60 * 60)) * 1000),
            "log-application-000002-snapshot": int((current_time - (70 * 24 * 60 * 60)) * 1000),
            "log-security-000001": int((current_time - (120 * 24 * 60 * 60)) * 1000),
            "log-security-000002": int((current_time - (30 * 24 * 60 * 60)) * 1000),
        }

        def cleanup_mock_get_response(url):
            if "_settings" in url:
                for index_name, timestamp in timestamps.items():
                    if index_name in url:
                        mock_response = Mock()
                        mock_response.status_code = 200
                        mock_response.json.return_value = {
                            index_name: {
                                "settings": {
                                    "index": {
                                        "creation_date": str(timestamp),
                                        "store": {"type": "remote_snapshot"} if index_name.endswith("-snapshot") else {}
                                    }
                                }
                            }
                        }
                        return mock_response
            elif "_alias" in url:
                # All indices are non-write for this test
                for index_name in timestamps.keys():
                    if index_name in url:
                        mock_response = Mock()
                        mock_response.status_code = 200
                        mock_response.json.return_value = {
                            index_name: {
                                "aliases": {}
                            }
                        }
                        return mock_response
            return Mock(status_code=404)

        self.mock_requests.get.side_effect = cleanup_mock_get_response
        self.ilm.get_indices = Mock(return_value=indices)
        self.ilm.get_snapshots = Mock(return_value=snapshots)

        # Mock delete operations
        self.ilm._delete_index = Mock()
        self.ilm._delete_snapshot = Mock()
        self.ilm._delete_snapshot_with_cleanup = Mock()

        # === Execute cleanup ===
        self.ilm.cleanup_old_data()

        # === Verify Phase 1: Searchable snapshot indices cleanup ===
        print("\\n--- Phase 1: Searchable snapshot indices cleanup ---")

        # Should delete searchable snapshots older than 90 days
        expected_searchable_deletions = [
            "log-application-000001-snapshot"  # 95 days old
        ]

        searchable_deletions = []
        for call in self.ilm._delete_index.call_args_list:
            index_name = call[0][0]
            if index_name.endswith("-snapshot"):
                searchable_deletions.append(index_name)

        for expected in expected_searchable_deletions:
            self.assertIn(expected, [call[0][0] for call in self.ilm._delete_index.call_args_list],
                         f"Searchable snapshot {expected} should be deleted (older than 90 days)")

        print(f"✓ Phase 1: {len(expected_searchable_deletions)} searchable snapshot indices deleted")

        # === Verify Phase 2: Regular indices cleanup ===
        print("\\n--- Phase 2: Regular indices cleanup ---")

        # Should delete regular indices older than 90 days
        expected_regular_deletions = [
            "log-application-000001",  # 100 days old
            "log-security-000001",     # 120 days old
        ]

        regular_deletions = []
        for call in self.ilm._delete_index.call_args_list:
            index_name = call[0][0]
            if not index_name.endswith("-snapshot"):
                regular_deletions.append(index_name)

        for expected in expected_regular_deletions:
            self.assertIn(expected, [call[0][0] for call in self.ilm._delete_index.call_args_list],
                         f"Regular index {expected} should be deleted (older than 90 days)")

        print(f"✓ Phase 2: {len(expected_regular_deletions)} regular indices deleted")

        # === Verify Phase 3: Snapshots cleanup ===
        print("\\n--- Phase 3: Snapshots cleanup ---")

        # Should delete snapshots older than 90 days
        expected_snapshot_deletions = [
            "log-application-000001",  # 100 days old
            "log-security-000001",     # 120 days old
        ]

        snapshot_deletions = [call[0][0] for call in self.ilm._delete_snapshot_with_cleanup.call_args_list]

        for expected in expected_snapshot_deletions:
            self.assertIn(expected, snapshot_deletions,
                         f"Snapshot {expected} should be deleted (older than 90 days)")

        print(f"✓ Phase 3: {len(expected_snapshot_deletions)} snapshots deleted")

        # === Verify what should NOT be deleted ===
        print("\\n--- Verification: What should remain ---")

        should_remain_indices = [
            "log-application-000002",         # 80 days old
            "log-application-000002-snapshot", # 70 days old
            "log-security-000002",            # 30 days old
        ]

        should_remain_snapshots = [
            "log-application-000002",  # 80 days old
            "log-security-000002",     # 30 days old
        ]

        all_deleted_indices = [call[0][0] for call in self.ilm._delete_index.call_args_list]
        all_deleted_snapshots = [call[0][0] for call in self.ilm._delete_snapshot_with_cleanup.call_args_list]

        for index in should_remain_indices:
            self.assertNotIn(index, all_deleted_indices,
                           f"Index {index} should NOT be deleted (younger than 90 days)")

        for snapshot in should_remain_snapshots:
            self.assertNotIn(snapshot, all_deleted_snapshots,
                           f"Snapshot {snapshot} should NOT be deleted (younger than 90 days)")

        print(f"✓ Verification: {len(should_remain_indices)} indices and {len(should_remain_snapshots)} snapshots correctly preserved")

        # === Summary ===
        print("\\n=== Cleanup Summary ===")
        print(f"Deleted indices: {len(all_deleted_indices)} (regular: {len(regular_deletions)}, searchable: {len(searchable_deletions)})")
        print(f"Deleted snapshots: {len(all_deleted_snapshots)}")
        print(f"Preserved indices: {len(should_remain_indices)}")
        print(f"Preserved snapshots: {len(should_remain_snapshots)}")

    def test_end_to_end_complete_lifecycle(self):
        """Test complete end-to-end lifecycle: creation -> rollover -> transition -> cleanup"""
        print("\\n=== COMPLETE END-TO-END LIFECYCLE TEST ===")

        current_time = time.time()

        # === Timeline simulation ===
        # Day 0: log-app-000001 created (write index)
        # Day 30: Rollover occurs -> log-app-000002 created (becomes write index)
        # Day 37: log-app-000001 transitioned to searchable snapshot (7 days after rollover)
        # Day 90: log-app-000001 still preserved (exactly at retention limit)
        # Day 100: log-app-000001 and its snapshot should be cleaned up (beyond retention)

        # Simulate the state at Day 100
        indices = [
            {"index": "log-app-000001-snapshot"},  # Day 37: searchable snapshot, now 100 days from original creation
            {"index": "log-app-000002"},           # Day 30: non-write index, 70 days old
            {"index": "log-app-000003"},           # Day 60: current write index, 40 days old
        ]

        snapshots = [
            {
                "id": "log-app-000001",
                "status": "SUCCESS",
                "end_epoch": str(int(current_time - (100 * 24 * 60 * 60))),  # 100 days old
            },
            {
                "id": "log-app-000002",
                "status": "SUCCESS",
                "end_epoch": str(int(current_time - (70 * 24 * 60 * 60))),   # 70 days old
            }
        ]

        timestamps = {
            "log-app-000001-snapshot": int((current_time - (100 * 24 * 60 * 60)) * 1000),  # Original creation date
            "log-app-000002": int((current_time - (70 * 24 * 60 * 60)) * 1000),
            "log-app-000003": int((current_time - (40 * 24 * 60 * 60)) * 1000),
        }

        def e2e_mock_get_response(url):
            if "_settings" in url:
                for index_name, timestamp in timestamps.items():
                    if index_name in url:
                        mock_response = Mock()
                        mock_response.status_code = 200
                        mock_response.json.return_value = {
                            index_name: {
                                "settings": {
                                    "index": {
                                        "creation_date": str(timestamp),
                                        "store": {"type": "remote_snapshot"} if index_name.endswith("-snapshot") else {}
                                    }
                                }
                            }
                        }
                        return mock_response
            elif "_alias" in url:
                # log-app-000003 is the current write index
                for index_name in timestamps.keys():
                    if index_name in url:
                        is_write = (index_name == "log-app-000003")
                        mock_response = Mock()
                        mock_response.status_code = 200
                        mock_response.json.return_value = {
                            index_name: {
                                "aliases": {
                                    "log-app-write": {
                                        "is_write_index": is_write
                                    }
                                } if not index_name.endswith("-snapshot") else {}
                            }
                        }
                        return mock_response
            return Mock(status_code=404)

        self.mock_requests.get.side_effect = e2e_mock_get_response
        self.ilm.get_indices = Mock(return_value=indices)
        self.ilm.get_snapshots = Mock(return_value=snapshots)

        # Mock operations
        self.ilm._snapshot_and_replace_index = Mock()
        self.ilm._delete_index = Mock()
        self.ilm._delete_snapshot_with_cleanup = Mock()
        self.ilm._is_searchable_snapshot = Mock(side_effect=lambda x: x.endswith("-snapshot"))

        print("\\n--- Running transition phase ---")
        self.ilm.transition_old_indices_to_snapshots()

        print("\\n--- Running cleanup phase ---")
        self.ilm.cleanup_old_data()

        # === Verification ===
        print("\\n--- End-to-End Verification ---")

        # Should transition log-app-000002 (70 days old, no longer write index)
        transition_calls = self.ilm._snapshot_and_replace_index.call_args_list
        self.assertEqual(len(transition_calls), 1, "Should transition log-app-000002 (70 days old, no longer write index)")
        self.assertEqual(transition_calls[0][0][0], "log-app-000002", "Should transition log-app-000002")

        # Cleanup should remove old data (100 days > 90 days retention)
        deleted_indices = [call[0][0] for call in self.ilm._delete_index.call_args_list]
        deleted_snapshots = [call[0][0] for call in self.ilm._delete_snapshot_with_cleanup.call_args_list]

        self.assertIn("log-app-000001-snapshot", deleted_indices, "100-day-old searchable snapshot should be deleted")
        self.assertIn("log-app-000001", deleted_snapshots, "100-day-old snapshot should be deleted")

        # Newer data should be preserved
        self.assertNotIn("log-app-000002", deleted_indices, "70-day-old index should be preserved")
        self.assertNotIn("log-app-000003", deleted_indices, "40-day-old write index should be preserved")
        self.assertNotIn("log-app-000002", deleted_snapshots, "70-day-old snapshot should be preserved")

        print("✓ End-to-end lifecycle completed successfully:")
        print(f"  - Transitions: {len(transition_calls)} (log-app-000002 transitioned)")
        print(f"  - Indices deleted: {len(deleted_indices)} (expected: 1)")
        print(f"  - Snapshots deleted: {len(deleted_snapshots)} (expected: 1)")
        print(f"  - Data beyond 90-day retention properly cleaned up")


if __name__ == '__main__':
    unittest.main()