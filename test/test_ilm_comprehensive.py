import unittest
import unittest.mock
import time
from unittest.mock import Mock, patch, MagicMock, call
import sys
import os

# Add parent directory to path to import modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ilm import Ilm
from settings import Settings


class TestIlmComprehensive(unittest.TestCase):
    """Comprehensive tests for ILM functionality"""

    def setUp(self):
        """Setup test fixtures"""
        self.settings = Settings(
            url="https://test-opensearch:9200",
            bucket="test-bucket",
            cert_file_path="/test/cert.pem",
            key_file_path="/test/key.pem",
            number_of_days_on_hot_storage=7,
            number_of_days_total_retention=90,
            repository="test-repo",
            rollover_age_days=30
        )
        
        # Mock the requests object
        self.mock_requests = Mock()
        with patch.object(self.settings, 'get_requests_object', return_value=self.mock_requests):
            self.ilm = Ilm(self.settings)

    def test_initialization(self):
        """Test ILM initialization with correct settings"""
        self.assertEqual(self.ilm.hot_storage_days, 7)
        self.assertEqual(self.ilm.total_retention_days, 90)
        self.assertEqual(self.ilm.rollover_size_gb, 50)  # Default value from Settings
        self.assertEqual(self.ilm.rollover_age_days, 30)  # Default value from Settings
        self.assertEqual(self.ilm.base_url, "https://test-opensearch:9200")

    def test_configuration_validation(self):
        """Test configuration validation"""
        # Test that validation is called during initialization
        with patch.object(Ilm, '_validate_configuration') as mock_validate:
            settings = Settings(
                url="https://test",
                bucket="test", 
                cert_file_path="/test",
                key_file_path="/test",
                number_of_days_on_hot_storage=7,
                number_of_days_total_retention=90,
                repository="test",
                rollover_age_days=30
            )
            Ilm(settings)
            mock_validate.assert_called_once()

    def test_rollover_age_validation_success(self):
        """Test that valid rollover age values are accepted"""
        settings = Settings(
            url="https://test",
            bucket="test",
            cert_file_path="/test",
            key_file_path="/test", 
            number_of_days_on_hot_storage=7,
            number_of_days_total_retention=90,
            repository="test",
            rollover_age_days=30  # Valid value
        )
        # Should not raise any exception
        ilm = Ilm(settings)
        self.assertEqual(ilm.rollover_age_days, 30)

    def test_rollover_age_validation_invalid_zero(self):
        """Test that zero rollover age is rejected"""
        settings = Settings(
            url="https://test",
            bucket="test",
            cert_file_path="/test", 
            key_file_path="/test",
            number_of_days_on_hot_storage=7,
            number_of_days_total_retention=90,
            repository="test",
            rollover_age_days=0  # Invalid: zero
        )
        with self.assertRaises(ValueError) as cm:
            Ilm(settings)
        self.assertIn("Rollover age must be > 0", str(cm.exception))

    def test_rollover_age_validation_invalid_negative(self):
        """Test that negative rollover age is rejected"""
        settings = Settings(
            url="https://test",
            bucket="test", 
            cert_file_path="/test",
            key_file_path="/test",
            number_of_days_on_hot_storage=7,
            number_of_days_total_retention=90,
            repository="test",
            rollover_age_days=-5  # Invalid: negative
        )
        with self.assertRaises(ValueError) as cm:
            Ilm(settings)
        self.assertIn("Rollover age must be > 0", str(cm.exception))

    def test_get_managed_indices(self):
        """Test optimized managed indices fetching"""
        # Mock pattern-based index fetching
        self.ilm._get_indices_by_pattern = Mock(side_effect=[
            [{"index": "log-000001"}, {"index": "log-000002"}],  # log* pattern
            [{"index": "alert-000001"}]  # alert* pattern
        ])
        
        result = self.ilm.get_managed_indices()
        
        # Should call pattern-based fetch for each managed pattern
        expected_calls = [
            unittest.mock.call("log*"),
            unittest.mock.call("alert*")
        ]
        self.ilm._get_indices_by_pattern.assert_has_calls(expected_calls)
        
        # Should return deduplicated results
        self.assertEqual(len(result), 3)
        index_names = {idx["index"] for idx in result}
        self.assertEqual(index_names, {"log-000001", "log-000002", "alert-000001"})

    def test_should_manage_index(self):
        """Test index management filtering"""
        # Mock _is_write_index to return False for regular indices
        self.ilm._is_write_index = Mock(return_value=False)

        # Should manage log indices
        self.assertTrue(self.ilm._should_manage_index("log-suricata-tls-000001"))
        self.assertTrue(self.ilm._should_manage_index("alert-ids-000002"))

        # Should not manage write aliases (now using robust write index detection)
        self.ilm._is_write_index = Mock(return_value=True)
        self.assertFalse(self.ilm._should_manage_index("log-suricata-tls-write"))

        # Reset for non-write index tests
        self.ilm._is_write_index = Mock(return_value=False)

        # Should not manage system indices
        self.assertFalse(self.ilm._should_manage_index(".kibana-1"))
        self.assertFalse(self.ilm._should_manage_index("random-index"))

    def test_get_index_age_days(self):
        """Test index age calculation"""
        # Mock index settings with creation date 10 days ago
        ten_days_ago_ms = (time.time() - (10 * 86400)) * 1000
        mock_response = Mock()
        mock_response.status_code = 200  # Add missing status_code
        mock_response.json.return_value = {
            "test-index": {
                "settings": {
                    "index": {
                        "creation_date": str(int(ten_days_ago_ms))
                    }
                }
            }
        }
        self.mock_requests.get.return_value = mock_response
        
        age = self.ilm._get_index_age_days("test-index")
        self.assertAlmostEqual(age, 10, delta=0.1)

    def test_is_write_index(self):
        """Test write index detection"""
        # Mock alias response showing write index
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "log-test-000001": {
                "aliases": {
                    "log-test-write": {
                        "is_write_index": True
                    }
                }
            }
        }
        self.mock_requests.get.return_value = mock_response
        
        self.assertTrue(self.ilm._is_write_index("log-test-000001"))

    def test_is_write_index_false(self):
        """Test write index detection returns false for non-write index"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "log-test-000001": {
                "aliases": {
                    "log-test-read": {
                        "is_write_index": False
                    }
                }
            }
        }
        self.mock_requests.get.return_value = mock_response
        
        self.assertFalse(self.ilm._is_write_index("log-test-000001"))

    def test_is_searchable_snapshot(self):
        """Test searchable snapshot detection"""
        # Mock settings response for searchable snapshot
        mock_response = Mock()
        mock_response.status_code = 200  # Add missing status_code
        mock_response.json.return_value = {
            "log-test-000001-snapshot": {
                "settings": {
                    "index": {
                        "store": {
                            "type": "remote_snapshot"
                        }
                    }
                }
            }
        }
        self.mock_requests.get.return_value = mock_response
        
        self.assertTrue(self.ilm._is_searchable_snapshot("log-test-000001-snapshot"))


    def test_get_write_aliases(self):
        """Test getting write aliases"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "log-test-000001": {
                "aliases": {
                    "log-test-write": {
                        "is_write_index": True
                    },
                    "log-test-read": {
                        "is_write_index": False
                    }
                }
            },
            "alert-test-000001": {
                "aliases": {
                    "alert-test-write": {
                        "is_write_index": True
                    }
                }
            }
        }
        self.mock_requests.get.return_value = mock_response
        
        aliases = self.ilm._get_write_aliases()
        self.assertIn("log-test-write", aliases)
        self.assertIn("alert-test-write", aliases)
        self.assertNotIn("log-test-read", aliases)

    def test_get_write_index(self):
        """Test getting write index for alias"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "log-test-000002": {
                "aliases": {
                    "log-test-write": {
                        "is_write_index": True
                    }
                }
            }
        }
        self.mock_requests.get.return_value = mock_response
        
        write_index = self.ilm._get_write_index("log-test-write")
        self.assertEqual(write_index, "log-test-000002")

    def test_create_snapshot(self):
        """Test snapshot creation with polling"""
        mock_response = Mock()
        mock_response.status_code = 200
        self.mock_requests.put.return_value = mock_response
        
        # Mock the polling method to return success immediately
        self.ilm._wait_for_snapshot_completion = Mock(return_value=True)
        
        result = self.ilm._create_snapshot("log-test-000001")
        self.assertTrue(result)
        
        # Verify correct API call (without wait_for_completion=true)
        self.mock_requests.put.assert_called_once()
        args, kwargs = self.mock_requests.put.call_args
        self.assertIn("_snapshot/data/log-test-000001", args[0])
        self.assertNotIn("wait_for_completion=true", args[0])
        self.assertEqual(kwargs['json']['indices'], ["log-test-000001"])
        
        # Verify polling was called
        self.ilm._wait_for_snapshot_completion.assert_called_once_with("log-test-000001")

    def test_create_snapshot_already_exists(self):
        """Test snapshot creation when snapshot already exists"""
        mock_response = Mock()
        mock_response.status_code = 400  # Already exists
        self.mock_requests.put.return_value = mock_response
        
        # Mock the polling method to return success
        self.ilm._wait_for_snapshot_completion = Mock(return_value=True)
        
        result = self.ilm._create_snapshot("log-test-000001")
        self.assertTrue(result)  # Should return True even for existing snapshots
        
        # Verify polling was called for existing snapshot
        self.ilm._wait_for_snapshot_completion.assert_called_once_with("log-test-000001")

    @patch('time.sleep')  # Mock sleep to speed up tests
    def test_wait_for_snapshot_completion_success(self, mock_sleep):
        """Test polling for snapshot completion - success case"""
        snapshot_name = "log-test-000001"
        
        # Mock status API response for successful completion
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "snapshots": [{
                "state": "SUCCESS"
            }]
        }
        self.mock_requests.get.return_value = mock_response
        
        result = self.ilm._wait_for_snapshot_completion(snapshot_name, max_wait_minutes=1)
        self.assertTrue(result)
        
        # Verify status API was called
        self.mock_requests.get.assert_called_with(
            f"https://test-opensearch:9200/_snapshot/data/{snapshot_name}/_status"
        )

    @patch('time.sleep')
    def test_wait_for_snapshot_completion_partial(self, mock_sleep):
        """Test polling for snapshot completion - partial success case"""
        snapshot_name = "log-test-000001"
        
        # Mock status API response for partial completion
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "snapshots": [{
                "state": "PARTIAL"
            }]
        }
        self.mock_requests.get.return_value = mock_response
        
        result = self.ilm._wait_for_snapshot_completion(snapshot_name, max_wait_minutes=1)
        self.assertTrue(result)  # PARTIAL is acceptable

    @patch('time.sleep')
    def test_wait_for_snapshot_completion_failed(self, mock_sleep):
        """Test polling for snapshot completion - failed case"""
        snapshot_name = "log-test-000001"
        
        # Mock status API response for failed snapshot
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "snapshots": [{
                "state": "FAILED"
            }]
        }
        self.mock_requests.get.return_value = mock_response
        
        result = self.ilm._wait_for_snapshot_completion(snapshot_name, max_wait_minutes=1)
        self.assertFalse(result)

    @patch('time.sleep')
    def test_wait_for_snapshot_completion_in_progress_then_success(self, mock_sleep):
        """Test polling for snapshot completion - in progress then success"""
        snapshot_name = "log-test-000001"
        
        # Mock status API responses: first IN_PROGRESS, then SUCCESS
        mock_responses = [
            Mock(status_code=200, 
                 json=Mock(return_value={"snapshots": [{"state": "IN_PROGRESS"}]})),
            Mock(status_code=200,
                 json=Mock(return_value={"snapshots": [{"state": "SUCCESS"}]}))
        ]
        self.mock_requests.get.side_effect = mock_responses
        
        result = self.ilm._wait_for_snapshot_completion(snapshot_name, max_wait_minutes=1)
        self.assertTrue(result)
        
        # Verify it polled twice
        self.assertEqual(self.mock_requests.get.call_count, 2)
        mock_sleep.assert_called_once_with(30)  # Should sleep between polls

    @patch('time.sleep')
    def test_wait_for_snapshot_completion_timeout(self, mock_sleep):
        """Test polling for snapshot completion - timeout case"""
        snapshot_name = "log-test-000001"
        
        # Mock status API response always returning IN_PROGRESS
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "snapshots": [{
                "state": "IN_PROGRESS"
            }]
        }
        self.mock_requests.get.return_value = mock_response
        
        # Test with very short timeout (should make exactly 2 polls: 60/30 = 2)
        result = self.ilm._wait_for_snapshot_completion(snapshot_name, max_wait_minutes=1)
        self.assertFalse(result)  # Should timeout and return False
        
        # Should have made 2 calls (max_polls = 1 minute * 60 seconds / 30 second intervals = 2)
        self.assertEqual(self.mock_requests.get.call_count, 2)
        
        # Should have slept twice (after each IN_PROGRESS poll)
        self.assertEqual(mock_sleep.call_count, 2)
        mock_sleep.assert_has_calls([call(30), call(30)])

    def test_create_searchable_snapshot_success(self):
        """Test creating searchable snapshot when none exists"""
        index_name = "log-test-000001"
        
        # Mock that searchable snapshot doesn't exist
        self.ilm._index_exists = Mock(return_value=False)
        
        # Mock successful creation
        mock_response = Mock()
        mock_response.status_code = 200
        self.mock_requests.post.return_value = mock_response
        
        result = self.ilm._create_searchable_snapshot(index_name)
        self.assertTrue(result)
        
        # Verify API call
        self.mock_requests.post.assert_called_once()
        args, kwargs = self.mock_requests.post.call_args
        self.assertIn("/_restore", args[0])
        self.assertEqual(kwargs['json']['storage_type'], "remote_snapshot")

    def test_create_searchable_snapshot_already_exists_valid(self):
        """Test creating searchable snapshot when valid one already exists"""
        index_name = "log-test-000001"
        
        # Mock that searchable snapshot exists and is valid
        self.ilm._index_exists = Mock(return_value=True)
        self.ilm._is_searchable_snapshot = Mock(return_value=True)
        
        result = self.ilm._create_searchable_snapshot(index_name)
        self.assertTrue(result)
        
        # Should not make API call since it already exists
        self.mock_requests.post.assert_not_called()

    def test_create_searchable_snapshot_already_exists_invalid(self):
        """Test creating searchable snapshot when invalid index with same name exists"""
        index_name = "log-test-000001"
        
        # Mock that index exists but is not a searchable snapshot
        self.ilm._index_exists = Mock(return_value=True)
        self.ilm._is_searchable_snapshot = Mock(return_value=False)
        self.ilm._delete_index = Mock()
        
        # Mock successful creation after cleanup
        mock_response = Mock()
        mock_response.status_code = 200
        self.mock_requests.post.return_value = mock_response
        
        result = self.ilm._create_searchable_snapshot(index_name)
        self.assertTrue(result)
        
        # Should delete the invalid index first
        self.ilm._delete_index.assert_called_once_with("log-test-000001-snapshot")
        
        # Then create the searchable snapshot
        self.mock_requests.post.assert_called_once()

    def test_create_searchable_snapshot_failure(self):
        """Test creating searchable snapshot when API call fails"""
        index_name = "log-test-000001"
        
        # Mock that searchable snapshot doesn't exist
        self.ilm._index_exists = Mock(return_value=False)
        
        # Mock failed creation
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.text = "Internal error"
        self.mock_requests.post.return_value = mock_response
        
        result = self.ilm._create_searchable_snapshot(index_name)
        self.assertFalse(result)

    def test_graceful_handling_of_existing_searchable_snapshots(self):
        """Test the complete flow handles existing searchable snapshots gracefully"""
        index_name = "log-test-000001"
        
        # Mock successful snapshot creation/validation
        self.ilm._create_snapshot_with_validation = Mock(return_value=True)
        
        # Mock that searchable snapshot already exists and is valid
        self.ilm._create_searchable_snapshot = Mock(return_value=True)
        
        # Mock other methods
        self.ilm._delete_index = Mock()
        
        self.ilm._snapshot_and_replace_index(index_name)
        
        # Should complete successfully without retries
        self.ilm._create_snapshot_with_validation.assert_called_once()
        self.ilm._create_searchable_snapshot.assert_called_once()
        self.ilm._delete_index.assert_called_once_with(index_name)  # Delete original

    def test_delete_snapshot_with_cleanup_regular_snapshot(self):
        """Test deleting regular snapshot (no cleanup needed)"""
        snapshot_name = "log-test-000001"
        
        self.ilm._delete_snapshot = Mock()
        
        self.ilm._delete_snapshot_with_cleanup(snapshot_name)
        
        # Should directly delete snapshot without any index cleanup
        self.ilm._delete_snapshot.assert_called_once_with(snapshot_name)

    def test_delete_snapshot_with_cleanup_searchable_snapshot_exists(self):
        """Test deleting searchable snapshot when backing index exists"""
        snapshot_name = "log-test-000001-snapshot"
        
        self.ilm._index_exists = Mock(return_value=True)
        self.ilm._delete_index = Mock()
        self.ilm._delete_snapshot = Mock()
        
        self.ilm._delete_snapshot_with_cleanup(snapshot_name)
        
        # Should check if index exists
        self.ilm._index_exists.assert_called_once_with(snapshot_name)
        
        # Should delete index first, then snapshot
        self.ilm._delete_index.assert_called_once_with(snapshot_name)
        self.ilm._delete_snapshot.assert_called_once_with(snapshot_name)

    def test_delete_snapshot_with_cleanup_searchable_snapshot_no_index(self):
        """Test deleting searchable snapshot when backing index doesn't exist"""
        snapshot_name = "log-test-000001-snapshot"
        
        self.ilm._index_exists = Mock(return_value=False)
        self.ilm._delete_index = Mock()
        self.ilm._delete_snapshot = Mock()
        
        self.ilm._delete_snapshot_with_cleanup(snapshot_name)
        
        # Should check if index exists
        self.ilm._index_exists.assert_called_once_with(snapshot_name)
        
        # Should not try to delete index, only delete snapshot
        self.ilm._delete_index.assert_not_called()
        self.ilm._delete_snapshot.assert_called_once_with(snapshot_name)

    def test_snapshot_age_days_with_endepoch_key(self):
        """Test snapshot age calculation with 'endEpoch' key"""
        snapshot_row = {'endEpoch': '1640995200'}  # 2022-01-01 00:00:00 UTC in seconds
        age = self.ilm._snapshot_age_days(snapshot_row)
        self.assertIsInstance(age, float)
        self.assertGreater(age, 1000)  # Should be over 1000 days old

    def test_snapshot_age_days_with_end_epoch_key(self):
        """Test snapshot age calculation with 'end_epoch' key"""
        snapshot_row = {'end_epoch': '1640995200'}  # 2022-01-01 00:00:00 UTC in seconds
        age = self.ilm._snapshot_age_days(snapshot_row)
        self.assertIsInstance(age, float)
        self.assertGreater(age, 1000)  # Should be over 1000 days old

    def test_snapshot_age_days_missing_key(self):
        """Test snapshot age calculation with missing keys"""
        snapshot_row = {'id': 'test-snapshot', 'status': 'SUCCESS'}
        age = self.ilm._snapshot_age_days(snapshot_row)
        self.assertEqual(age, -1.0)  # Returns -1.0 for unknown age
    
    def test_snapshot_age_days_zero_end_epoch(self):
        """Test snapshot age calculation with zero end_epoch (the main bug)"""
        snapshot_row = {'id': 'test-snapshot', 'endEpoch': '0'}
        age = self.ilm._snapshot_age_days(snapshot_row)
        self.assertEqual(age, -1.0)  # Should return -1.0, not 20,000+ days
    
    def test_snapshot_age_days_zero_end_epoch_int(self):
        """Test snapshot age calculation with zero end_epoch as int"""
        snapshot_row = {'id': 'test-snapshot', 'endEpoch': 0}
        age = self.ilm._snapshot_age_days(snapshot_row)
        self.assertEqual(age, -1.0)  # Should return -1.0, not 20,000+ days
    
    def test_snapshot_age_days_fallback_to_start_epoch(self):
        """Test snapshot age calculation falls back to start_epoch when end_epoch is 0"""
        start_time = int(time.time()) - (10 * 24 * 60 * 60)  # 10 days ago
        snapshot_row = {
            'id': 'test-snapshot', 
            'endEpoch': '0',  # Invalid
            'startEpoch': str(start_time)  # Valid fallback
        }
        age = self.ilm._snapshot_age_days(snapshot_row)
        self.assertGreater(age, 9)  # Should be ~10 days old
        self.assertLess(age, 11)
    
    def test_snapshot_age_days_invalid_values(self):
        """Test snapshot age calculation with invalid values"""
        test_cases = [
            {'id': 'test1', 'endEpoch': 'invalid'},
            {'id': 'test2', 'endEpoch': ''},
            {'id': 'test3', 'endEpoch': None},
            {'id': 'test4', 'end_epoch': 'abc'},
            {'id': 'test5'}  # No time fields at all
        ]
        for snapshot_row in test_cases:
            age = self.ilm._snapshot_age_days(snapshot_row)
            self.assertEqual(age, -1.0, f"Failed for {snapshot_row}")
    
    def test_snapshot_age_days_api_fallback(self):
        """Test snapshot age calculation falls back to API call"""
        # Mock the detailed API response
        mock_response = Mock()
        mock_response.json.return_value = {
            'snapshots': [{
                'end_time_in_millis': int((time.time() - (15 * 24 * 60 * 60)) * 1000)  # 15 days ago
            }]
        }
        self.mock_requests.get.return_value = mock_response
        
        snapshot_row = {'id': 'test-snapshot'}  # No endEpoch field
        age = self.ilm._snapshot_age_days(snapshot_row)
        
        # Should call the detailed API
        self.mock_requests.get.assert_called_with(f"{self.ilm.base_url}/_snapshot/data/test-snapshot")
        self.assertGreater(age, 14)  # Should be ~15 days old
        self.assertLess(age, 16)

    def test_cleanup_skips_zero_age_snapshots(self):
        """Test cleanup properly skips snapshots with zero/invalid end_epoch"""
        # Mock snapshots with problematic ages
        mock_snapshots = [
            {'id': 'snapshot-zero', 'endEpoch': '0'},  # Should be skipped with warning
            {'id': 'snapshot-invalid', 'endEpoch': 'invalid'},  # Should be skipped silently
            {'id': 'snapshot-old', 'endEpoch': str(int(time.time() - (100 * 24 * 60 * 60)))}  # Should be deleted
        ]
        
        self.ilm.get_snapshots = Mock(return_value=mock_snapshots)
        self.ilm.get_indices = Mock(return_value=[])
        self.ilm._delete_snapshot_with_cleanup = Mock()
        
        self.ilm.cleanup_old_data()
        
        # Only the valid old snapshot should be deleted
        self.ilm._delete_snapshot_with_cleanup.assert_called_once_with('snapshot-old')

    def test_cleanup_warns_about_ridiculous_ages(self):
        """Test cleanup warns about snapshots that compute to ridiculous ages"""
        # Create a snapshot that would compute to ~20,000 days (simulate the original bug)
        # Use a very old timestamp to trigger the sanity check 
        mock_snapshots = [
            {'id': 'snapshot-ridiculous', 'endEpoch': '1'}  # Jan 1, 1970 - causes ~20,000 day age
        ]
        
        self.ilm.get_snapshots = Mock(return_value=mock_snapshots)
        self.ilm.get_indices = Mock(return_value=[])
        self.ilm._delete_snapshot_with_cleanup = Mock()
        
        # Just run cleanup and verify it doesn't delete the ridiculous snapshot
        self.ilm.cleanup_old_data()
        
        # Should not delete snapshot with ridiculous age
        self.ilm._delete_snapshot_with_cleanup.assert_not_called()

    def test_cleanup_logic_bug_fix(self):
        """Test that non-managed index alongside managed index does not trigger age_days reference error"""
        # Mock indices: one managed, one non-managed
        mock_indices = [
            {'index': 'log-test-000001'},  # managed
            {'index': 'kibana-dashboard-000001'}  # not managed
        ]
        
        self.ilm.get_indices = Mock(return_value=mock_indices)
        self.ilm._should_manage_index = Mock(side_effect=lambda x: x.startswith('log-'))
        self.ilm._get_index_age_days = Mock(return_value=100.0)  # Old enough (> 90 days retention)
        self.ilm._delete_index = Mock()
        self.ilm._get_corresponding_snapshot_name = Mock(return_value=None)
        self.ilm.get_snapshots = Mock(return_value=[])  # No snapshots to avoid phase 3
        
        # Should not raise any reference errors
        self.ilm.cleanup_old_data()
        
        # Only managed index should be processed
        self.ilm._get_index_age_days.assert_called_once_with('log-test-000001')
        self.ilm._delete_index.assert_called_once_with('log-test-000001')

    def test_restore_guard_old_snapshots_cleaned_up(self):
        """Test that old snapshots are assumed to be cleaned up by cleanup phase"""
        # After cleanup runs, old snapshots (>= retention period) should not exist
        # This test documents that we rely on cleanup to remove old snapshots
        # rather than checking age in restore logic
        
        # Mock only young and valid-age snapshots (cleanup would have removed old ones)
        young_snapshot = {
            'id': 'log-test-young', 
            'status': 'SUCCESS',
            'end_epoch': int(time.time() - (5 * 24 * 60 * 60))  # 5 days old (in seconds)
        }
        valid_snapshot = {
            'id': 'log-test-valid',
            'status': 'SUCCESS', 
            'end_epoch': int(time.time() - (30 * 24 * 60 * 60))  # 30 days old (in seconds)
        }
        
        self.ilm.get_snapshots = Mock(return_value=[young_snapshot, valid_snapshot])
        self.ilm.get_indices = Mock(return_value=[])
        self.ilm._restore_as_searchable = Mock()
        
        # Mock snapshot details
        def mock_snapshot_details(url):
            mock_response = Mock()
            mock_response.json.return_value = {
                "snapshots": [{
                    "indices": [url.split("/")[-1]]
                }]
            }
            return mock_response
        self.mock_requests.get.side_effect = mock_snapshot_details
        self.ilm._should_manage_index = Mock(return_value=True)
        
        self.ilm.restore_missing_searchable_snapshots()
        
        # Should restore only the valid-age snapshot (young one is skipped)
        self.ilm._restore_as_searchable.assert_called_once_with("log-test-valid", set())

    def test_restore_guard_too_young(self):
        """Test that snapshots younger than hot storage are not restored"""
        # Mock a snapshot that's too young
        young_snapshot = {
            'id': 'log-test-young',
            'status': 'SUCCESS', 
            'endEpoch': str(int((time.time() - (5 * 24 * 60 * 60)) * 1000))  # 5 days old
        }
        
        self.ilm.get_snapshots = Mock(return_value=[young_snapshot])
        self.ilm.get_indices = Mock(return_value=[])
        self.ilm._restore_as_searchable = Mock()
        
        self.ilm.restore_missing_searchable_snapshots()
        
        # Should not restore - too young (5 < 7 days hot storage)
        self.ilm._restore_as_searchable.assert_not_called()

    def test_restore_happy_path(self):
        """Test that valid snapshots in the restoration window are restored"""
        # Mock a snapshot in the valid age range (between hot and retention)
        valid_snapshot = {
            'id': 'log-test-valid',
            'status': 'SUCCESS',
            'end_epoch': int(time.time() - (30 * 24 * 60 * 60))  # 30 days old (in seconds)
        }
        
        # Mock snapshot details
        mock_details = {
            'snapshots': [{'indices': ['log-test-000001']}]
        }
        mock_response = Mock()
        mock_response.json.return_value = mock_details
        
        self.ilm.get_snapshots = Mock(return_value=[valid_snapshot])
        self.ilm.get_indices = Mock(return_value=[])  # No existing indices
        self.ilm._should_manage_index = Mock(return_value=True)
        self.ilm._restore_as_searchable = Mock()
        self.mock_requests.get.return_value = mock_response
        
        self.ilm.restore_missing_searchable_snapshots()
        
        # Should restore - valid age and no existing indices
        self.ilm._restore_as_searchable.assert_called_once_with('log-test-valid', set())

    def test_three_phase_cleanup_order(self):
        """Test that three-phase cleanup processes in correct order"""
        # Mock indices: searchable snapshot, regular index
        mock_indices = [
            {'index': 'log-old-000001-snapshot'},  # searchable snapshot
            {'index': 'log-old-000001'}  # regular index
        ]
        
        # Mock old snapshots (including the one backing the searchable snapshot)
        mock_snapshots = [
            {
                'id': 'log-old-000001',  # Backing snapshot for searchable index
                'end_epoch': int(time.time() - (200 * 24 * 60 * 60))  # 200 days old (in seconds)
            },
            {
                'id': 'log-orphan-000001',  # Orphan snapshot
                'end_epoch': int(time.time() - (200 * 24 * 60 * 60))  # 200 days old (in seconds)
            }
        ]
        
        deletion_order = []
        
        def track_index_deletion(index_name):
            deletion_order.append(f"index:{index_name}")
            
        def track_snapshot_deletion(snapshot_name):
            deletion_order.append(f"snapshot:{snapshot_name}")
        
        self.ilm.get_indices = Mock(return_value=mock_indices)
        self.ilm.get_snapshots = Mock(return_value=mock_snapshots)
        self.ilm._should_manage_index = Mock(return_value=True)
        self.ilm._get_index_age_days = Mock(return_value=200.0)  # Old enough for regular indices
        self.ilm._get_searchable_snapshot_age_days = Mock(return_value=200.0)  # Old enough for searchable snapshots
        self.ilm._get_corresponding_snapshot_name = Mock(return_value='log-old-000001')
        self.ilm._delete_index = Mock(side_effect=track_index_deletion)
        self.ilm._delete_snapshot = Mock(side_effect=track_snapshot_deletion)
        self.ilm._delete_snapshot_with_cleanup = Mock(side_effect=track_snapshot_deletion)
        
        self.ilm.cleanup_old_data()
        
        # Verify order: searchable snapshot index first, then regular index, then snapshots
        expected_order = [
            "index:log-old-000001-snapshot",  # Phase 1
            "index:log-old-000001",           # Phase 2
            "snapshot:log-old-000001",        # Phase 2 corresponding
            "snapshot:log-old-000001",        # Phase 3 (duplicate due to snapshot being old)
            "snapshot:log-orphan-000001"      # Phase 3 orphan
        ]
        self.assertEqual(deletion_order, expected_order)

    def test_rollover_alias(self):
        """Test alias rollover"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "rolled_over": True,
            "old_index": "log-test-000001",
            "new_index": "log-test-000002"
        }
        self.mock_requests.post.return_value = mock_response
        
        result = self.ilm._rollover_alias("log-test-write")
        self.assertTrue(result)
        
        # Verify correct API call with rollover conditions
        self.mock_requests.post.assert_called_once_with(
            "https://test-opensearch:9200/log-test-write/_rollover",
            json={"conditions": {"max_size": "50gb", "max_age": "30d"}}
        )


    def test_check_and_rollover_by_size(self):
        """Test rollover check delegated to OpenSearch"""
        # Mock write aliases
        self.ilm._get_write_aliases = Mock(return_value=["log-test-write"])
        
        # Mock write index
        self.ilm._get_write_index = Mock(return_value="log-test-000001")
        
        # Mock rollover - OpenSearch decides based on conditions
        self.ilm._rollover_alias = Mock(return_value=True)
        
        self.ilm.check_and_rollover_by_size()
        
        # Verify rollover was attempted (OpenSearch makes the decision)
        self.ilm._rollover_alias.assert_called_once_with("log-test-write")


    def test_is_ready_for_snapshot(self):
        """Test checking if index is ready for snapshot"""
        index_name = "log-test-000001"
        
        # Mock methods
        self.ilm._is_write_index = Mock(return_value=False)
        self.ilm._is_searchable_snapshot = Mock(return_value=False)
        self.ilm._get_index_age_days = Mock(return_value=10)  # Older than 7 days
        
        result = self.ilm._is_ready_for_snapshot(index_name)
        self.assertTrue(result)

    def test_is_ready_for_snapshot_write_index(self):
        """Test that write indices are not ready for snapshot"""
        index_name = "log-test-000001"
        
        self.ilm._is_write_index = Mock(return_value=True)
        
        result = self.ilm._is_ready_for_snapshot(index_name)
        self.assertFalse(result)

    def test_is_ready_for_snapshot_too_young(self):
        """Test that young indices are not ready for snapshot"""
        index_name = "log-test-000001"
        
        self.ilm._is_write_index = Mock(return_value=False)
        self.ilm._is_searchable_snapshot = Mock(return_value=False)
        self.ilm._get_index_age_days = Mock(return_value=5)  # Younger than 7 days
        
        result = self.ilm._is_ready_for_snapshot(index_name)
        self.assertFalse(result)

    @patch('time.sleep')  # Mock sleep to speed up tests
    def test_snapshot_and_replace_index_success(self, mock_sleep):
        """Test successful snapshot and replace process"""
        index_name = "log-test-000001"
        
        # Mock all the steps for success (force_merge is currently commented out)
        self.ilm._create_snapshot_with_validation = Mock(return_value=True)
        self.ilm._create_searchable_snapshot = Mock(return_value=True)
        self.ilm._delete_index = Mock()
        
        self.ilm._snapshot_and_replace_index(index_name)
        
        # Verify core steps were called
        self.ilm._create_snapshot_with_validation.assert_called_once_with(index_name)
        self.ilm._create_searchable_snapshot.assert_called_once_with(index_name)
        self.ilm._delete_index.assert_called_once_with(index_name)

    @patch('time.sleep')  # Mock sleep to prevent actual delays in retry logic
    def test_snapshot_and_replace_index_retry_on_failure(self, mock_sleep):
        """Test retry logic when snapshot creation fails"""
        index_name = "log-test-000001"
        
        # Mock methods
        self.ilm._force_merge = Mock()
        self.ilm._create_snapshot_with_validation = Mock(side_effect=[False, False, True])  # Fail twice, succeed third time
        self.ilm._create_searchable_snapshot = Mock(return_value=True)
        self.ilm._delete_index = Mock()
        self.ilm._cleanup_failed_snapshot = Mock()
        
        self.ilm._snapshot_and_replace_index(index_name, max_retries=3)
        
        # Verify retries happened
        self.assertEqual(self.ilm._create_snapshot_with_validation.call_count, 3)
        self.assertEqual(self.ilm._cleanup_failed_snapshot.call_count, 2)  # Called for first 2 failures
        self.ilm._delete_index.assert_called_once_with(index_name)  # Only called on final success

    @patch('time.sleep')  # Mock sleep to prevent actual delays in retry logic
    def test_snapshot_and_replace_index_all_retries_exhausted(self, mock_sleep):
        """Test behavior when all retries are exhausted"""
        index_name = "log-test-000001"
        
        # Mock methods - all attempts fail
        self.ilm._force_merge = Mock()
        self.ilm._create_snapshot_with_validation = Mock(return_value=False)
        self.ilm._cleanup_failed_snapshot = Mock()
        self.ilm._delete_index = Mock()
        
        self.ilm._snapshot_and_replace_index(index_name, max_retries=2)
        
        # Verify retries happened but original index was preserved
        self.assertEqual(self.ilm._create_snapshot_with_validation.call_count, 2)
        self.assertEqual(self.ilm._cleanup_failed_snapshot.call_count, 2)
        self.ilm._delete_index.assert_not_called()  # Original index should be preserved

    def test_hot_storage_equals_total_retention(self):
        """Test behavior when hot storage period equals total retention period"""
        # Create ILM with equal hot and total retention
        settings = Mock()
        settings.number_of_days_on_hot_storage = 30
        settings.number_of_days_total_retention = 30  # Same as hot storage
        settings.rollover_size_gb = 50
        settings.rollover_age_days = 30
        settings.managed_index_patterns = ("log", "alert")
        settings.get_requests_object = Mock()
        settings.url = "https://test"
        
        ilm_equal = Ilm(settings)
        
        # Mock get_indices to avoid API calls
        ilm_equal.get_indices = Mock(return_value=[])
        ilm_equal.get_snapshots = Mock(return_value=[])
        
        # Test transition_old_indices_to_snapshots - should skip
        ilm_equal.transition_old_indices_to_snapshots()
        ilm_equal.get_indices.assert_not_called()
        
        # Test restore_missing_searchable_snapshots - should proceed
        ilm_equal.restore_missing_searchable_snapshots()
        ilm_equal.get_snapshots.assert_called_once()

    def test_hot_storage_less_than_total_retention(self):
        """Test normal behavior when hot storage < total retention"""
        # Test with normal settings (hot_storage=7, total_retention=90)
        self.ilm.get_indices = Mock(return_value=[])
        self.ilm.get_snapshots = Mock(return_value=[])
        
        # Test transition_old_indices_to_snapshots - should proceed
        self.ilm.transition_old_indices_to_snapshots()
        self.ilm.get_indices.assert_called_once()
        
        # Reset mock
        self.ilm.get_snapshots.reset_mock()
        
        # Test restore_missing_searchable_snapshots - should proceed
        self.ilm.restore_missing_searchable_snapshots()
        self.ilm.get_snapshots.assert_called_once()

    def test_transition_old_indices_to_snapshots(self):
        """Test transitioning old indices to snapshots"""
        # Mock get_indices
        mock_indices = [
            {"index": "log-test-000001"},
            {"index": "log-test-write"},  # Should be filtered out by _should_manage_index
            {"index": ".kibana-1"},       # Should be filtered out by _should_manage_index
            {"index": "log-test-000002"}
        ]
        self.ilm.get_indices = Mock(return_value=mock_indices)
        
        # Mock other methods
        self.ilm._should_manage_index = Mock(side_effect=lambda x: x.startswith(("log", "alert")) and not x.endswith("-write"))
        self.ilm._is_ready_for_snapshot = Mock(side_effect=lambda x: x == "log-test-000001")
        self.ilm._snapshot_and_replace_index = Mock()
        
        self.ilm.transition_old_indices_to_snapshots()
        
        # Verify only the ready index was processed
        self.ilm._snapshot_and_replace_index.assert_called_once_with("log-test-000001")

    def test_cleanup_old_data(self):
        """Test cleanup of old indices and snapshots"""
        # Mock indices
        mock_indices = [
            {"index": "log-old-000001"},
            {"index": "log-new-000001"}
        ]
        self.ilm.get_indices = Mock(return_value=mock_indices)
        
        # Mock snapshots
        mock_snapshots = [
            {"id": "old-snapshot", "end_epoch": str(int(time.time() - (100 * 86400)))},  # 100 days old
            {"id": "new-snapshot", "end_epoch": str(int(time.time() - (10 * 86400)))}   # 10 days old
        ]
        self.ilm.get_snapshots = Mock(return_value=mock_snapshots)
        
        # Mock other methods
        self.ilm._should_manage_index = Mock(return_value=True)
        self.ilm._get_index_age_days = Mock(side_effect=lambda x: 100 if "old" in x else 10)
        self.ilm._get_corresponding_snapshot_name = Mock(return_value="log-old-000001")
        self.ilm._delete_index = Mock()
        self.ilm._delete_snapshot = Mock()
        
        self.ilm.cleanup_old_data()
        
        # Verify old index was deleted
        self.ilm._delete_index.assert_called_once_with("log-old-000001")
        
        # Verify snapshots were deleted: corresponding snapshot + old standalone snapshot
        expected_calls = [
            unittest.mock.call("log-old-000001"),  # Corresponding snapshot for deleted index
            unittest.mock.call("old-snapshot")     # Old standalone snapshot
        ]
        self.ilm._delete_snapshot.assert_has_calls(expected_calls, any_order=False)

    def test_cleanup_old_snapshots_with_real_data(self):
        """Test cleanup using real snapshot data from production to verify old snapshot deletion"""
        import time
        
        # Real snapshot data from production, but make them definitely older than 180 days
        very_old_timestamp = str(int(time.time() - (200 * 86400)))  # 200 days ago
        mock_snapshots = [
            {
                "id": "log-suricata-ssh-2025.02.21",
                "status": "SUCCESS", 
                "start_epoch": very_old_timestamp,  # 200 days ago
                "end_epoch": very_old_timestamp,    # 200 days ago 
                "endEpoch": very_old_timestamp      # Alternative field name
            },
            {
                "id": "log-cisco-ise-2025.02.21", 
                "status": "SUCCESS",
                "start_epoch": very_old_timestamp,  # 200 days ago
                "end_epoch": very_old_timestamp,    # 200 days ago
                "endEpoch": very_old_timestamp
            },
            {
                "id": "alerts-2025.02.21",
                "status": "SUCCESS", 
                "start_epoch": very_old_timestamp,  # 200 days ago
                "end_epoch": very_old_timestamp,    # 200 days ago
                "endEpoch": very_old_timestamp
            },
            {
                "id": "log-recent-data-2025.08.01",  # Recent snapshot, should not be deleted
                "status": "SUCCESS",
                "start_epoch": str(int(time.time() - (30 * 86400))),  # 30 days ago
                "end_epoch": str(int(time.time() - (30 * 86400))),
                "endEpoch": str(int(time.time() - (30 * 86400)))
            }
        ]
        
        # Mock searchable snapshot indices that correspond to old snapshots
        mock_indices = [
            {"index": "log-suricata-ssh-2025.02.21-snapshot"},    # Created recently from old snapshot
            {"index": "log-cisco-ise-2025.02.21-snapshot"},      # Created recently from old snapshot  
            {"index": "alerts-2025.02.21-snapshot"},             # Created recently from old snapshot
            {"index": "log-recent-data-2025.08.01-snapshot"},    # Recent, should not be deleted
            {"index": "log-regular-index-000001"}                # Regular index, not a searchable snapshot
        ]
        
        # Configure ILM with 180 day retention (matching production config)
        self.ilm.total_retention_days = 180
        
        # Mock methods
        self.ilm.get_snapshots = Mock(return_value=mock_snapshots)
        self.ilm.get_indices = Mock(return_value=mock_indices)
        self.ilm._should_manage_index = Mock(return_value=True)
        self.ilm._delete_snapshot_with_cleanup = Mock()
        self.ilm._delete_index = Mock()
        
        # Mock age calculation for indices (searchable snapshots have recent creation dates)
        def mock_index_age(index_name):
            if "2025.02.21" in index_name:
                return 5.0  # Recent creation date (searchable snapshots created recently)
            elif "2025.08.01" in index_name:
                return 30.0  # 30 days old
            else:
                return 10.0  # Default recent age
                
        self.ilm._get_index_age_days = Mock(side_effect=mock_index_age)
        self.ilm._index_exists = Mock(return_value=True)
        self.ilm._is_searchable_snapshot = Mock(lambda x: x.endswith('-snapshot'))
        
        # Run cleanup
        self.ilm.cleanup_old_data()
        
        # Verify that old snapshots are deleted despite searchable snapshot indices being recent
        expected_snapshot_deletions = [
            unittest.mock.call("log-suricata-ssh-2025.02.21"),
            unittest.mock.call("log-cisco-ise-2025.02.21"), 
            unittest.mock.call("alerts-2025.02.21")
        ]
        self.ilm._delete_snapshot_with_cleanup.assert_has_calls(expected_snapshot_deletions, any_order=True)
        
        # Verify recent snapshot is NOT deleted
        deleted_snapshots = [call[0][0] for call in self.ilm._delete_snapshot_with_cleanup.call_args_list]
        self.assertNotIn("log-recent-data-2025.08.01", deleted_snapshots)
        
        # Verify that old searchable snapshot indices ARE deleted in Phase 1
        # (now uses snapshot age, not index creation age)
        phase1_deletions = [call[0][0] for call in self.ilm._delete_index.call_args_list 
                           if call[0][0].endswith('-snapshot')]
        
        # Phase 1 should delete searchable snapshots older than retention based on SNAPSHOT creation date
        # The February 2025 snapshots are 200 days old (mocked), so they should be deleted
        old_searchable_indices_deleted_phase1 = [idx for idx in phase1_deletions if "2025.02.21" in idx]
        self.assertEqual(len(old_searchable_indices_deleted_phase1), 3, 
                        "Old searchable snapshot indices should be deleted in Phase 1 based on snapshot age")
        
        # Verify recent snapshot index is NOT deleted
        recent_indices_deleted = [idx for idx in phase1_deletions if "2025.08.01" in idx]
        self.assertEqual(len(recent_indices_deleted), 0,
                        "Recent searchable snapshot indices should not be deleted")

    def test_get_corresponding_snapshot_name(self):
        """Test getting corresponding snapshot name for an index"""
        # Test searchable snapshot index
        result = self.ilm._get_corresponding_snapshot_name("log-000001-snapshot")
        self.assertEqual(result, "log-000001")
        
        # Test regular index
        result = self.ilm._get_corresponding_snapshot_name("log-000001")
        self.assertEqual(result, "log-000001")
        
        # Test complex searchable snapshot name
        result = self.ilm._get_corresponding_snapshot_name("alert-system-2024.01.15-snapshot")
        self.assertEqual(result, "alert-system-2024.01.15")

    def test_should_manage_index_with_custom_patterns(self):
        """Test _should_manage_index with custom patterns"""
        # Create ILM with custom patterns
        settings = Mock()
        settings.number_of_days_on_hot_storage = 7
        settings.number_of_days_total_retention = 90
        settings.rollover_size_gb = 50
        settings.rollover_age_days = 30
        settings.managed_index_patterns = ("data", "metrics", "traces")
        settings.get_requests_object = Mock()
        settings.url = "https://test"
        
        custom_ilm = Ilm(settings)

        # Mock _is_write_index to return False for regular indices
        custom_ilm._is_write_index = Mock(return_value=False)

        # Test indices that should be managed
        self.assertTrue(custom_ilm._should_manage_index("data-000001"))
        self.assertTrue(custom_ilm._should_manage_index("metrics-host-000001"))
        self.assertTrue(custom_ilm._should_manage_index("traces-jaeger-000001"))

        # Test indices that should NOT be managed
        self.assertFalse(custom_ilm._should_manage_index("log-000001"))  # Not in custom patterns
        self.assertFalse(custom_ilm._should_manage_index("alert-000001"))  # Not in custom patterns
        self.assertFalse(custom_ilm._should_manage_index("system-000001"))  # Not in patterns

        # Test write aliases (now using robust write index detection)
        custom_ilm._is_write_index = Mock(return_value=True)
        self.assertFalse(custom_ilm._should_manage_index("data-write"))  # Write alias

    def test_should_manage_index_with_default_patterns(self):
        """Test _should_manage_index with default patterns (log, alert)"""
        # Mock _is_write_index to return False for regular indices
        self.ilm._is_write_index = Mock(return_value=False)

        # Test indices that should be managed with default patterns
        self.assertTrue(self.ilm._should_manage_index("log-000001"))
        self.assertTrue(self.ilm._should_manage_index("alert-000001"))
        self.assertTrue(self.ilm._should_manage_index("log-system-2024.01.15"))

        # Test indices that should NOT be managed
        self.assertFalse(self.ilm._should_manage_index("data-000001"))  # Not in default patterns
        self.assertFalse(self.ilm._should_manage_index(".kibana"))  # System index

        # Test write aliases (now using robust write index detection)
        self.ilm._is_write_index = Mock(return_value=True)
        self.assertFalse(self.ilm._should_manage_index("log-write"))  # Write alias

    def test_create_snapshot_with_validation_success(self):
        """Test snapshot creation with validation - success case"""
        index_name = "log-test-000001"
        
        self.ilm._create_snapshot = Mock(return_value=True)
        self.ilm._validate_snapshot_health = Mock(return_value=True)
        
        result = self.ilm._create_snapshot_with_validation(index_name)
        self.assertTrue(result)
        
        self.ilm._create_snapshot.assert_called_once_with(index_name)
        self.ilm._validate_snapshot_health.assert_called_once_with(index_name)

    def test_create_snapshot_with_validation_snapshot_fails(self):
        """Test snapshot creation with validation - snapshot creation fails"""
        index_name = "log-test-000001"
        
        self.ilm._create_snapshot = Mock(return_value=False)
        self.ilm._validate_snapshot_health = Mock()
        
        result = self.ilm._create_snapshot_with_validation(index_name)
        self.assertFalse(result)
        
        self.ilm._create_snapshot.assert_called_once_with(index_name)
        self.ilm._validate_snapshot_health.assert_not_called()  # Should not be called if snapshot creation fails

    def test_create_snapshot_with_validation_validation_fails(self):
        """Test snapshot creation with validation - validation fails"""
        index_name = "log-test-000001"
        
        self.ilm._create_snapshot = Mock(return_value=True)
        self.ilm._validate_snapshot_health = Mock(return_value=False)
        
        result = self.ilm._create_snapshot_with_validation(index_name)
        self.assertFalse(result)
        
        self.ilm._create_snapshot.assert_called_once_with(index_name)
        self.ilm._validate_snapshot_health.assert_called_once_with(index_name)

    def test_validate_snapshot_health_success(self):
        """Test snapshot health validation - success case"""
        snapshot_name = "log-test-000001"
        
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "snapshots": [{
                "state": "SUCCESS",
                "failures": []
            }]
        }
        self.mock_requests.get.return_value = mock_response
        
        result = self.ilm._validate_snapshot_health(snapshot_name)
        self.assertTrue(result)

    def test_validate_snapshot_health_failed_state(self):
        """Test snapshot health validation - failed state"""
        snapshot_name = "log-test-000001"
        
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "snapshots": [{
                "state": "FAILED",
                "failures": ["Index not found"]
            }]
        }
        self.mock_requests.get.return_value = mock_response
        
        result = self.ilm._validate_snapshot_health(snapshot_name)
        self.assertFalse(result)

    def test_validate_snapshot_health_with_failures(self):
        """Test snapshot health validation - SUCCESS with failures (now accepted with warning)"""
        snapshot_name = "log-test-000001"
        
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "snapshots": [{
                "state": "SUCCESS",
                "failures": ["Some warning"]
            }]
        }
        self.mock_requests.get.return_value = mock_response
        
        result = self.ilm._validate_snapshot_health(snapshot_name)
        self.assertTrue(result)  # Now accepts SUCCESS with failures but logs warning

    def test_validate_snapshot_health_partial_state(self):
        """Test snapshot health validation - PARTIAL state (accepted with warning)"""
        snapshot_name = "log-test-000001"
        
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "snapshots": [{
                "state": "PARTIAL",
                "failures": [
                    {
                        "index": "log-test-000001",
                        "shard_id": 4,
                        "reason": "IllegalStateException[Connection pool shut down]",
                        "status": "INTERNAL_SERVER_ERROR"
                    }
                ],
                "shards": {
                    "total": 5,
                    "successful": 3,
                    "failed": 2
                }
            }]
        }
        self.mock_requests.get.return_value = mock_response
        
        result = self.ilm._validate_snapshot_health(snapshot_name)
        self.assertTrue(result)  # PARTIAL snapshots are now accepted for restore

    def test_cleanup_failed_snapshot(self):
        """Test cleanup of failed snapshot artifacts - searchable index deleted first"""
        index_name = "log-test-000001"
        
        # Track call order
        call_order = []
        
        def mock_delete_index(name):
            call_order.append(f"delete_index:{name}")
            
        def mock_delete_snapshot(name):
            call_order.append(f"delete_snapshot:{name}")
        
        self.ilm._delete_snapshot = Mock(side_effect=mock_delete_snapshot)
        self.ilm._delete_index = Mock(side_effect=mock_delete_index)
        self.ilm._snapshot_exists = Mock(return_value=True)
        self.ilm._index_exists = Mock(return_value=True)
        
        self.ilm._cleanup_failed_snapshot(index_name)
        
        # Verify existence checks
        self.ilm._snapshot_exists.assert_called_once_with(index_name)
        self.ilm._index_exists.assert_called_once_with("log-test-000001-snapshot")
        
        # Verify cleanup calls when artifacts exist
        self.ilm._delete_index.assert_called_once_with("log-test-000001-snapshot")
        self.ilm._delete_snapshot.assert_called_once_with(index_name)
        
        # Verify correct order: searchable index deleted BEFORE snapshot
        self.assertEqual(call_order, [
            "delete_index:log-test-000001-snapshot",
            "delete_snapshot:log-test-000001"
        ])

    def test_cleanup_failed_snapshot_not_exist(self):
        """Test cleanup when artifacts don't exist"""
        index_name = "log-test-000001"
        
        self.ilm._delete_snapshot = Mock()
        self.ilm._delete_index = Mock()
        self.ilm._snapshot_exists = Mock(return_value=False)
        self.ilm._index_exists = Mock(return_value=False)
        
        self.ilm._cleanup_failed_snapshot(index_name)
        
        # Verify existence checks were made
        self.ilm._snapshot_exists.assert_called_once_with(index_name)
        self.ilm._index_exists.assert_called_once_with("log-test-000001-snapshot")
        
        # Verify no cleanup calls when artifacts don't exist
        self.ilm._delete_snapshot.assert_not_called()
        self.ilm._delete_index.assert_not_called()

    def test_restore_missing_searchable_snapshots(self):
        """Test comprehensive restore of missing searchable snapshots"""
        # Mock existing indices - missing some searchable snapshots
        mock_indices = [
            {"index": "log-test-000001"},  # Original exists
            {"index": "log-test-000002-snapshot"}  # Searchable exists
        ]
        self.ilm.get_indices = Mock(return_value=mock_indices)
        
        # Mock snapshots with age information (30 days old - within restoration window)
        old_time_seconds = int(time.time() - (30 * 24 * 60 * 60))
        
        mock_snapshots = [
            {"id": "log-test-000001", "status": "SUCCESS", "end_epoch": old_time_seconds},  # Has original, no searchable needed
            {"id": "log-test-000002", "status": "SUCCESS", "end_epoch": old_time_seconds},  # Has searchable, no restore needed  
            {"id": "log-test-000003", "status": "SUCCESS", "end_epoch": old_time_seconds},  # Missing both, should restore
            {"id": "log-test-000004", "status": "FAILED", "end_epoch": old_time_seconds}   # Failed snapshot, skip
        ]
        self.ilm.get_snapshots = Mock(return_value=mock_snapshots)
        
        # Mock snapshot details API calls
        def mock_snapshot_details(url):
            mock_response = Mock()
            mock_response.json.return_value = {
                "snapshots": [{
                    "indices": [url.split("/")[-1]]  # Extract snapshot name as index name
                }]
            }
            return mock_response
        
        self.mock_requests.get.side_effect = mock_snapshot_details
        
        # Mock methods
        self.ilm._should_manage_index = Mock(return_value=True)
        self.ilm._restore_as_searchable = Mock()
        
        self.ilm.restore_missing_searchable_snapshots()
        
        # Verify only the missing snapshot was restored (now with existing_indices parameter)
        expected_existing_indices = {"log-test-000001", "log-test-000002-snapshot"}
        self.ilm._restore_as_searchable.assert_called_once_with("log-test-000003", expected_existing_indices)

    def test_restore_missing_searchable_snapshots_no_missing(self):
        """Test restore when no snapshots are missing"""
        # Mock existing indices - all searchable snapshots exist
        mock_indices = [
            {"index": "log-test-000001-snapshot"},
            {"index": "log-test-000002-snapshot"}
        ]
        self.ilm.get_indices = Mock(return_value=mock_indices)
        
        # Mock snapshots
        mock_snapshots = [
            {"id": "log-test-000001", "status": "SUCCESS"},
            {"id": "log-test-000002", "status": "SUCCESS"}
        ]
        self.ilm.get_snapshots = Mock(return_value=mock_snapshots)
        
        # Mock snapshot details
        def mock_snapshot_details(url):
            mock_response = Mock()
            mock_response.json.return_value = {
                "snapshots": [{
                    "indices": [url.split("/")[-1]]
                }]
            }
            return mock_response
        
        self.mock_requests.get.side_effect = mock_snapshot_details
        self.ilm._should_manage_index = Mock(return_value=True)
        self.ilm._restore_as_searchable = Mock()
        
        self.ilm.restore_missing_searchable_snapshots()
        
        # Verify no restores were performed
        self.ilm._restore_as_searchable.assert_not_called()


if __name__ == '__main__':
    unittest.main(verbosity=2)