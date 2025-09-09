#!/usr/bin/env python3

import unittest
import json
from unittest.mock import Mock, patch
import sys
import os

# Add parent directory to path to import modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from snapshot import Snapshot
from settings import Settings


class TestSnapshot(unittest.TestCase):
    """Unit tests for Snapshot class"""

    def setUp(self):
        """Set up test fixtures before each test method."""
        self.mock_settings = Mock(spec=Settings)
        self.mock_settings.url = "https://test-opensearch:9200"
        self.mock_settings.bucket = "test-bucket"
        self.mock_settings.repository = "data"
        
        self.mock_requests = Mock()
        self.mock_settings.get_requests_object.return_value = self.mock_requests

    @patch('snapshot.Snapshot.register_bucket')
    def test_init(self, mock_register_bucket):
        """Test Snapshot initialization"""
        mock_register_bucket.return_value = True
        snapshot = Snapshot(self.mock_settings)
        
        self.assertEqual(snapshot.base_url, "https://test-opensearch:9200")
        self.assertEqual(snapshot.bucket_name, "test-bucket")
        self.assertEqual(snapshot.repository, "data")
        self.assertEqual(snapshot.requests, self.mock_requests)
        
        # Check indices list
        expected_indices = [".kibana*", 
                           ".opensearch-sap-pre-packaged-rules-config", 
                           ".plugins-ml-config", 
                           ".opensearch-observability", 
                           ".opensearch-notifications-config", 
                           ".opensearch-sap-log-types-config"]
        self.assertEqual(snapshot.indices, expected_indices)
        
        # Verify register_bucket was called
        mock_register_bucket.assert_called_once()

    def test_register_bucket_already_exists(self):
        """Test register_bucket when repository already exists"""
        mock_response = Mock()
        mock_response.status_code = 200
        self.mock_requests.get.return_value = mock_response
        
        with patch('snapshot.Snapshot.__init__', return_value=None):
            snapshot = Snapshot.__new__(Snapshot)
            snapshot.base_url = self.mock_settings.url
            snapshot.repository = self.mock_settings.repository
            snapshot.requests = self.mock_requests
            result = snapshot.register_bucket()
        
        self.assertTrue(result)
        self.mock_requests.get.assert_called_with("https://test-opensearch:9200/_snapshot/data")

    def test_register_bucket_new_repository_success(self):
        """Test register_bucket when creating new repository successfully"""
        # Mock 404 response for initial check
        mock_get_response = Mock()
        mock_get_response.status_code = 404
        
        # Mock 200 response for repository creation
        mock_put_response = Mock()
        mock_put_response.status_code = 200
        
        self.mock_requests.get.return_value = mock_get_response
        self.mock_requests.put.return_value = mock_put_response
        
        with patch('snapshot.Snapshot.__init__', return_value=None):
            snapshot = Snapshot.__new__(Snapshot)
            snapshot.base_url = self.mock_settings.url
            snapshot.repository = self.mock_settings.repository
            snapshot.bucket_name = self.mock_settings.bucket
            snapshot.requests = self.mock_requests
            result = snapshot.register_bucket()
        
        self.assertTrue(result)
        
        # Verify repository creation call
        expected_payload = {
            "type": "s3",
            "settings": {
                "bucket": "test-bucket",
                "compress": True
            }
        }
        self.mock_requests.put.assert_called_with(
            "https://test-opensearch:9200/_snapshot/data", 
            data=json.dumps(expected_payload)
        )

    def test_register_bucket_new_repository_failure(self):
        """Test register_bucket when repository creation fails"""
        # Mock 404 response for initial check
        mock_get_response = Mock()
        mock_get_response.status_code = 404
        
        # Mock 500 response for repository creation failure
        mock_put_response = Mock()
        mock_put_response.status_code = 500
        mock_put_response.text = "Internal server error"
        
        self.mock_requests.get.return_value = mock_get_response
        self.mock_requests.put.return_value = mock_put_response
        
        with patch('snapshot.Snapshot.__init__', return_value=None):
            snapshot = Snapshot.__new__(Snapshot)
            snapshot.base_url = self.mock_settings.url
            snapshot.repository = self.mock_settings.repository
            snapshot.bucket_name = self.mock_settings.bucket
            snapshot.requests = self.mock_requests
            result = snapshot.register_bucket()
        
        self.assertFalse(result)

    def test_restore_snapshot_success(self):
        """Test successful snapshot restoration"""
        # Mock successful delete responses for indices
        mock_delete_response = Mock()
        mock_delete_response.status_code = 200
        
        # Mock successful restore response
        mock_restore_response = Mock()
        mock_restore_response.status_code = 200
        
        self.mock_requests.delete.return_value = mock_delete_response
        self.mock_requests.post.return_value = mock_restore_response
        
        with patch('snapshot.Snapshot.__init__', return_value=None):
            snapshot = Snapshot.__new__(Snapshot)
            snapshot.base_url = self.mock_settings.url
            snapshot.repository = self.mock_settings.repository
            snapshot.bucket_name = self.mock_settings.bucket
            snapshot.requests = self.mock_requests
            snapshot.indices = [".kibana*", 
                               ".opensearch-sap-pre-packaged-rules-config", 
                               ".plugins-ml-config", 
                               ".opensearch-observability", 
                               ".opensearch-notifications-config", 
                               ".opensearch-sap-log-types-config"]
            result = snapshot.restore_snapshot("test-snapshot-123")
        
        self.assertTrue(result)
        
        # Verify delete calls for each index
        self.assertEqual(self.mock_requests.delete.call_count, len(snapshot.indices))
        
        # Verify restore call
        expected_restore_payload = {
            "indices": snapshot.indices,
            "include_global_state": False
        }
        self.mock_requests.post.assert_called_with(
            "https://test-opensearch:9200/_snapshot/data/test-snapshot-123/_restore",
            data=json.dumps(expected_restore_payload)
        )

    def test_restore_snapshot_failure(self):
        """Test failed snapshot restoration"""
        # Mock successful delete responses for indices
        mock_delete_response = Mock()
        mock_delete_response.status_code = 200
        
        # Mock failed restore response
        mock_restore_response = Mock()
        mock_restore_response.status_code = 500
        mock_restore_response.text = "Restore failed"
        
        self.mock_requests.delete.return_value = mock_delete_response
        self.mock_requests.post.return_value = mock_restore_response
        
        with patch('snapshot.Snapshot.__init__', return_value=None):
            snapshot = Snapshot.__new__(Snapshot)
            snapshot.base_url = self.mock_settings.url
            snapshot.repository = self.mock_settings.repository
            snapshot.bucket_name = self.mock_settings.bucket
            snapshot.requests = self.mock_requests
            snapshot.indices = [".kibana*", 
                               ".opensearch-sap-pre-packaged-rules-config", 
                               ".plugins-ml-config", 
                               ".opensearch-observability", 
                               ".opensearch-notifications-config", 
                               ".opensearch-sap-log-types-config"]
            result = snapshot.restore_snapshot("test-snapshot-123")
        
        self.assertFalse(result)

    def test_get_snapshots_success(self):
        """Test successful snapshots listing"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = "snapshot1 SUCCESS\nsnapshot2 SUCCESS"
        
        self.mock_requests.get.return_value = mock_response
        
        with patch('snapshot.Snapshot.__init__', return_value=None):
            snapshot = Snapshot.__new__(Snapshot)
            snapshot.base_url = self.mock_settings.url
            snapshot.repository = self.mock_settings.repository
            snapshot.bucket_name = self.mock_settings.bucket
            snapshot.requests = self.mock_requests
            snapshot.indices = [".kibana*", 
                               ".opensearch-sap-pre-packaged-rules-config", 
                               ".plugins-ml-config", 
                               ".opensearch-observability", 
                               ".opensearch-notifications-config", 
                               ".opensearch-sap-log-types-config"]
            result = snapshot.get_snapshots()
        
        self.assertTrue(result)
        self.mock_requests.get.assert_called_with(
            "https://test-opensearch:9200/_cat/snapshots/data?v&s=endEpoch"
        )

    def test_get_snapshots_failure(self):
        """Test failed snapshots listing"""
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.text = "Internal server error"
        
        self.mock_requests.get.return_value = mock_response
        
        with patch('snapshot.Snapshot.__init__', return_value=None):
            snapshot = Snapshot.__new__(Snapshot)
            snapshot.base_url = self.mock_settings.url
            snapshot.repository = self.mock_settings.repository
            snapshot.bucket_name = self.mock_settings.bucket
            snapshot.requests = self.mock_requests
            snapshot.indices = [".kibana*", 
                               ".opensearch-sap-pre-packaged-rules-config", 
                               ".plugins-ml-config", 
                               ".opensearch-observability", 
                               ".opensearch-notifications-config", 
                               ".opensearch-sap-log-types-config"]
            result = snapshot.get_snapshots()
        
        self.assertFalse(result)

    def test_get_latest_snapshot_success(self):
        """Test successful latest snapshot retrieval"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = json.dumps([
            {"id": "snapshot1", "status": "SUCCESS"},
            {"id": "snapshot2", "status": "SUCCESS"},
            {"id": "snapshot3", "status": "SUCCESS"}
        ])
        
        self.mock_requests.get.return_value = mock_response
        
        with patch('snapshot.Snapshot.__init__', return_value=None):
            snapshot = Snapshot.__new__(Snapshot)
            snapshot.base_url = self.mock_settings.url
            snapshot.repository = self.mock_settings.repository
            snapshot.bucket_name = self.mock_settings.bucket
            snapshot.requests = self.mock_requests
            snapshot.indices = [".kibana*", 
                               ".opensearch-sap-pre-packaged-rules-config", 
                               ".plugins-ml-config", 
                               ".opensearch-observability", 
                               ".opensearch-notifications-config", 
                               ".opensearch-sap-log-types-config"]
            result = snapshot.get_latest_snapshot()
        
        self.assertEqual(result, "snapshot3")
        self.mock_requests.get.assert_called_with(
            "https://test-opensearch:9200/_cat/snapshots/data?v&s=endEpoch&format=json"
        )

    def test_get_latest_snapshot_failure(self):
        """Test failed latest snapshot retrieval"""
        mock_response = Mock()
        mock_response.status_code = 500
        
        self.mock_requests.get.return_value = mock_response
        
        with patch('snapshot.Snapshot.__init__', return_value=None):
            snapshot = Snapshot.__new__(Snapshot)
            snapshot.base_url = self.mock_settings.url
            snapshot.repository = self.mock_settings.repository
            snapshot.bucket_name = self.mock_settings.bucket
            snapshot.requests = self.mock_requests
            snapshot.indices = [".kibana*", 
                               ".opensearch-sap-pre-packaged-rules-config", 
                               ".plugins-ml-config", 
                               ".opensearch-observability", 
                               ".opensearch-notifications-config", 
                               ".opensearch-sap-log-types-config"]
            result = snapshot.get_latest_snapshot()
        
        self.assertEqual(result, "")

    def test_get_latest_snapshot_empty_list(self):
        """Test latest snapshot retrieval with empty snapshot list"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = json.dumps([])
        
        self.mock_requests.get.return_value = mock_response
        
        with patch('snapshot.Snapshot.__init__', return_value=None):
            snapshot = Snapshot.__new__(Snapshot)
            snapshot.base_url = self.mock_settings.url
            snapshot.repository = self.mock_settings.repository
            snapshot.bucket_name = self.mock_settings.bucket
            snapshot.requests = self.mock_requests
            snapshot.indices = [".kibana*", 
                               ".opensearch-sap-pre-packaged-rules-config", 
                               ".plugins-ml-config", 
                               ".opensearch-observability", 
                               ".opensearch-notifications-config", 
                               ".opensearch-sap-log-types-config"]
            with self.assertRaises(IndexError):
                snapshot.get_latest_snapshot()


if __name__ == '__main__':
    unittest.main()