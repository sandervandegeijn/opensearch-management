#!/usr/bin/env python3

import unittest
import json
import yaml
from unittest.mock import Mock, patch, mock_open
import sys
import os

# Add parent directory to path to import modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ingest_pipeline_manager import IngestPipelineManager
from settings import Settings


class TestIngestPipelineManager(unittest.TestCase):
    """Unit tests for IngestPipelineManager class"""

    def setUp(self):
        """Set up test fixtures before each test method."""
        self.mock_settings = Mock(spec=Settings)
        self.mock_settings.url = "https://test-opensearch:9200"
        
        self.mock_requests = Mock()
        self.mock_settings.get_requests_object.return_value = self.mock_requests
        
        self.pipeline_manager = IngestPipelineManager(self.mock_settings)

    def test_init(self):
        """Test IngestPipelineManager initialization"""
        self.assertEqual(self.pipeline_manager.base_url, "https://test-opensearch:9200")
        self.assertEqual(self.pipeline_manager.requests, self.mock_requests)

    def test_read_yaml_valid_file(self):
        """Test reading a valid YAML file"""
        test_yaml_data = {
            "description": "Test pipeline",
            "processors": [
                {"set": {"field": "test_field", "value": "test_value"}}
            ]
        }
        yaml_content = yaml.dump(test_yaml_data)
        
        with patch("builtins.open", mock_open(read_data=yaml_content)):
            result = self.pipeline_manager._read_yaml("test_pipeline.yml")
        
        self.assertEqual(result, test_yaml_data)

    def test_read_yaml_invalid_file(self):
        """Test reading an invalid YAML file"""
        invalid_yaml = "invalid: yaml: content: ["
        
        with patch("builtins.open", mock_open(read_data=invalid_yaml)):
            with self.assertRaises(yaml.YAMLError):
                self.pipeline_manager._read_yaml("invalid_pipeline.yml")

    def test_upload_json_success(self):
        """Test successful pipeline upload"""
        mock_response = Mock()
        mock_response.status_code = 200
        self.mock_requests.put.return_value = mock_response
        
        test_json_data = json.dumps({
            "description": "Test pipeline",
            "processors": [{"set": {"field": "test", "value": "value"}}]
        })
        
        result = self.pipeline_manager._upload_json(test_json_data, "test-pipeline")
        
        self.assertEqual(result, 200)
        self.mock_requests.put.assert_called_with(
            "https://test-opensearch:9200/_ingest/pipeline/test-pipeline",
            data=test_json_data
        )

    def test_upload_json_failure(self):
        """Test failed pipeline upload"""
        mock_response = Mock()
        mock_response.status_code = 400
        mock_response.text = "Bad request - invalid pipeline"
        self.mock_requests.put.return_value = mock_response
        
        test_json_data = json.dumps({
            "description": "Invalid pipeline",
            "processors": []
        })
        
        result = self.pipeline_manager._upload_json(test_json_data, "invalid-pipeline")
        
        self.assertEqual(result, 400)

    @patch('glob.glob')
    @patch.object(IngestPipelineManager, '_read_yaml')
    @patch.object(IngestPipelineManager, '_upload_json')
    def test_sync_to_cluster_success(self, mock_upload, mock_read, mock_glob):
        """Test successful pipeline synchronization to cluster"""
        # Mock glob to return list of YAML files
        mock_glob.return_value = [
            "/path/to/pipelines/nginx.yml",
            "/path/to/pipelines/apache.yml",
            "/path/to/pipelines/filebeat.yml"
        ]
        
        # Mock YAML data for each pipeline
        mock_pipeline_data = {
            "description": "Test ingest pipeline",
            "processors": [
                {"grok": {"field": "message", "patterns": ["%{COMMONAPACHELOG}"]}},
                {"date": {"field": "timestamp", "formats": ["dd/MMM/yyyy:HH:mm:ss Z"]}}
            ]
        }
        mock_read.return_value = mock_pipeline_data
        
        # Mock successful upload
        mock_upload.return_value = 200
        
        self.pipeline_manager.sync_to_cluster("/path/to/pipelines")
        
        # Verify glob was called with correct pattern
        mock_glob.assert_called_with("/path/to/pipelines/*.yml")
        
        # Verify _read_yaml was called for each file
        self.assertEqual(mock_read.call_count, 3)
        mock_read.assert_any_call("/path/to/pipelines/nginx.yml")
        mock_read.assert_any_call("/path/to/pipelines/apache.yml")
        mock_read.assert_any_call("/path/to/pipelines/filebeat.yml")
        
        # Verify _upload_json was called for each pipeline
        self.assertEqual(mock_upload.call_count, 3)
        expected_json = json.dumps(mock_pipeline_data)
        mock_upload.assert_any_call(expected_json, "nginx")
        mock_upload.assert_any_call(expected_json, "apache")
        mock_upload.assert_any_call(expected_json, "filebeat")

    @patch('glob.glob')
    def test_sync_to_cluster_no_files(self, mock_glob):
        """Test synchronization when no YAML files are found"""
        # Mock glob to return empty list
        mock_glob.return_value = []
        
        # This should not raise an exception
        self.pipeline_manager.sync_to_cluster("/path/to/empty")
        
        # Verify glob was called
        mock_glob.assert_called_with("/path/to/empty/*.yml")

    @patch('os.path.basename')
    @patch('glob.glob')
    @patch.object(IngestPipelineManager, '_read_yaml')
    @patch.object(IngestPipelineManager, '_upload_json')
    def test_sync_to_cluster_basename_extraction(self, mock_upload, mock_read, mock_glob, mock_basename):
        """Test that pipeline names are correctly extracted from file paths"""
        mock_glob.return_value = ["/complex/path/to/my-custom-pipeline.yml"]
        mock_basename.return_value = "my-custom-pipeline.yml"
        mock_read.return_value = {"description": "Custom pipeline", "processors": []}
        mock_upload.return_value = 200
        
        self.pipeline_manager.sync_to_cluster("/complex/path/to")
        
        # Verify basename was called to extract filename
        mock_basename.assert_called_with("/complex/path/to/my-custom-pipeline.yml")
        
        # Verify pipeline name was correctly stripped of .yml extension
        expected_json = json.dumps({"description": "Custom pipeline", "processors": []})
        mock_upload.assert_called_with(expected_json, "my-custom-pipeline")

    @patch('glob.glob')
    @patch.object(IngestPipelineManager, '_read_yaml')
    @patch.object(IngestPipelineManager, '_upload_json')
    def test_sync_to_cluster_mixed_results(self, mock_upload, mock_read, mock_glob):
        """Test synchronization with mixed success/failure results"""
        # Mock glob to return list of YAML files
        mock_glob.return_value = [
            "/path/to/pipelines/success.yml",
            "/path/to/pipelines/failure.yml"
        ]
        
        # Mock YAML data
        mock_read.return_value = {"description": "Test", "processors": []}
        
        # Mock mixed upload results
        mock_upload.side_effect = [200, 400]  # First succeeds, second fails
        
        # This should not raise an exception despite one failure
        self.pipeline_manager.sync_to_cluster("/path/to/pipelines")
        
        # Verify both uploads were attempted
        self.assertEqual(mock_upload.call_count, 2)

    def test_yaml_to_json_conversion(self):
        """Test YAML to JSON conversion in the sync process"""
        test_yaml_data = {
            "description": "YAML to JSON test pipeline",
            "processors": [
                {
                    "set": {
                        "field": "converted",
                        "value": True
                    }
                }
            ]
        }
        
        yaml_content = yaml.dump(test_yaml_data)
        expected_json = json.dumps(test_yaml_data)
        
        with patch("builtins.open", mock_open(read_data=yaml_content)):
            yaml_result = self.pipeline_manager._read_yaml("test.yml")
        
        json_result = json.dumps(yaml_result)
        
        # The JSON should match our expected conversion
        self.assertEqual(json_result, expected_json)
        
        # The data should be equivalent when parsed back
        self.assertEqual(json.loads(json_result), test_yaml_data)

    def test_upload_json_headers_not_explicitly_set(self):
        """Test that upload_json method doesn't explicitly set headers (relies on session)"""
        mock_response = Mock()
        mock_response.status_code = 200
        self.mock_requests.put.return_value = mock_response
        
        test_json_data = '{"test": "data"}'
        
        self.pipeline_manager._upload_json(test_json_data, "test-pipeline")
        
        # Verify the put call was made without explicit headers
        # (headers should come from the session configuration in Settings)
        self.mock_requests.put.assert_called_with(
            "https://test-opensearch:9200/_ingest/pipeline/test-pipeline",
            data=test_json_data
        )


if __name__ == '__main__':
    unittest.main()