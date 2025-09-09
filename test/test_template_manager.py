#!/usr/bin/env python3

import unittest
import json
import os
import tempfile
from unittest.mock import Mock, patch, mock_open
import sys

# Add parent directory to path to import modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from template_manager import TemplateManager, TemplateType
from settings import Settings


class TestTemplateManager(unittest.TestCase):
    """Unit tests for TemplateManager class"""

    def setUp(self):
        """Set up test fixtures before each test method."""
        self.mock_settings = Mock(spec=Settings)
        self.mock_settings.url = "https://test-opensearch:9200"
        
        self.mock_requests = Mock()
        self.mock_settings.get_requests_object.return_value = self.mock_requests
        
        self.template_manager = TemplateManager(self.mock_settings)

    def test_init(self):
        """Test TemplateManager initialization"""
        self.assertEqual(self.template_manager.base_url, "https://test-opensearch:9200")
        self.assertEqual(self.template_manager.requests, self.mock_requests)

    def test_read_json_valid_file(self):
        """Test reading a valid JSON file"""
        test_json_data = {"test": "data", "nested": {"key": "value"}}
        json_content = json.dumps(test_json_data)
        
        with patch("builtins.open", mock_open(read_data=json_content)):
            result = self.template_manager._read_json("test_file.json")
        
        self.assertEqual(result, test_json_data)

    def test_read_json_invalid_file(self):
        """Test reading an invalid JSON file"""
        invalid_json = "{ invalid json"
        
        with patch("builtins.open", mock_open(read_data=invalid_json)):
            with self.assertRaises(json.JSONDecodeError):
                self.template_manager._read_json("invalid_file.json")

    def test_upload_json_index_template_success(self):
        """Test successful index template upload"""
        mock_response = Mock()
        mock_response.status_code = 200
        self.mock_requests.put.return_value = mock_response
        
        test_data = {"template": {"settings": {"number_of_shards": 1}}}
        
        result = self.template_manager._upload_json(
            test_data, "test-template", TemplateType.INDEX_TEMPLATE
        )
        
        self.assertEqual(result, 200)
        self.mock_requests.put.assert_called_with(
            "https://test-opensearch:9200/_index_template/test-template",
            json=test_data
        )

    def test_upload_json_component_template_success(self):
        """Test successful component template upload"""
        mock_response = Mock()
        mock_response.status_code = 200
        self.mock_requests.put.return_value = mock_response
        
        test_data = {"template": {"settings": {"number_of_shards": 1}}}
        version = "8.0.0"
        
        result = self.template_manager._upload_json(
            test_data, "test-component", TemplateType.COMPONENT_TEMPLATE, version
        )
        
        self.assertEqual(result, 200)
        expected_name = f"ecs_{version}_test-component"
        self.mock_requests.put.assert_called_with(
            f"https://test-opensearch:9200/_component_template/{expected_name}",
            json=test_data
        )

    def test_upload_json_component_template_no_version(self):
        """Test component template upload without version raises error"""
        test_data = {"template": {"settings": {"number_of_shards": 1}}}
        
        with self.assertRaises(ValueError) as context:
            self.template_manager._upload_json(
                test_data, "test-component", TemplateType.COMPONENT_TEMPLATE
            )
        
        self.assertEqual(str(context.exception), "Version is required for component templates")

    def test_upload_json_index_template_failure(self):
        """Test failed index template upload"""
        mock_response = Mock()
        mock_response.status_code = 400
        mock_response.text = "Bad request"
        self.mock_requests.put.return_value = mock_response
        
        test_data = {"template": {"settings": {"number_of_shards": 1}}}
        
        result = self.template_manager._upload_json(
            test_data, "test-template", TemplateType.INDEX_TEMPLATE
        )
        
        self.assertEqual(result, 400)

    @patch('glob.glob')
    @patch.object(TemplateManager, '_read_json')
    @patch.object(TemplateManager, '_upload_json')
    def test_sync_to_cluster_index_templates(self, mock_upload, mock_read, mock_glob):
        """Test syncing index templates to cluster"""
        # Mock glob to return list of JSON files
        mock_glob.return_value = [
            "/path/to/templates/template1.json",
            "/path/to/templates/template2.json"
        ]
        
        # Mock JSON data
        mock_template_data = {"template": {"settings": {"number_of_shards": 1}}}
        mock_read.return_value = mock_template_data
        
        # Mock successful upload
        mock_upload.return_value = 200
        
        self.template_manager.sync_to_cluster(
            "/path/to/templates", TemplateType.INDEX_TEMPLATE
        )
        
        # Verify glob was called with correct pattern
        mock_glob.assert_called_with("/path/to/templates/*.json")
        
        # Verify _read_json was called for each file
        self.assertEqual(mock_read.call_count, 2)
        mock_read.assert_any_call("/path/to/templates/template1.json")
        mock_read.assert_any_call("/path/to/templates/template2.json")
        
        # Verify _upload_json was called for each template
        self.assertEqual(mock_upload.call_count, 2)
        mock_upload.assert_any_call(mock_template_data, "template1", TemplateType.INDEX_TEMPLATE, None)
        mock_upload.assert_any_call(mock_template_data, "template2", TemplateType.INDEX_TEMPLATE, None)

    @patch('glob.glob')
    @patch.object(TemplateManager, '_read_json')
    @patch.object(TemplateManager, '_upload_json')
    def test_sync_to_cluster_component_templates(self, mock_upload, mock_read, mock_glob):
        """Test syncing component templates to cluster"""
        # Mock glob to return list of JSON files
        mock_glob.return_value = ["/path/to/components/component1.json"]
        
        # Mock JSON data
        mock_component_data = {"template": {"mappings": {"properties": {"field1": {"type": "text"}}}}}
        mock_read.return_value = mock_component_data
        
        # Mock successful upload
        mock_upload.return_value = 200
        
        version = "8.0.0"
        self.template_manager.sync_to_cluster(
            "/path/to/components", TemplateType.COMPONENT_TEMPLATE, version
        )
        
        # Verify _upload_json was called with version
        mock_upload.assert_called_with(
            mock_component_data, "component1", TemplateType.COMPONENT_TEMPLATE, version
        )

    @patch('glob.glob')
    def test_sync_to_cluster_no_files(self, mock_glob):
        """Test syncing when no JSON files are found"""
        # Mock glob to return empty list
        mock_glob.return_value = []
        
        # This should not raise an exception
        self.template_manager.sync_to_cluster(
            "/path/to/empty", TemplateType.INDEX_TEMPLATE
        )
        
        # Verify glob was called
        mock_glob.assert_called_with("/path/to/empty/*.json")

    def test_template_type_enum(self):
        """Test TemplateType enum values"""
        self.assertEqual(TemplateType.INDEX_TEMPLATE.value, 1)
        self.assertEqual(TemplateType.COMPONENT_TEMPLATE.value, 2)

    @patch('os.path.basename')
    @patch('glob.glob')
    @patch.object(TemplateManager, '_read_json')
    @patch.object(TemplateManager, '_upload_json')
    def test_sync_to_cluster_basename_extraction(self, mock_upload, mock_read, mock_glob, mock_basename):
        """Test that template names are correctly extracted from file paths"""
        mock_glob.return_value = ["/complex/path/to/my-template.json"]
        mock_basename.return_value = "my-template.json"
        mock_read.return_value = {"test": "data"}
        mock_upload.return_value = 200
        
        self.template_manager.sync_to_cluster(
            "/complex/path/to", TemplateType.INDEX_TEMPLATE
        )
        
        # Verify basename was called to extract filename
        mock_basename.assert_called_with("/complex/path/to/my-template.json")
        
        # Verify template name was correctly stripped of .json extension
        mock_upload.assert_called_with(
            {"test": "data"}, "my-template", TemplateType.INDEX_TEMPLATE, None
        )


if __name__ == '__main__':
    unittest.main()