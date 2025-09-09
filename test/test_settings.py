#!/usr/bin/env python3

import unittest
from unittest.mock import Mock, patch
import requests
import sys
import os

# Add parent directory to path to import modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from settings import Settings


class TestSettings(unittest.TestCase):
    """Unit tests for Settings class"""

    def setUp(self):
        """Set up test fixtures before each test method."""
        self.url = "https://test-opensearch:9200"
        self.bucket = "test-bucket"
        self.cert_file_path = "/path/to/cert.pem"
        self.key_file_path = "/path/to/key.pem"
        self.number_of_days_on_hot_storage = 7
        self.number_of_days_total_retention = 90
        self.repository = "data"
        self.rollover_size_gb = 50
        self.rollover_age_days = 30

    def test_init_with_default_rollover_size(self):
        """Test Settings initialization with default rollover size"""
        settings = Settings(
            url=self.url,
            bucket=self.bucket,
            cert_file_path=self.cert_file_path,
            key_file_path=self.key_file_path,
            number_of_days_on_hot_storage=self.number_of_days_on_hot_storage,
            number_of_days_total_retention=self.number_of_days_total_retention,
            repository=self.repository
        )
        
        self.assertEqual(settings.url, self.url)
        self.assertEqual(settings.bucket, self.bucket)
        self.assertEqual(settings.cert_file_path, self.cert_file_path)
        self.assertEqual(settings.key_file_path, self.key_file_path)
        self.assertEqual(settings.number_of_days_on_hot_storage, self.number_of_days_on_hot_storage)
        self.assertEqual(settings.number_of_days_total_retention, self.number_of_days_total_retention)
        self.assertEqual(settings.repository, self.repository)
        self.assertEqual(settings.rollover_size_gb, 50)  # Default value
        self.assertEqual(settings.rollover_age_days, 30)  # Default value
        self.assertEqual(settings.managed_index_patterns, ("log", "alert"))  # Default value

    def test_init_with_custom_rollover_size(self):
        """Test Settings initialization with custom rollover size"""
        custom_rollover_size = 100
        
        settings = Settings(
            url=self.url,
            bucket=self.bucket,
            cert_file_path=self.cert_file_path,
            key_file_path=self.key_file_path,
            number_of_days_on_hot_storage=self.number_of_days_on_hot_storage,
            number_of_days_total_retention=self.number_of_days_total_retention,
            repository=self.repository,
            rollover_size_gb=custom_rollover_size
        )
        
        self.assertEqual(settings.rollover_size_gb, custom_rollover_size)

    def test_init_with_custom_rollover_age(self):
        """Test Settings initialization with custom rollover age"""
        custom_rollover_age = 60
        
        settings = Settings(
            url=self.url,
            bucket=self.bucket,
            cert_file_path=self.cert_file_path,
            key_file_path=self.key_file_path,
            number_of_days_on_hot_storage=self.number_of_days_on_hot_storage,
            number_of_days_total_retention=self.number_of_days_total_retention,
            repository=self.repository,
            rollover_age_days=custom_rollover_age
        )
        
        self.assertEqual(settings.rollover_age_days, custom_rollover_age)

    def test_init_with_custom_patterns(self):
        """Test Settings initialization with custom index patterns"""
        custom_patterns = ("data", "metrics", "traces")
        
        settings = Settings(
            url=self.url,
            bucket=self.bucket,
            cert_file_path=self.cert_file_path,
            key_file_path=self.key_file_path,
            number_of_days_on_hot_storage=self.number_of_days_on_hot_storage,
            number_of_days_total_retention=self.number_of_days_total_retention,
            repository=self.repository,
            rollover_size_gb=self.rollover_size_gb,
            rollover_age_days=self.rollover_age_days,
            managed_index_patterns=custom_patterns
        )
        
        self.assertEqual(settings.managed_index_patterns, custom_patterns)

    @patch('requests.Session')
    def test_get_requests_object(self, mock_session_class):
        """Test get_requests_object method"""
        mock_session = Mock()
        mock_session_class.return_value = mock_session
        
        settings = Settings(
            url=self.url,
            bucket=self.bucket,
            cert_file_path=self.cert_file_path,
            key_file_path=self.key_file_path,
            number_of_days_on_hot_storage=self.number_of_days_on_hot_storage,
            number_of_days_total_retention=self.number_of_days_total_retention,
            repository=self.repository,
            rollover_size_gb=self.rollover_size_gb,
            rollover_age_days=self.rollover_age_days
        )
        
        result = settings.get_requests_object()
        
        # Verify session was created and configured
        mock_session_class.assert_called_once()
        self.assertEqual(result, mock_session)
        
        # Verify session configuration
        self.assertEqual(mock_session.cert, (self.cert_file_path, self.key_file_path))
        self.assertEqual(mock_session.verify, False)
        self.assertEqual(mock_session.headers, {"content-type": "application/json", 'charset':'UTF-8'})

    def test_settings_immutable_after_creation(self):
        """Test that settings can be modified after creation"""
        settings = Settings(
            url=self.url,
            bucket=self.bucket,
            cert_file_path=self.cert_file_path,
            key_file_path=self.key_file_path,
            number_of_days_on_hot_storage=self.number_of_days_on_hot_storage,
            number_of_days_total_retention=self.number_of_days_total_retention,
            repository=self.repository,
            rollover_size_gb=self.rollover_size_gb,
            rollover_age_days=self.rollover_age_days
        )
        
        # Test that we can modify values (they're not frozen)
        settings.url = "https://new-url:9200"
        self.assertEqual(settings.url, "https://new-url:9200")

    def test_cert_tuple_creation(self):
        """Test that certificate tuple is created correctly"""
        settings = Settings(
            url=self.url,
            bucket=self.bucket,
            cert_file_path=self.cert_file_path,
            key_file_path=self.key_file_path,
            number_of_days_on_hot_storage=self.number_of_days_on_hot_storage,
            number_of_days_total_retention=self.number_of_days_total_retention,
            repository=self.repository,
            rollover_size_gb=self.rollover_size_gb,
            rollover_age_days=self.rollover_age_days
        )
        
        with patch('requests.Session') as mock_session_class:
            mock_session = Mock()
            mock_session_class.return_value = mock_session
            
            settings.get_requests_object()
            
            # Verify cert tuple was set correctly
            expected_cert = (self.cert_file_path, self.key_file_path)
            mock_session.cert = expected_cert
            self.assertEqual(mock_session.cert, expected_cert)


if __name__ == '__main__':
    unittest.main()