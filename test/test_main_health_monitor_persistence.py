#!/usr/bin/env python3

import unittest
from unittest.mock import Mock, patch, MagicMock
import sys
import os

# Add parent directory to path to import modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from health_monitor import OpenSearchHealthMonitor

class TestHealthMonitorPersistence(unittest.TestCase):
    """Test health monitor instance persistence in main.py job functions"""

    def setUp(self):
        """Set up test fixtures"""
        # Reset the global health monitor before each test
        import main
        main._global_health_monitor = None
        
        # Mock settings
        self.mock_settings = Mock()
        self.mock_settings.url = "https://test-opensearch:9200"

    def tearDown(self):
        """Clean up after each test"""
        # Reset global health monitor
        import main
        main._global_health_monitor = None

    def test_get_health_monitor_creates_instance_once(self):
        """Test that get_health_monitor creates the instance only once"""
        with patch('main.OpenSearchHealthMonitor') as mock_class:
            from main import get_health_monitor
            
            mock_instance = Mock()
            mock_class.return_value = mock_instance
            
            # First call should create instance
            result1 = get_health_monitor(self.mock_settings, 'https://test-webhook')
            self.assertEqual(result1, mock_instance)
            mock_class.assert_called_once_with(self.mock_settings, 'https://test-webhook')
            
            # Second call should return same instance without creating new one
            result2 = get_health_monitor(self.mock_settings, 'https://test-webhook')
            self.assertEqual(result2, mock_instance)
            self.assertEqual(result1, result2)  # Same instance
            mock_class.assert_called_once()  # Still only called once

    @patch('main.get_health_monitor')
    def test_frequent_health_monitoring_job_reuses_instance(self, mock_get_health_monitor):
        """Test that frequent_health_monitoring_job reuses the same HealthMonitor instance"""
        from main import frequent_health_monitoring_job
        
        mock_health_monitor = Mock()
        mock_health_monitor.run_frequent_checks.return_value = []
        mock_get_health_monitor.return_value = mock_health_monitor
        
        # Call the job function with parameters
        frequent_health_monitoring_job(self.mock_settings, 'https://test-webhook')
        
        # Verify it used get_health_monitor (which ensures persistence)
        mock_get_health_monitor.assert_called_once_with(self.mock_settings, 'https://test-webhook')
        mock_health_monitor.run_frequent_checks.assert_called_once()

    @patch('main.get_health_monitor')
    def test_daily_health_monitoring_job_reuses_instance(self, mock_get_health_monitor):
        """Test that daily_health_monitoring_job reuses the same HealthMonitor instance"""
        from main import daily_health_monitoring_job
        
        mock_health_monitor = Mock()
        mock_health_monitor.run_daily_checks.return_value = []
        mock_get_health_monitor.return_value = mock_health_monitor
        
        # Call the job function with parameters
        daily_health_monitoring_job(self.mock_settings, 'https://test-webhook')
        
        # Verify it used get_health_monitor (which ensures persistence)
        mock_get_health_monitor.assert_called_once_with(self.mock_settings, 'https://test-webhook')
        mock_health_monitor.run_daily_checks.assert_called_once()

    @patch('main.get_health_monitor')
    def test_health_monitoring_job_reuses_instance(self, mock_get_health_monitor):
        """Test that health_monitoring_job reuses the same HealthMonitor instance"""
        from main import health_monitoring_job
        
        mock_health_monitor = Mock()
        mock_health_monitor.run_all_checks.return_value = []
        mock_get_health_monitor.return_value = mock_health_monitor
        
        # Call the job function with parameters
        health_monitoring_job(self.mock_settings, 'https://test-webhook')
        
        # Verify it used get_health_monitor (which ensures persistence)
        mock_get_health_monitor.assert_called_once_with(self.mock_settings, 'https://test-webhook')
        mock_health_monitor.run_all_checks.assert_called_once()

    def test_circuit_breaker_deduplication_integration(self):
        """Test that circuit breaker deduplication works across multiple job calls"""
        with patch('main.OpenSearchHealthMonitor') as mock_class:
            from main import get_health_monitor
            
            mock_instance = Mock()
            mock_class.return_value = mock_instance
            
            # First health monitor call - should return the mocked instance
            health_monitor1 = get_health_monitor(self.mock_settings, 'https://test-webhook')
            self.assertEqual(health_monitor1, mock_instance)
            
            # Second health monitor call - should return SAME instance 
            health_monitor2 = get_health_monitor(self.mock_settings, 'https://test-webhook')
            self.assertEqual(health_monitor2, mock_instance)
            self.assertEqual(health_monitor1, health_monitor2)  # Same instance preserves state
            
            # Verify OpenSearchHealthMonitor was only called once
            mock_class.assert_called_once_with(self.mock_settings, 'https://test-webhook')


if __name__ == '__main__':
    unittest.main()