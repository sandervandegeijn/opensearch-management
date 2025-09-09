"""Base test class for health monitoring tests"""
import unittest
from unittest.mock import Mock
from health_monitor import OpenSearchHealthMonitor, HealthAlert
from teams_webhook import TeamsWebhook, SeverityLevel
from settings import Settings


class BaseHealthTest(unittest.TestCase):
    """Base class for health monitoring tests with common setup"""
    
    def setUp(self):
        """Set up test fixtures"""
        # Create mock settings
        self.mock_settings = Mock(spec=Settings)
        self.mock_settings.url = "https://test-opensearch:9200"
        
        # Mock the requests session
        self.session = Mock()
        self.session.get = Mock()
        self.mock_settings.get_requests_object.return_value = self.session
        
        # Create health monitor instance
        self.health_monitor = OpenSearchHealthMonitor(self.mock_settings, "https://test-webhook-url")
    
    def create_mock_response(self, json_data):
        """Helper method to create a properly mocked response"""
        mock_response = Mock()
        mock_response.json.return_value = json_data
        mock_response.status_code = 200
        mock_response.text = '{"test": "response"}'
        mock_response.raise_for_status.return_value = None
        return mock_response