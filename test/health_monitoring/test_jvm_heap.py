"""JVM heap monitoring tests"""
from unittest.mock import Mock
from teams_webhook import SeverityLevel
from .base_health_test import BaseHealthTest


class TestJVMHeapMonitoring(BaseHealthTest):
    """Tests for JVM heap memory monitoring functionality"""
    
    def test_jvm_heap_usage_normal(self):
        """Test JVM heap usage check when all nodes are normal"""
        mock_response = self.create_mock_response([
            {"n": "node-1", "hp": "75", "hm": "8gb", "hc": "6gb"},
            {"n": "node-2", "hp": "80", "hm": "8gb", "hc": "6.4gb"},
            {"n": "node-3", "hp": "70", "hm": "8gb", "hc": "5.6gb"}
        ])
        self.session.get.return_value = mock_response
        
        alerts = self.health_monitor.check_jvm_heap_usage()
        
        self.assertEqual(len(alerts), 0)
        self.session.get.assert_called_once_with(
            "https://test-opensearch:9200/_cat/nodes?v&h=n,hp,hm,hc&format=json"
        )
    
    def test_jvm_heap_usage_warning(self):
        """Test JVM heap usage check when nodes have warning level usage (90-94%)"""
        mock_response = self.create_mock_response([
            {"n": "node-1", "hp": "91", "hm": "8gb", "hc": "7.3gb"},
            {"n": "node-2", "hp": "93", "hm": "8gb", "hc": "7.4gb"},
            {"n": "node-3", "hp": "85", "hm": "8gb", "hc": "6.8gb"}
        ])
        self.session.get.return_value = mock_response
        
        alerts = self.health_monitor.check_jvm_heap_usage()
        
        self.assertEqual(len(alerts), 1)
        alert = alerts[0]
        self.assertEqual(alert.check_name, "jvm_heap_warning")
        self.assertEqual(alert.severity, SeverityLevel.MEDIUM)
        self.assertIn("High JVM heap usage detected on 2 node(s) (>= 90%)", alert.message)
        
        # Check alert details
        self.assertEqual(len(alert.details["nodes"]), 2)
        node_names = [node["name"] for node in alert.details["nodes"]]
        self.assertIn("node-1", node_names)
        self.assertIn("node-2", node_names)
    
    def test_jvm_heap_usage_critical(self):
        """Test JVM heap usage check when nodes have critical level usage (95%+)"""
        mock_response = self.create_mock_response([
            {"n": "node-1", "hp": "96", "hm": "8gb", "hc": "7.7gb"},
            {"n": "node-2", "hp": "98", "hm": "8gb", "hc": "7.8gb"},
            {"n": "node-3", "hp": "85", "hm": "8gb", "hc": "6.8gb"}
        ])
        self.session.get.return_value = mock_response
        
        alerts = self.health_monitor.check_jvm_heap_usage()
        
        self.assertEqual(len(alerts), 1)
        alert = alerts[0]
        self.assertEqual(alert.check_name, "jvm_heap_critical")
        self.assertEqual(alert.severity, SeverityLevel.HIGH)
        self.assertIn("Critical JVM heap usage detected on 2 node(s) (>= 95%)", alert.message)
        
        # Check alert details
        self.assertEqual(len(alert.details["nodes"]), 2)
        node_names = [node["name"] for node in alert.details["nodes"]]
        self.assertIn("node-1", node_names)
        self.assertIn("node-2", node_names)
    
    def test_jvm_heap_usage_production_data(self):
        """Test JVM heap usage check with real production data (should not trigger alerts)"""
        # Using actual production data showing healthy JVM heap usage
        mock_response = self.create_mock_response([
            {"n": "opensearch-data-nodes-hot-5", "hp": "77", "hm": "4gb", "hc": "3.1gb"},
            {"n": "opensearch-data-nodes-hot-3", "hp": "72", "hm": "4gb", "hc": "2.9gb"},
            {"n": "opensearch-data-nodes-hot-4", "hp": "58", "hm": "4gb", "hc": "2.3gb"}
        ])
        self.session.get.return_value = mock_response
        
        alerts = self.health_monitor.check_jvm_heap_usage()
        
        # Should not trigger any alerts as all nodes are below 90%
        self.assertEqual(len(alerts), 0)
    
    def test_jvm_heap_usage_api_error(self):
        """Test JVM heap usage check when API call fails"""
        mock_response = Mock()
        mock_response.raise_for_status.side_effect = Exception("API connection failed")
        self.session.get.return_value = mock_response
        
        alerts = self.health_monitor.check_jvm_heap_usage()
        
        self.assertEqual(len(alerts), 1)
        alert = alerts[0]
        self.assertEqual(alert.check_name, "jvm_heap_check_error")
        self.assertEqual(alert.severity, SeverityLevel.HIGH)
        self.assertIn("Failed to check JVM heap usage: API connection failed", alert.message)