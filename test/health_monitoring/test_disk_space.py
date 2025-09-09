"""Disk space monitoring tests"""
from teams_webhook import SeverityLevel
from .base_health_test import BaseHealthTest


class TestDiskSpaceMonitoring(BaseHealthTest):
    """Tests for disk space monitoring functionality"""
    
    def test_disk_space_normal(self):
        """Test disk space check with normal usage"""
        mock_response = self.create_mock_response([
            {"n": "node-1", "r": "dim", "dt": "1tb", "du": "700gb", "dup": "70.0%"},
            {"n": "node-2", "r": "dim", "dt": "1tb", "du": "800gb", "dup": "80.0%"},
            {"n": "node-3", "r": "dim", "dt": "1tb", "du": "850gb", "dup": "85.0%"}
        ])
        self.session.get.return_value = mock_response
        
        alerts = self.health_monitor.check_disk_space()
        
        self.assertEqual(len(alerts), 0)
        self.session.get.assert_called_once_with(
            "https://test-opensearch:9200/_cat/nodes?v&h=n,id,v,r,rp,dt,du,dup&format=json"
        )
    
    def test_disk_space_warning(self):
        """Test disk space check with warning level usage"""
        mock_response = self.create_mock_response([
            {"n": "node-1", "r": "dim", "dt": "1tb", "du": "900gb", "dup": "91.0%"},
            {"n": "node-2", "r": "dim", "dt": "1tb", "du": "920gb", "dup": "92.0%"},
            {"n": "node-3", "r": "dim", "dt": "1tb", "du": "850gb", "dup": "89.0%"}
        ])
        self.session.get.return_value = mock_response
        
        alerts = self.health_monitor.check_disk_space()
        
        self.assertEqual(len(alerts), 1)
        alert = alerts[0]
        self.assertEqual(alert.check_name, "disk_space_warning")
        self.assertEqual(alert.severity, SeverityLevel.MEDIUM)
        self.assertIn("Data nodes average disk usage is 90.7% (> 90%)", alert.message)
        
        # Check alert details - should include high usage nodes  
        high_usage_nodes = alert.details["high_usage_nodes"]
        self.assertAlmostEqual(alert.details["average_usage"], 90.7, places=1)
    
    def test_disk_space_critical(self):
        """Test disk space check with critical level usage"""
        mock_response = self.create_mock_response([
            {"n": "node-1", "r": "dim", "dt": "1tb", "du": "950gb", "dup": "95.0%"},
            {"n": "node-2", "r": "dim", "dt": "1tb", "du": "940gb", "dup": "94.0%"},
            {"n": "node-3", "r": "dim", "dt": "1tb", "du": "930gb", "dup": "93.0%"}
        ])
        self.session.get.return_value = mock_response
        
        alerts = self.health_monitor.check_disk_space()
        
        self.assertEqual(len(alerts), 1)
        alert = alerts[0]
        self.assertEqual(alert.check_name, "disk_space_critical")
        self.assertEqual(alert.severity, SeverityLevel.HIGH)
        self.assertIn("Data nodes average disk usage is 94.0% (>= 93%)", alert.message)
        
        # Check alert details
        high_usage_nodes = alert.details["high_usage_nodes"]
        self.assertEqual(len(high_usage_nodes), 3)
        self.assertAlmostEqual(alert.details["average_usage"], 94.0, places=1)