"""Cluster health monitoring tests"""
from unittest.mock import Mock
from datetime import datetime, timedelta
from teams_webhook import SeverityLevel
from .base_health_test import BaseHealthTest


class TestClusterHealthMonitoring(BaseHealthTest):
    """Tests for cluster health monitoring functionality"""
    
    def test_cluster_health_green(self):
        """Test cluster health check when status is green"""
        mock_response = self.create_mock_response({
            "status": "green",
            "cluster_name": "test-cluster",
            "active_shards": 100,
            "relocating_shards": 0,
            "initializing_shards": 0,
            "unassigned_shards": 0
        })
        self.session.get.return_value = mock_response
        
        alerts = self.health_monitor.check_cluster_health()
        
        self.assertEqual(len(alerts), 0)
        self.session.get.assert_called_once_with(
            "https://test-opensearch:9200/_cluster/health"
        )
    
    def test_cluster_health_red(self):
        """Test cluster health check when status is red"""
        mock_response = self.create_mock_response({
            "status": "red",
            "cluster_name": "test-cluster",
            "active_shards": 80,
            "relocating_shards": 0,
            "initializing_shards": 5,
            "unassigned_shards": 15
        })
        self.session.get.return_value = mock_response
        
        alerts = self.health_monitor.check_cluster_health()
        
        self.assertEqual(len(alerts), 1)
        alert = alerts[0]
        self.assertEqual(alert.check_name, "cluster_health")
        self.assertEqual(alert.severity, SeverityLevel.HIGH)
        self.assertIn("OpenSearch cluster 'test-cluster' status is RED", alert.message)
        self.assertEqual(alert.details["status"], "red")
        self.assertEqual(alert.details["unassigned_shards"], 15)
    
    def test_cluster_health_yellow_first_detection(self):
        """Test cluster health check when status is yellow (first detection - no alert)"""
        mock_response = self.create_mock_response({
            "status": "yellow",
            "cluster_name": "test-cluster",
            "active_shards": 95,
            "relocating_shards": 0,
            "initializing_shards": 0,
            "unassigned_shards": 5
        })
        self.session.get.return_value = mock_response
        
        alerts = self.health_monitor.check_cluster_health()
        
        # First detection of yellow - no alert yet
        self.assertEqual(len(alerts), 0)
        # Timer should be started
        self.assertIsNotNone(self.health_monitor._yellow_status_start_time)
    
    def test_cluster_health_yellow_sustained_15_minutes(self):
        """Test cluster health check when yellow status persists for 15+ minutes"""
        mock_response = self.create_mock_response({
            "status": "yellow",
            "cluster_name": "test-cluster",
            "active_shards": 95,
            "relocating_shards": 0,
            "initializing_shards": 0,
            "unassigned_shards": 5
        })
        self.session.get.return_value = mock_response
        
        # Set the timer to 16 minutes ago
        self.health_monitor._yellow_status_start_time = datetime.now() - timedelta(minutes=16)
        
        alerts = self.health_monitor.check_cluster_health()
        
        self.assertEqual(len(alerts), 1)
        alert = alerts[0]
        self.assertEqual(alert.check_name, "cluster_health")
        self.assertEqual(alert.severity, SeverityLevel.MEDIUM)
        self.assertIn("OpenSearch cluster 'test-cluster' status is YELLOW (sustained for 15+ minutes)", alert.message)
        self.assertEqual(alert.details["status"], "yellow")
        self.assertGreaterEqual(alert.details["duration_minutes"], 15)
    
    def test_cluster_health_yellow_not_yet_15_minutes(self):
        """Test cluster health check when yellow status persists but less than 15 minutes"""
        mock_response = self.create_mock_response({
            "status": "yellow", 
            "cluster_name": "test-cluster",
            "active_shards": 95,
            "relocating_shards": 0,
            "initializing_shards": 0,
            "unassigned_shards": 5
        })
        self.session.get.return_value = mock_response
        
        # Set the timer to 10 minutes ago (less than 15)
        self.health_monitor._yellow_status_start_time = datetime.now() - timedelta(minutes=10)
        
        alerts = self.health_monitor.check_cluster_health()
        
        # No alert yet, still waiting for 15 minutes
        self.assertEqual(len(alerts), 0)
        self.assertIsNotNone(self.health_monitor._yellow_status_start_time)
    
    def test_cluster_health_yellow_to_green_resets_timer(self):
        """Test that yellow->green transition resets the timer"""
        # First call - yellow status starts timer
        mock_response = self.create_mock_response({
            "status": "yellow",
            "cluster_name": "test-cluster"
        })
        self.session.get.return_value = mock_response
        
        alerts1 = self.health_monitor.check_cluster_health()
        self.assertEqual(len(alerts1), 0)
        self.assertIsNotNone(self.health_monitor._yellow_status_start_time)
        
        # Second call - back to green, timer should reset
        mock_response.json.return_value = {
            "status": "green",
            "cluster_name": "test-cluster"
        }
        
        alerts2 = self.health_monitor.check_cluster_health()
        self.assertEqual(len(alerts2), 0)
        self.assertIsNone(self.health_monitor._yellow_status_start_time)
    
    def test_cluster_health_yellow_to_red_resets_timer(self):
        """Test that yellow->red transition resets timer and alerts immediately"""
        # First call - yellow status starts timer
        mock_response = self.create_mock_response({
            "status": "yellow",
            "cluster_name": "test-cluster"
        })
        self.session.get.return_value = mock_response
        
        alerts1 = self.health_monitor.check_cluster_health()
        self.assertEqual(len(alerts1), 0)
        self.assertIsNotNone(self.health_monitor._yellow_status_start_time)
        
        # Second call - escalated to red, should alert immediately
        mock_response.json.return_value = {
            "status": "red",
            "cluster_name": "test-cluster",
            "active_shards": 80,
            "relocating_shards": 0,
            "initializing_shards": 0,
            "unassigned_shards": 20
        }
        
        alerts2 = self.health_monitor.check_cluster_health()
        self.assertEqual(len(alerts2), 1)
        self.assertEqual(alerts2[0].severity, SeverityLevel.HIGH)
        self.assertIn("RED", alerts2[0].message)
        self.assertIsNone(self.health_monitor._yellow_status_start_time)
    
    def test_cluster_health_error(self):
        """Test cluster health check when API call fails"""
        mock_response = Mock()
        mock_response.raise_for_status.side_effect = Exception("Connection failed")
        self.session.get.return_value = mock_response
        
        alerts = self.health_monitor.check_cluster_health()
        
        self.assertEqual(len(alerts), 1)
        alert = alerts[0]
        self.assertEqual(alert.check_name, "cluster_health_error")
        self.assertEqual(alert.severity, SeverityLevel.HIGH)
        self.assertIn("Failed to check cluster health: Connection failed", alert.message)