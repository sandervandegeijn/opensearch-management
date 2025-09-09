import unittest
import json
from datetime import datetime, timedelta
from unittest.mock import Mock, patch, MagicMock
from health_monitor import OpenSearchHealthMonitor, HealthAlert
from teams_webhook import TeamsWebhook, SeverityLevel
from settings import Settings


class TestHealthMonitor(unittest.TestCase):
    
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
    
    def test_cluster_health_green(self):
        """Test cluster health check when status is green"""
        # Mock successful green response
        mock_response = Mock()
        mock_response.json.return_value = {
            "status": "green",
            "cluster_name": "test-cluster",
            "active_shards": 100,
            "relocating_shards": 0,
            "initializing_shards": 0,
            "unassigned_shards": 0
        }
        mock_response.status_code = 200
        mock_response.text = '{"test": "response"}'
        mock_response.raise_for_status.return_value = None
        self.mock_settings.get_requests_object().get.return_value = mock_response
        
        alerts = self.health_monitor.check_cluster_health()
        
        self.assertEqual(len(alerts), 0)
        self.mock_settings.get_requests_object().get.assert_called_once_with(
            "https://test-opensearch:9200/_cluster/health"
        )
    
    def test_cluster_health_yellow_first_detection(self):
        """Test cluster health check when status is yellow (first detection - no alert)"""
        mock_response = Mock()
        mock_response.json.return_value = {
            "status": "yellow",
            "cluster_name": "test-cluster",
            "active_shards": 90,
            "relocating_shards": 0,
            "initializing_shards": 5,
            "unassigned_shards": 10
        }
        mock_response.status_code = 200
        mock_response.text = '{"test": "response"}'
        mock_response.raise_for_status.return_value = None
        self.mock_settings.get_requests_object().get.return_value = mock_response
        
        alerts = self.health_monitor.check_cluster_health()
        
        # Should not alert on first yellow detection
        self.assertEqual(len(alerts), 0)
        # Timer should be set
        self.assertIsNotNone(self.health_monitor._yellow_status_start_time)
    
    @patch('health_monitor.datetime')
    def test_cluster_health_yellow_sustained_15_minutes(self, mock_datetime):
        """Test cluster health check when yellow status persists for 15+ minutes"""
        # Setup mock datetime
        start_time = datetime(2024, 1, 1, 12, 0, 0)
        alert_time = start_time + timedelta(minutes=15)
        mock_datetime.now.return_value = alert_time
        mock_datetime.side_effect = lambda *args, **kw: datetime(*args, **kw)
        
        # Set the yellow status start time to 15 minutes ago
        self.health_monitor._yellow_status_start_time = start_time
        
        mock_response = Mock()
        mock_response.json.return_value = {
            "status": "yellow",
            "cluster_name": "test-cluster",
            "active_shards": 90,
            "relocating_shards": 0,
            "initializing_shards": 5,
            "unassigned_shards": 10
        }
        mock_response.status_code = 200
        mock_response.text = '{"test": "response"}'
        mock_response.raise_for_status.return_value = None
        self.mock_settings.get_requests_object().get.return_value = mock_response
        
        alerts = self.health_monitor.check_cluster_health()
        
        # Should alert after 15 minutes
        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts[0].severity, SeverityLevel.MEDIUM)
        self.assertIn("YELLOW", alerts[0].message)
        self.assertIn("sustained for 15+ minutes", alerts[0].message)
        self.assertIn("test-cluster", alerts[0].message)
        self.assertEqual(alerts[0].details["duration_minutes"], 15)

    @patch('health_monitor.datetime')
    def test_cluster_health_yellow_not_yet_15_minutes(self, mock_datetime):
        """Test cluster health check when yellow status persists but less than 15 minutes"""
        # Setup mock datetime
        start_time = datetime(2024, 1, 1, 12, 0, 0)
        current_time = start_time + timedelta(minutes=10)  # Only 10 minutes
        mock_datetime.now.return_value = current_time
        mock_datetime.side_effect = lambda *args, **kw: datetime(*args, **kw)
        
        # Set the yellow status start time to 10 minutes ago
        self.health_monitor._yellow_status_start_time = start_time
        
        mock_response = Mock()
        mock_response.json.return_value = {
            "status": "yellow",
            "cluster_name": "test-cluster",
            "active_shards": 90,
            "relocating_shards": 0,
            "initializing_shards": 5,
            "unassigned_shards": 10
        }
        mock_response.status_code = 200
        mock_response.text = '{"test": "response"}'
        mock_response.raise_for_status.return_value = None
        self.mock_settings.get_requests_object().get.return_value = mock_response
        
        alerts = self.health_monitor.check_cluster_health()
        
        # Should not alert before 15 minutes
        self.assertEqual(len(alerts), 0)
        # Timer should still be set to original time
        self.assertEqual(self.health_monitor._yellow_status_start_time, start_time)

    def test_cluster_health_yellow_to_green_resets_timer(self):
        """Test that yellow->green transition resets the timer"""
        # First set yellow status to start timer
        mock_response = Mock()
        mock_response.json.return_value = {
            "status": "yellow",
            "cluster_name": "test-cluster",
            "active_shards": 90,
            "relocating_shards": 0,
            "initializing_shards": 5,
            "unassigned_shards": 10
        }
        mock_response.status_code = 200
        mock_response.text = '{"test": "response"}'
        mock_response.raise_for_status.return_value = None
        self.mock_settings.get_requests_object().get.return_value = mock_response
        
        self.health_monitor.check_cluster_health()
        self.assertIsNotNone(self.health_monitor._yellow_status_start_time)
        
        # Now change to green
        mock_response.json.return_value = {
            "status": "green",
            "cluster_name": "test-cluster",
            "active_shards": 100,
            "relocating_shards": 0,
            "initializing_shards": 0,
            "unassigned_shards": 0
        }
        
        alerts = self.health_monitor.check_cluster_health()
        
        # Should have no alerts and timer should be reset
        self.assertEqual(len(alerts), 0)
        self.assertIsNone(self.health_monitor._yellow_status_start_time)

    def test_cluster_health_yellow_to_red_resets_timer(self):
        """Test that yellow->red transition resets timer and alerts immediately"""
        # First set yellow status to start timer
        mock_response = Mock()
        mock_response.json.return_value = {
            "status": "yellow",
            "cluster_name": "test-cluster",
            "active_shards": 90,
            "relocating_shards": 0,
            "initializing_shards": 5,
            "unassigned_shards": 10
        }
        mock_response.status_code = 200
        mock_response.text = '{"test": "response"}'
        mock_response.raise_for_status.return_value = None
        self.mock_settings.get_requests_object().get.return_value = mock_response
        
        self.health_monitor.check_cluster_health()
        self.assertIsNotNone(self.health_monitor._yellow_status_start_time)
        
        # Now change to red
        mock_response.json.return_value = {
            "status": "red",
            "cluster_name": "test-cluster",
            "active_shards": 50,
            "relocating_shards": 0,
            "initializing_shards": 0,
            "unassigned_shards": 50
        }
        
        alerts = self.health_monitor.check_cluster_health()
        
        # Should alert immediately for red and reset timer
        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts[0].severity, SeverityLevel.HIGH)
        self.assertIn("RED", alerts[0].message)
        self.assertIsNone(self.health_monitor._yellow_status_start_time)
    
    def test_cluster_health_red(self):
        """Test cluster health check when status is red"""
        mock_response = Mock()
        mock_response.json.return_value = {
            "status": "red",
            "cluster_name": "test-cluster",
            "active_shards": 50,
            "relocating_shards": 0,
            "initializing_shards": 0,
            "unassigned_shards": 50
        }
        mock_response.status_code = 200
        mock_response.text = '{"test": "response"}'
        mock_response.raise_for_status.return_value = None
        self.mock_settings.get_requests_object().get.return_value = mock_response
        
        alerts = self.health_monitor.check_cluster_health()
        
        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts[0].severity, SeverityLevel.HIGH)
        self.assertIn("RED", alerts[0].message)
    
    def test_cluster_health_error(self):
        """Test cluster health check when API call fails"""
        self.mock_settings.get_requests_object().get.side_effect = Exception("Connection failed")
        
        alerts = self.health_monitor.check_cluster_health()
        
        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts[0].severity, SeverityLevel.HIGH)
        self.assertIn("Failed to check cluster health", alerts[0].message)
    
    def test_disk_space_normal(self):
        """Test disk space check with normal usage"""
        mock_response = Mock()
        mock_response.json.return_value = [
            {"n": "data-node-1", "r": "d", "dup": "75.5%"},
            {"n": "data-node-2", "r": "d", "dup": "80.2%"},
            {"n": "master-node", "r": "master", "dup": "45.0%"}
        ]
        mock_response.status_code = 200
        mock_response.text = '{"test": "response"}'
        mock_response.raise_for_status.return_value = None
        self.mock_settings.get_requests_object().get.return_value = mock_response
        
        alerts = self.health_monitor.check_disk_space()
        
        self.assertEqual(len(alerts), 0)
    
    def test_disk_space_warning(self):
        """Test disk space check with warning level usage"""
        mock_response = Mock()
        mock_response.json.return_value = [
            {"n": "data-node-1", "r": "d", "dup": "91.5%"},
            {"n": "data-node-2", "r": "d", "dup": "89.2%"}
        ]
        mock_response.status_code = 200
        mock_response.text = '{"test": "response"}'
        mock_response.raise_for_status.return_value = None
        self.mock_settings.get_requests_object().get.return_value = mock_response
        
        alerts = self.health_monitor.check_disk_space()
        
        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts[0].severity, SeverityLevel.MEDIUM)
        self.assertIn("90.3%", alerts[0].message)  # Average of 91.5 and 89.2
    
    def test_disk_space_critical(self):
        """Test disk space check with critical level usage"""
        mock_response = Mock()
        mock_response.json.return_value = [
            {"n": "data-node-1", "r": "d", "dup": "95.0%"},
            {"n": "data-node-2", "r": "d", "dup": "94.0%"}
        ]
        mock_response.status_code = 200
        mock_response.text = '{"test": "response"}'
        mock_response.raise_for_status.return_value = None
        self.mock_settings.get_requests_object().get.return_value = mock_response
        
        alerts = self.health_monitor.check_disk_space()
        
        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts[0].severity, SeverityLevel.HIGH)
        self.assertIn("94.5%", alerts[0].message)  # Average of 95.0 and 94.0
    
    def test_data_snapshots_success(self):
        """Test data snapshots check with all successful snapshots"""
        mock_response = Mock()
        mock_response.json.return_value = [
            {"id": "snapshot-1", "status": "SUCCESS"},
            {"id": "snapshot-2", "status": "SUCCESS"}
        ]
        mock_response.status_code = 200
        mock_response.text = '{"test": "response"}'
        mock_response.raise_for_status.return_value = None
        self.mock_settings.get_requests_object().get.return_value = mock_response
        
        alerts = self.health_monitor.check_data_snapshots()
        
        self.assertEqual(len(alerts), 0)
        self.mock_settings.get_requests_object().get.assert_called_once_with(
            "https://test-opensearch:9200/_cat/snapshots/data?v&s=endEpoch&format=json"
        )
    
    def test_data_snapshots_failed(self):
        """Test data snapshots check with failed snapshots"""
        mock_response = Mock()
        mock_response.json.return_value = [
            {"id": "snapshot-1", "status": "SUCCESS"},
            {"id": "snapshot-2", "status": "FAILED", "startEpoch": "1234567890", "endEpoch": "1234567900"},
            {"id": "snapshot-3", "status": "FAILED", "startEpoch": "1234567800", "endEpoch": "1234567850"}
        ]
        mock_response.status_code = 200
        mock_response.text = '{"test": "response"}'
        mock_response.raise_for_status.return_value = None
        self.mock_settings.get_requests_object().get.return_value = mock_response
        
        alerts = self.health_monitor.check_data_snapshots()
        
        self.assertEqual(len(alerts), 2)
        self.assertTrue(all(alert.severity == SeverityLevel.HIGH for alert in alerts))
        self.assertIn("snapshot-2", alerts[0].message)
        self.assertIn("snapshot-3", alerts[1].message)
        self.assertTrue(all("FAILED" in alert.message for alert in alerts))
    
    def test_data_snapshots_partial(self):
        """Test data snapshots check with partial snapshots"""
        mock_response = Mock()
        mock_response.json.return_value = [
            {"id": "snapshot-1", "status": "SUCCESS"},
            {"id": "snapshot-2", "status": "PARTIAL", "startEpoch": "1234567890", "endEpoch": "1234567900"},
            {"id": "snapshot-3", "status": "PARTIAL", "startEpoch": "1234567800", "endEpoch": "1234567850"}
        ]
        mock_response.status_code = 200
        mock_response.text = '{"test": "response"}'
        mock_response.raise_for_status.return_value = None
        self.mock_settings.get_requests_object().get.return_value = mock_response
        
        alerts = self.health_monitor.check_data_snapshots()
        
        self.assertEqual(len(alerts), 2)
        self.assertTrue(all(alert.severity == SeverityLevel.MEDIUM for alert in alerts))
        self.assertIn("snapshot-2", alerts[0].message)
        self.assertIn("snapshot-3", alerts[1].message)
        self.assertTrue(all("PARTIAL" in alert.message for alert in alerts))
    
    def test_data_snapshots_mixed_statuses(self):
        """Test data snapshots check with mixed failed and partial snapshots"""
        mock_response = Mock()
        mock_response.json.return_value = [
            {"id": "snapshot-1", "status": "SUCCESS"},
            {"id": "snapshot-2", "status": "FAILED", "startEpoch": "1234567890", "endEpoch": "1234567900"},
            {"id": "snapshot-3", "status": "PARTIAL", "startEpoch": "1234567800", "endEpoch": "1234567850"}
        ]
        mock_response.status_code = 200
        mock_response.text = '{"test": "response"}'
        mock_response.raise_for_status.return_value = None
        self.mock_settings.get_requests_object().get.return_value = mock_response
        
        alerts = self.health_monitor.check_data_snapshots()
        
        self.assertEqual(len(alerts), 2)
        # Check that we have one HIGH and one MEDIUM severity alert
        severities = [alert.severity for alert in alerts]
        self.assertIn(SeverityLevel.HIGH, severities)
        self.assertIn(SeverityLevel.MEDIUM, severities)
        self.assertIn("snapshot-2", alerts[0].message)
        self.assertIn("snapshot-3", alerts[1].message)
        self.assertIn("FAILED", alerts[0].message)
        self.assertIn("PARTIAL", alerts[1].message)
    
    def test_disaster_recovery_snapshots_failed(self):
        """Test DR snapshots check with failed snapshots"""
        mock_response = Mock()
        mock_response.json.return_value = [
            {"id": "dr-snapshot-1", "status": "FAILED"}
        ]
        mock_response.status_code = 200
        mock_response.text = '{"test": "response"}'
        mock_response.raise_for_status.return_value = None
        self.mock_settings.get_requests_object().get.return_value = mock_response
        
        alerts = self.health_monitor.check_disaster_recovery_snapshots()
        
        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts[0].severity, SeverityLevel.HIGH)
        self.assertIn("dr-snapshot-1", alerts[0].message)
        self.assertIn("disaster-recovery", alerts[0].message)
        self.mock_settings.get_requests_object().get.assert_called_once_with(
            "https://test-opensearch:9200/_cat/snapshots/disaster-recovery?v&s=endEpoch&format=json"
        )
    
    def test_disaster_recovery_snapshots_partial(self):
        """Test DR snapshots check with partial snapshots"""
        mock_response = Mock()
        mock_response.json.return_value = [
            {"id": "dr-snapshot-1", "status": "PARTIAL", "startEpoch": "1234567890", "endEpoch": "1234567900"}
        ]
        mock_response.status_code = 200
        mock_response.text = '{"test": "response"}'
        mock_response.raise_for_status.return_value = None
        self.mock_settings.get_requests_object().get.return_value = mock_response
        
        alerts = self.health_monitor.check_disaster_recovery_snapshots()
        
        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts[0].severity, SeverityLevel.MEDIUM)
        self.assertIn("dr-snapshot-1", alerts[0].message)
        self.assertIn("PARTIAL", alerts[0].message)
        self.assertIn("disaster-recovery", alerts[0].message)
    
    def test_disaster_recovery_snapshots_mixed_statuses(self):
        """Test DR snapshots check with mixed failed and partial snapshots"""
        mock_response = Mock()
        mock_response.json.return_value = [
            {"id": "dr-snapshot-1", "status": "SUCCESS"},
            {"id": "dr-snapshot-2", "status": "FAILED", "startEpoch": "1234567890", "endEpoch": "1234567900"},
            {"id": "dr-snapshot-3", "status": "PARTIAL", "startEpoch": "1234567800", "endEpoch": "1234567850"}
        ]
        mock_response.status_code = 200
        mock_response.text = '{"test": "response"}'
        mock_response.raise_for_status.return_value = None
        self.mock_settings.get_requests_object().get.return_value = mock_response
        
        alerts = self.health_monitor.check_disaster_recovery_snapshots()
        
        self.assertEqual(len(alerts), 2)
        # Check that we have one HIGH and one MEDIUM severity alert
        severities = [alert.severity for alert in alerts]
        self.assertIn(SeverityLevel.HIGH, severities)
        self.assertIn(SeverityLevel.MEDIUM, severities)
        # Find the failed and partial alerts
        failed_alert = next(alert for alert in alerts if "FAILED" in alert.message)
        partial_alert = next(alert for alert in alerts if "PARTIAL" in alert.message)
        self.assertIn("dr-snapshot-2", failed_alert.message)
        self.assertIn("dr-snapshot-3", partial_alert.message)
    
    @patch('health_monitor.OpenSearchHealthMonitor.check_cluster_health')
    @patch('health_monitor.OpenSearchHealthMonitor.check_disk_space')
    @patch('health_monitor.OpenSearchHealthMonitor.check_jvm_heap_usage')
    @patch('health_monitor.OpenSearchHealthMonitor.check_circuit_breakers')
    @patch('health_monitor.OpenSearchHealthMonitor.check_thread_pool_queues')
    @patch('health_monitor.OpenSearchHealthMonitor.check_data_snapshots')
    @patch('health_monitor.OpenSearchHealthMonitor.check_disaster_recovery_snapshots')
    def test_run_all_checks_with_alerts(self, mock_dr_snapshots, mock_data_snapshots,
                                       mock_thread_pool, mock_circuit_breakers,
                                       mock_jvm_heap, mock_disk_space, mock_cluster_health):
        """Test run_all_checks method with various alerts"""
        
        # Setup mock returns
        mock_cluster_health.return_value = [
            HealthAlert("cluster_health", SeverityLevel.HIGH, "Cluster is RED")
        ]
        mock_disk_space.return_value = [
            HealthAlert("disk_space", SeverityLevel.MEDIUM, "Disk usage high")
        ]
        mock_jvm_heap.return_value = []
        mock_circuit_breakers.return_value = []
        mock_thread_pool.return_value = []
        mock_data_snapshots.return_value = []
        mock_dr_snapshots.return_value = [
            HealthAlert("dr_snapshots", SeverityLevel.HIGH, "DR snapshot failed")
        ]
        
        # Mock webhook
        self.health_monitor.webhook = Mock(spec=TeamsWebhook)
        
        alerts = self.health_monitor.run_all_checks()
        
        self.assertEqual(len(alerts), 3)
        self.assertEqual(sum(1 for alert in alerts if alert.severity == SeverityLevel.HIGH), 2)
        self.assertEqual(sum(1 for alert in alerts if alert.severity == SeverityLevel.MEDIUM), 1)
        
        # Verify all check methods were called
        mock_cluster_health.assert_called_once()
        mock_disk_space.assert_called_once()
        mock_jvm_heap.assert_called_once()
        mock_circuit_breakers.assert_called_once()
        mock_thread_pool.assert_called_once()
        mock_data_snapshots.assert_called_once()
        mock_dr_snapshots.assert_called_once()
    
    def test_run_all_checks_exception_handling(self):
        """Test run_all_checks handles exceptions gracefully"""
        # Force an exception in one of the checks
        with patch.object(self.health_monitor, 'check_cluster_health', side_effect=Exception("Test error")):
            alerts = self.health_monitor.run_all_checks()
            
            # Should still return alerts for the error
            self.assertGreaterEqual(len(alerts), 1)
            error_alerts = [alert for alert in alerts if "health_monitor_error" in alert.check_name]
            self.assertEqual(len(error_alerts), 1)
            self.assertEqual(error_alerts[0].severity, SeverityLevel.HIGH)
    
    def test_test_all_checks(self):
        """Test the test_all_checks method"""
        # Mock successful responses for all checks
        with patch.object(self.health_monitor, 'check_cluster_health', return_value=[]), \
             patch.object(self.health_monitor, 'check_disk_space', return_value=[]), \
             patch.object(self.health_monitor, 'check_data_snapshots', return_value=[]), \
             patch.object(self.health_monitor, 'check_disaster_recovery_snapshots', return_value=[]):
            
            # Mock webhook test
            self.health_monitor.webhook = Mock(spec=TeamsWebhook)
            self.health_monitor.webhook.test_connection.return_value = True
            
            results = self.health_monitor.test_all_checks()
            
            self.assertTrue(results['cluster_health'])
            self.assertTrue(results['disk_space'])
            self.assertTrue(results['data_snapshots'])
            self.assertTrue(results['dr_snapshots'])
            self.assertTrue(results['webhook_test'])
            self.assertEqual(len(results['errors']), 0)

    @patch('health_monitor.OpenSearchHealthMonitor.check_cluster_health')
    @patch('health_monitor.OpenSearchHealthMonitor.check_disk_space')
    @patch('health_monitor.OpenSearchHealthMonitor.check_jvm_heap_usage')
    @patch('health_monitor.OpenSearchHealthMonitor.check_circuit_breakers')
    @patch('health_monitor.OpenSearchHealthMonitor.check_thread_pool_queues')
    def test_run_frequent_checks_with_alerts(self, mock_thread_pool, mock_circuit_breakers,
                                           mock_jvm_heap, mock_disk_space, mock_cluster_health):
        """Test run_frequent_checks method with cluster and disk alerts only"""
        
        # Setup mock returns
        mock_cluster_health.return_value = [
            HealthAlert("cluster_health", SeverityLevel.HIGH, "Cluster is RED")
        ]
        mock_disk_space.return_value = [
            HealthAlert("disk_space", SeverityLevel.MEDIUM, "Disk usage high")
        ]
        mock_jvm_heap.return_value = []
        mock_circuit_breakers.return_value = []
        mock_thread_pool.return_value = []
        
        # Mock webhook
        self.health_monitor.webhook = Mock(spec=TeamsWebhook)
        
        alerts = self.health_monitor.run_frequent_checks()
        
        self.assertEqual(len(alerts), 2)
        self.assertEqual(sum(1 for alert in alerts if alert.severity == SeverityLevel.HIGH), 1)
        self.assertEqual(sum(1 for alert in alerts if alert.severity == SeverityLevel.MEDIUM), 1)
        
        # Verify frequent check methods were called (cluster health, disk space, JVM heap, circuit breakers, thread pools)
        mock_cluster_health.assert_called_once()
        mock_disk_space.assert_called_once()
        mock_jvm_heap.assert_called_once()
        mock_circuit_breakers.assert_called_once()
        mock_thread_pool.assert_called_once()

    @patch('health_monitor.OpenSearchHealthMonitor.check_cluster_health')
    @patch('health_monitor.OpenSearchHealthMonitor.check_disk_space')
    @patch('health_monitor.OpenSearchHealthMonitor.check_jvm_heap_usage')
    @patch('health_monitor.OpenSearchHealthMonitor.check_circuit_breakers')
    @patch('health_monitor.OpenSearchHealthMonitor.check_thread_pool_queues')
    def test_run_frequent_checks_no_alerts(self, mock_thread_pool, mock_circuit_breakers,
                                          mock_jvm_heap, mock_disk_space, mock_cluster_health):
        """Test run_frequent_checks method with no alerts"""
        
        # Setup mock returns with no alerts
        mock_cluster_health.return_value = []
        mock_disk_space.return_value = []
        mock_jvm_heap.return_value = []
        mock_circuit_breakers.return_value = []
        mock_thread_pool.return_value = []
        
        alerts = self.health_monitor.run_frequent_checks()
        
        self.assertEqual(len(alerts), 0)
        
        # Verify frequent check methods were called (cluster health, disk space, JVM heap, circuit breakers, thread pools)
        mock_cluster_health.assert_called_once()
        mock_disk_space.assert_called_once()
        mock_jvm_heap.assert_called_once()
        mock_circuit_breakers.assert_called_once()
        mock_thread_pool.assert_called_once()

    def test_run_frequent_checks_exception_handling(self):
        """Test run_frequent_checks handles exceptions gracefully"""
        # Force an exception in one of the checks
        with patch.object(self.health_monitor, 'check_cluster_health', side_effect=Exception("Test error")):
            alerts = self.health_monitor.run_frequent_checks()
            
            # Should still return alerts for the error
            self.assertGreaterEqual(len(alerts), 1)
            error_alerts = [alert for alert in alerts if "health_monitor_error" in alert.check_name]
            self.assertEqual(len(error_alerts), 1)
            self.assertEqual(error_alerts[0].severity, SeverityLevel.HIGH)
            self.assertIn("Frequent health monitoring system failed", error_alerts[0].message)

    @patch('health_monitor.OpenSearchHealthMonitor.check_data_snapshots')
    @patch('health_monitor.OpenSearchHealthMonitor.check_disaster_recovery_snapshots')
    def test_run_daily_checks_with_alerts(self, mock_dr_snapshots, mock_data_snapshots):
        """Test run_daily_checks method with data and DR snapshot alerts"""
        
        # Setup mock returns with failed snapshots
        mock_data_snapshots.return_value = [
            HealthAlert("data_snapshots", SeverityLevel.HIGH, "Data snapshot failed")
        ]
        mock_dr_snapshots.return_value = [
            HealthAlert("dr_snapshots", SeverityLevel.MEDIUM, "DR snapshot partial")
        ]
        
        # Mock webhook
        self.health_monitor.webhook = Mock(spec=TeamsWebhook)
        
        alerts = self.health_monitor.run_daily_checks()
        
        self.assertEqual(len(alerts), 2)
        self.assertEqual(sum(1 for alert in alerts if alert.severity == SeverityLevel.HIGH), 1)
        self.assertEqual(sum(1 for alert in alerts if alert.severity == SeverityLevel.MEDIUM), 1)
        
        # Verify both data and DR snapshots checks were called
        mock_data_snapshots.assert_called_once()
        mock_dr_snapshots.assert_called_once()

    @patch('health_monitor.OpenSearchHealthMonitor.check_data_snapshots')
    @patch('health_monitor.OpenSearchHealthMonitor.check_disaster_recovery_snapshots')
    def test_run_daily_checks_no_alerts(self, mock_dr_snapshots, mock_data_snapshots):
        """Test run_daily_checks method with no alerts"""
        
        # Setup mock returns with no alerts
        mock_data_snapshots.return_value = []
        mock_dr_snapshots.return_value = []
        
        alerts = self.health_monitor.run_daily_checks()
        
        self.assertEqual(len(alerts), 0)
        
        # Verify both data and DR snapshots checks were called
        mock_data_snapshots.assert_called_once()
        mock_dr_snapshots.assert_called_once()

    def test_run_daily_checks_exception_handling(self):
        """Test run_daily_checks handles exceptions gracefully"""
        # Force an exception in the check
        with patch.object(self.health_monitor, 'check_data_snapshots', side_effect=Exception("Test error")):
            alerts = self.health_monitor.run_daily_checks()
            
            # Should still return alerts for the error
            self.assertGreaterEqual(len(alerts), 1)
            error_alerts = [alert for alert in alerts if "health_monitor_error" in alert.check_name]
            self.assertEqual(len(error_alerts), 1)
            self.assertEqual(error_alerts[0].severity, SeverityLevel.HIGH)
            self.assertIn("Daily health monitoring system failed", error_alerts[0].message)
    
    def test_jvm_heap_usage_normal(self):
        """Test JVM heap usage check when all nodes are normal"""
        mock_response = Mock()
        mock_response.json.return_value = [
            {"n": "node-1", "hp": "75", "hm": "8gb", "hc": "6gb"},
            {"n": "node-2", "hp": "80", "hm": "8gb", "hc": "6.4gb"},
            {"n": "node-3", "hp": "65", "hm": "8gb", "hc": "5.2gb"}
        ]
        mock_response.status_code = 200
        mock_response.text = '{"test": "response"}'
        mock_response.raise_for_status.return_value = None
        self.mock_settings.get_requests_object().get.return_value = mock_response
        
        alerts = self.health_monitor.check_jvm_heap_usage()
        
        self.assertEqual(len(alerts), 0)
        self.mock_settings.get_requests_object().get.assert_called_once_with(
            "https://test-opensearch:9200/_cat/nodes?v&h=n,hp,hm,hc&format=json"
        )
    
    def test_jvm_heap_usage_production_data(self):
        """Test JVM heap usage check with real production data (should not trigger alerts)"""
        mock_response = Mock()
        # Using actual production data from user
        mock_response.json.return_value = [
            {"n": "opensearch-master-nodes-0", "hp": "33", "hm": "6gb", "hc": "2gb"},
            {"n": "opensearch-data-nodes-hot-5", "hp": "70", "hm": "4.3gb", "hc": "3gb"},
            {"n": "opensearch-master-nodes-2", "hp": "20", "hm": "6gb", "hc": "1.2gb"},
            {"n": "opensearch-search-nodes-1", "hp": "28", "hm": "6gb", "hc": "1.7gb"},
            {"n": "opensearch-data-nodes-hot-6", "hp": "72", "hm": "4.3gb", "hc": "3.1gb"},
            {"n": "opensearch-data-nodes-hot-4", "hp": "68", "hm": "4.3gb", "hc": "2.9gb"},
            {"n": "opensearch-ingest-nodes-0", "hp": "58", "hm": "1.2gb", "hc": "730.5mb"},
            {"n": "opensearch-data-nodes-hot-3", "hp": "37", "hm": "4.3gb", "hc": "1.6gb"},
            {"n": "opensearch-data-nodes-hot-7", "hp": "77", "hm": "4.3gb", "hc": "3.3gb"},  # Highest at 77%
            {"n": "opensearch-data-nodes-hot-2", "hp": "55", "hm": "4.3gb", "hc": "2.3gb"},
            {"n": "opensearch-data-nodes-hot-1", "hp": "33", "hm": "4.3gb", "hc": "1.4gb"},
            {"n": "opensearch-data-nodes-hot-0", "hp": "57", "hm": "4.3gb", "hc": "2.4gb"},
            {"n": "opensearch-master-nodes-1", "hp": "21", "hm": "6gb", "hc": "1.2gb"},
            {"n": "opensearch-search-nodes-0", "hp": "37", "hm": "6gb", "hc": "2.2gb"},
            {"n": "opensearch-ingest-nodes-1", "hp": "38", "hm": "1.2gb", "hc": "488.6mb"}
        ]
        mock_response.status_code = 200
        mock_response.text = '{"test": "response"}'
        mock_response.raise_for_status.return_value = None
        self.mock_settings.get_requests_object().get.return_value = mock_response
        
        alerts = self.health_monitor.check_jvm_heap_usage()
        
        # Should have no alerts since highest is 77% (below 90% warning threshold)
        self.assertEqual(len(alerts), 0)
        self.mock_settings.get_requests_object().get.assert_called_once_with(
            "https://test-opensearch:9200/_cat/nodes?v&h=n,hp,hm,hc&format=json"
        )
    
    def test_jvm_heap_usage_warning(self):
        """Test JVM heap usage check when nodes have warning level usage (90-94%)"""
        mock_response = Mock()
        mock_response.json.return_value = [
            {"n": "node-1", "hp": "92", "hm": "8gb", "hc": "7.4gb"},
            {"n": "node-2", "hp": "75", "hm": "8gb", "hc": "6gb"},
            {"n": "node-3", "hp": "91", "hm": "8gb", "hc": "7.3gb"}
        ]
        mock_response.status_code = 200
        mock_response.text = '{"test": "response"}'
        mock_response.raise_for_status.return_value = None
        self.mock_settings.get_requests_object().get.return_value = mock_response
        
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
        self.assertIn("node-3", node_names)
    
    def test_jvm_heap_usage_critical(self):
        """Test JVM heap usage check when nodes have critical level usage (95%+)"""
        mock_response = Mock()
        mock_response.json.return_value = [
            {"n": "node-1", "hp": "96", "hm": "8gb", "hc": "7.7gb"},
            {"n": "node-2", "hp": "98", "hm": "8gb", "hc": "7.8gb"},
            {"n": "node-3", "hp": "85", "hm": "8gb", "hc": "6.8gb"}
        ]
        mock_response.status_code = 200
        mock_response.text = '{"test": "response"}'
        mock_response.raise_for_status.return_value = None
        self.mock_settings.get_requests_object().get.return_value = mock_response
        
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
    
    def test_jvm_heap_usage_api_error(self):
        """Test JVM heap usage check when API call fails"""
        mock_response = Mock()
        mock_response.raise_for_status.side_effect = Exception("API connection failed")
        self.mock_settings.get_requests_object().get.return_value = mock_response
        
        alerts = self.health_monitor.check_jvm_heap_usage()
        
        self.assertEqual(len(alerts), 1)
        alert = alerts[0]
        self.assertEqual(alert.check_name, "jvm_heap_check_error")
        self.assertEqual(alert.severity, SeverityLevel.HIGH)
        self.assertIn("Failed to check JVM heap usage: API connection failed", alert.message)
    
    def test_circuit_breaker_normal(self):
        """Test circuit breaker check when all breakers are normal"""
        mock_response = Mock()
        # Normal circuit breaker status with no trips
        mock_response.json.return_value = {
            "nodes": {
                "node1": {
                    "name": "test-node-1",
                    "breakers": {
                        "parent": {"tripped": 0, "limit_size_in_bytes": 1000000, "estimated_size_in_bytes": 500000, "limit_size": "1mb", "estimated_size": "500kb"},
                        "request": {"tripped": 0, "limit_size_in_bytes": 600000, "estimated_size_in_bytes": 100000, "limit_size": "600kb", "estimated_size": "100kb"},
                        "fielddata": {"tripped": 0, "limit_size_in_bytes": 400000, "estimated_size_in_bytes": 50000, "limit_size": "400kb", "estimated_size": "50kb"}
                    }
                }
            }
        }
        mock_response.status_code = 200
        mock_response.text = '{"nodes": {"node1": {"name": "test-node-1"}}}'
        mock_response.status_code = 200
        mock_response.text = '{"test": "response"}'
        mock_response.raise_for_status.return_value = None
        self.session.get.return_value = mock_response
        
        alerts = self.health_monitor.check_circuit_breakers()
        
        self.assertEqual(len(alerts), 0)
        self.session.get.assert_called_once_with(
            "https://test-opensearch:9200/_nodes/stats/breaker"
        )
    
    def test_circuit_breaker_production_data(self):
        """Test circuit breaker check with real production data (should trigger critical alerts)"""
        mock_response = Mock()
        # Using actual production data showing trips - simplified sample of your full data
        mock_response.json.return_value = {
            "nodes": {
                "_vxbOtloQmapzz0DbXBsjA": {
                    "name": "opensearch-data-nodes-hot-5",
                    "breakers": {
                        "request": {
                            "limit_size_in_bytes": 2778306969,
                            "limit_size": "2.5gb",
                            "estimated_size_in_bytes": 0,
                            "estimated_size": "0b",
                            "overhead": 1,
                            "tripped": 0
                        },
                        "fielddata": {
                            "limit_size_in_bytes": 1852204646,
                            "limit_size": "1.7gb",
                            "estimated_size_in_bytes": 1520,
                            "estimated_size": "1.4kb",
                            "overhead": 1.03,
                            "tripped": 0
                        },
                        "parent": {
                            "limit_size_in_bytes": 4398986035,
                            "limit_size": "4gb",
                            "estimated_size_in_bytes": 3320291096,
                            "estimated_size": "3gb",
                            "overhead": 1,
                            "tripped": 104
                        }
                    }
                },
                "pP5muAyTSA2Z45yO8Ws0VA": {
                    "name": "opensearch-data-nodes-hot-3",
                    "breakers": {
                        "request": {
                            "limit_size_in_bytes": 2778306969,
                            "limit_size": "2.5gb",
                            "estimated_size_in_bytes": 0,
                            "estimated_size": "0b",
                            "overhead": 1,
                            "tripped": 0
                        },
                        "fielddata": {
                            "limit_size_in_bytes": 1852204646,
                            "limit_size": "1.7gb",
                            "estimated_size_in_bytes": 2948,
                            "estimated_size": "2.8kb",
                            "overhead": 1.03,
                            "tripped": 0
                        },
                        "parent": {
                            "limit_size_in_bytes": 4398986035,
                            "limit_size": "4gb",
                            "estimated_size_in_bytes": 3177812992,
                            "estimated_size": "2.9gb",
                            "overhead": 1,
                            "tripped": 40
                        }
                    }
                },
                "LQSYXzHbTfqowAOj3nrU3w": {
                    "name": "opensearch-data-nodes-hot-4",
                    "breakers": {
                        "request": {
                            "limit_size_in_bytes": 2778306969,
                            "limit_size": "2.5gb",
                            "estimated_size_in_bytes": 0,
                            "estimated_size": "0b",
                            "overhead": 1,
                            "tripped": 0
                        },
                        "fielddata": {
                            "limit_size_in_bytes": 1852204646,
                            "limit_size": "1.7gb",
                            "estimated_size_in_bytes": 1044,
                            "estimated_size": "1kb",
                            "overhead": 1.03,
                            "tripped": 0
                        },
                        "parent": {
                            "limit_size_in_bytes": 4398986035,
                            "limit_size": "4gb",
                            "estimated_size_in_bytes": 2570597288,
                            "estimated_size": "2.3gb",
                            "overhead": 1,
                            "tripped": 5
                        }
                    }
                }
            }
        }
        mock_response.status_code = 200
        mock_response.text = '{"test": "response"}'
        mock_response.raise_for_status.return_value = None
        self.mock_settings.get_requests_object().get.return_value = mock_response
        
        alerts = self.health_monitor.check_circuit_breakers()
        
        # Should trigger critical alert due to NEW parent breaker trips (first run - all trips are new)
        self.assertEqual(len(alerts), 1)
        alert = alerts[0]
        self.assertEqual(alert.check_name, "circuit_breaker_new_trips_critical")
        self.assertEqual(alert.severity, SeverityLevel.HIGH)
        self.assertIn("149 new parent breaker trips on nodes:", alert.message)
        
        # Check alert details
        critical_nodes = alert.details["nodes_with_new_trips"]
        self.assertEqual(len(critical_nodes), 3)
        node_names = [node["node"] for node in critical_nodes]
        self.assertIn("opensearch-data-nodes-hot-5", node_names)
        self.assertIn("opensearch-data-nodes-hot-3", node_names)
        self.assertIn("opensearch-data-nodes-hot-4", node_names)
    
    def test_circuit_breaker_high_usage_warning(self):
        """Test circuit breaker check with high usage but no trips"""
        mock_response = Mock()
        mock_response.json.return_value = {
            "nodes": {
                "node1": {
                    "name": "test-node-1",
                    "breakers": {
                        "parent": {"tripped": 0, "limit_size_in_bytes": 1000000, "estimated_size_in_bytes": 950000, "limit_size": "1mb", "estimated_size": "950kb"},  # 95% usage, no trips
                        "fielddata": {"tripped": 0, "limit_size_in_bytes": 400000, "estimated_size_in_bytes": 380000, "limit_size": "400kb", "estimated_size": "380kb"}  # 95% usage, no trips
                    }
                }
            }
        }
        mock_response.status_code = 200
        mock_response.text = '{"test": "response"}'
        mock_response.raise_for_status.return_value = None
        self.session.get.return_value = mock_response
        
        alerts = self.health_monitor.check_circuit_breakers()
        
        # Should only trigger high usage warning, no new trips
        self.assertEqual(len(alerts), 1)
        alert = alerts[0]
        self.assertEqual(alert.check_name, "circuit_breaker_high_usage")
        self.assertEqual(alert.severity, SeverityLevel.MEDIUM)
        self.assertIn("High circuit breaker memory usage detected on 1 node(s) (≥90%)", alert.message)
    
    def test_circuit_breaker_no_spam_on_repeated_calls(self):
        """Test that circuit breakers don't spam alerts on repeated identical trip counts"""
        mock_response = Mock()
        mock_response.json.return_value = {
            "nodes": {
                "node1": {
                    "name": "test-node-1",
                    "breakers": {
                        "parent": {"tripped": 5, "limit_size_in_bytes": 1000000, "estimated_size_in_bytes": 500000, "limit_size": "1mb", "estimated_size": "500kb"}
                    }
                }
            }
        }
        mock_response.status_code = 200
        mock_response.text = '{"test": "response"}'
        mock_response.raise_for_status.return_value = None
        self.session.get.return_value = mock_response
        
        # First call - should alert on NEW trips (5 new trips)
        alerts1 = self.health_monitor.check_circuit_breakers()
        self.assertEqual(len(alerts1), 1)
        self.assertEqual(alerts1[0].check_name, "circuit_breaker_new_trips_critical")
        self.assertIn("5 new parent breaker trips on nodes: test-node-1", alerts1[0].message)
        
        # Second call with SAME data - should NOT alert (no new trips)
        alerts2 = self.health_monitor.check_circuit_breakers()
        self.assertEqual(len(alerts2), 0)
        
        # Third call with HIGHER trip count - should alert on new trips only
        mock_response.json.return_value = {
            "nodes": {
                "node1": {
                    "name": "test-node-1", 
                    "breakers": {
                        "parent": {"tripped": 8, "limit_size_in_bytes": 1000000, "estimated_size_in_bytes": 500000, "limit_size": "1mb", "estimated_size": "500kb"}
                    }
                }
            }
        }
        
        alerts3 = self.health_monitor.check_circuit_breakers()
        self.assertEqual(len(alerts3), 1)
        self.assertEqual(alerts3[0].check_name, "circuit_breaker_new_trips_critical")
        self.assertIn("3 new parent breaker trips on nodes: test-node-1", alerts3[0].message)  # Only 3 new trips (8-5)
    
    def test_circuit_breaker_api_error(self):
        """Test circuit breaker check when API call fails"""
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.text = '{"error": "server error"}'
        mock_response.raise_for_status.side_effect = Exception("API connection failed")
        self.session.get.return_value = mock_response
        
        alerts = self.health_monitor.check_circuit_breakers()
        
        self.assertEqual(len(alerts), 1)
        alert = alerts[0]
        self.assertEqual(alert.check_name, "circuit_breaker_check_error")
        self.assertEqual(alert.severity, SeverityLevel.HIGH)
        self.assertIn("Failed to check circuit breakers: API connection failed", alert.message)
    
    def test_thread_pool_queue_normal(self):
        """Test thread pool queue check when all queues are normal (using real production data)"""
        mock_response = Mock()
        # Using actual production data showing healthy thread pools
        mock_response.json.return_value = [
            {"n": "search", "name": "search", "active": "0", "queue": "0", "rejected": "0"},
            {"n": "write", "name": "write", "active": "0", "queue": "0", "rejected": "0"},
            {"n": "search", "name": "search", "active": "0", "queue": "0", "rejected": "0"},
            {"n": "write", "name": "write", "active": "0", "queue": "0", "rejected": "0"},
            {"n": "search", "name": "search", "active": "0", "queue": "0", "rejected": "0"},
            {"n": "write", "name": "write", "active": "0", "queue": "0", "rejected": "0"}
        ]
        mock_response.status_code = 200
        mock_response.text = '{"test": "response"}'
        mock_response.raise_for_status.return_value = None
        self.mock_settings.get_requests_object().get.return_value = mock_response
        
        alerts = self.health_monitor.check_thread_pool_queues()
        
        self.assertEqual(len(alerts), 0)
        self.mock_settings.get_requests_object().get.assert_called_once_with(
            "https://test-opensearch:9200/_cat/thread_pool/search,write,bulk?v&h=n,name,active,queue,rejected&format=json"
        )
    
    def test_thread_pool_queue_warning(self):
        """Test thread pool queue check with warning level queue sizes"""
        mock_response = Mock()
        mock_response.json.return_value = [
            {"n": "node-1", "name": "search", "active": "5", "queue": "75", "rejected": "0"},  # Warning level
            {"n": "node-1", "name": "write", "active": "2", "queue": "0", "rejected": "0"},
            {"n": "node-2", "name": "bulk", "active": "1", "queue": "60", "rejected": "0"}   # Warning level
        ]
        mock_response.status_code = 200
        mock_response.text = '{"test": "response"}'
        mock_response.raise_for_status.return_value = None
        self.mock_settings.get_requests_object().get.return_value = mock_response
        
        alerts = self.health_monitor.check_thread_pool_queues()
        
        self.assertEqual(len(alerts), 1)
        alert = alerts[0]
        self.assertEqual(alert.check_name, "thread_pool_queue_warning")
        self.assertEqual(alert.severity, SeverityLevel.MEDIUM)
        self.assertIn("High thread pool queue sizes detected on 2 pool(s)", alert.message)
        
        # Check details
        warning_queues = alert.details["warning_queues"]
        self.assertEqual(len(warning_queues), 2)
        queue_info = {q["pool"]: q["queue_size"] for q in warning_queues}
        self.assertEqual(queue_info["search"], 75)
        self.assertEqual(queue_info["bulk"], 60)
    
    def test_thread_pool_queue_critical(self):
        """Test thread pool queue check with critical level queue sizes"""
        mock_response = Mock()
        mock_response.json.return_value = [
            {"n": "node-1", "name": "search", "active": "8", "queue": "150", "rejected": "0"},  # Critical level
            {"n": "node-1", "name": "write", "active": "3", "queue": "10", "rejected": "0"},
            {"n": "node-2", "name": "bulk", "active": "2", "queue": "120", "rejected": "0"}   # Critical level
        ]
        mock_response.status_code = 200
        mock_response.text = '{"test": "response"}'
        mock_response.raise_for_status.return_value = None
        self.mock_settings.get_requests_object().get.return_value = mock_response
        
        alerts = self.health_monitor.check_thread_pool_queues()
        
        self.assertEqual(len(alerts), 1)
        alert = alerts[0]
        self.assertEqual(alert.check_name, "thread_pool_queue_critical")
        self.assertEqual(alert.severity, SeverityLevel.HIGH)
        self.assertIn("Critical thread pool queue sizes detected on 2 pool(s)", alert.message)
    
    def test_thread_pool_rejections(self):
        """Test thread pool queue check with rejections"""
        mock_response = Mock()
        mock_response.json.return_value = [
            {"n": "node-1", "name": "search", "active": "5", "queue": "25", "rejected": "15"},  # Rejections
            {"n": "node-1", "name": "write", "active": "2", "queue": "5", "rejected": "0"},
            {"n": "node-2", "name": "bulk", "active": "1", "queue": "30", "rejected": "8"}   # Rejections
        ]
        mock_response.status_code = 200
        mock_response.text = '{"test": "response"}'
        mock_response.raise_for_status.return_value = None
        self.session.get.return_value = mock_response
        
        alerts = self.health_monitor.check_thread_pool_queues()
        
        self.assertEqual(len(alerts), 1)
        alert = alerts[0]
        self.assertEqual(alert.check_name, "thread_pool_new_rejections")
        self.assertEqual(alert.severity, SeverityLevel.HIGH)
        self.assertIn("NEW thread pool rejections detected on 2 pool(s) - 23 new rejections", alert.message)
        
        # Check details
        rejection_nodes = alert.details["nodes_with_new_rejections"]
        self.assertEqual(len(rejection_nodes), 2)
        total_new_rejections = sum(node["new_rejections"] for node in rejection_nodes)
        self.assertEqual(total_new_rejections, 23)
    
    def test_thread_pool_no_spam_on_repeated_rejections(self):
        """Test that thread pool rejections don't spam alerts on repeated identical rejection counts"""
        mock_response = Mock()
        mock_response.json.return_value = [
            {"n": "node-1", "name": "search", "active": "2", "queue": "10", "rejected": "5"},
        ]
        mock_response.status_code = 200
        mock_response.text = '{"test": "response"}'
        mock_response.raise_for_status.return_value = None
        self.session.get.return_value = mock_response
        
        # First call - should alert on NEW rejections (5 new)
        alerts1 = self.health_monitor.check_thread_pool_queues()
        self.assertEqual(len(alerts1), 1)
        self.assertEqual(alerts1[0].check_name, "thread_pool_new_rejections")
        self.assertIn("5 new rejections", alerts1[0].message)
        
        # Second call with SAME data - should NOT alert (no new rejections)
        alerts2 = self.health_monitor.check_thread_pool_queues()
        self.assertEqual(len(alerts2), 0)
        
        # Third call with HIGHER rejection count - should alert on new rejections only
        mock_response.json.return_value = [
            {"n": "node-1", "name": "search", "active": "2", "queue": "10", "rejected": "8"},
        ]
        
        alerts3 = self.health_monitor.check_thread_pool_queues()
        self.assertEqual(len(alerts3), 1)
        self.assertEqual(alerts3[0].check_name, "thread_pool_new_rejections")
        self.assertIn("3 new rejections", alerts3[0].message)  # Only 3 new rejections (8-5)
    
    def test_thread_pool_mixed_issues(self):
        """Test thread pool queue check with both high queues and rejections"""
        mock_response = Mock()
        mock_response.json.return_value = [
            {"n": "node-1", "name": "search", "active": "8", "queue": "150", "rejected": "5"},  # Critical queue + rejections
            {"n": "node-1", "name": "write", "active": "3", "queue": "75", "rejected": "0"},   # Warning queue
            {"n": "node-2", "name": "bulk", "active": "2", "queue": "10", "rejected": "12"}   # Rejections only
        ]
        mock_response.status_code = 200
        mock_response.text = '{"test": "response"}'
        mock_response.raise_for_status.return_value = None
        self.mock_settings.get_requests_object().get.return_value = mock_response
        
        alerts = self.health_monitor.check_thread_pool_queues()
        
        # Should have 3 alerts: critical queue, warning queue, and rejections
        self.assertEqual(len(alerts), 3)
        
        alert_types = [alert.check_name for alert in alerts]
        self.assertIn("thread_pool_queue_critical", alert_types)
        self.assertIn("thread_pool_queue_warning", alert_types)
        self.assertIn("thread_pool_new_rejections", alert_types)
    
    def test_thread_pool_api_error(self):
        """Test thread pool queue check when API call fails"""
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.text = '{"error": "server error"}'
        mock_response.raise_for_status.side_effect = Exception("API connection failed")
        self.session.get.return_value = mock_response
        
        alerts = self.health_monitor.check_thread_pool_queues()
        
        self.assertEqual(len(alerts), 1)
        alert = alerts[0]
        self.assertEqual(alert.check_name, "thread_pool_check_error")
        self.assertEqual(alert.severity, SeverityLevel.HIGH)
        self.assertIn("Failed to check thread pool queues: API connection failed", alert.message)
    
    def test_thread_pool_invalid_data(self):
        """Test thread pool queue check handles invalid data gracefully"""
        mock_response = Mock()
        mock_response.json.return_value = [
            {"n": "node-1", "name": "search", "active": "5", "queue": "25", "rejected": "0"},  # Valid
            {"n": "node-2", "name": "write", "active": "invalid", "queue": "10", "rejected": "0"},  # Invalid active
            {"n": "node-3"},  # Missing data
            {"n": "node-4", "name": "bulk", "active": "2", "queue": "150", "rejected": "5"}  # Valid critical
        ]
        mock_response.status_code = 200
        mock_response.text = '{"test": "response"}'
        mock_response.raise_for_status.return_value = None
        self.mock_settings.get_requests_object().get.return_value = mock_response
        
        alerts = self.health_monitor.check_thread_pool_queues()
        
        # Should have alerts for valid data only (critical queue and rejections)
        self.assertEqual(len(alerts), 2)
        
        alert_types = [alert.check_name for alert in alerts]
        self.assertIn("thread_pool_queue_critical", alert_types)
        self.assertIn("thread_pool_new_rejections", alert_types)


class TestTeamsWebhook(unittest.TestCase):
    
    @patch('teams_webhook.requests.post')
    def test_send_simple_message_success(self, mock_post):
        """Test sending a simple message successfully"""
        webhook_url = "https://test-webhook-url"
        mock_post.return_value.status_code = 200
        
        webhook = TeamsWebhook(webhook_url)
        result = webhook.send_simple_message("Test message")
        
        self.assertTrue(result)
        self.assertEqual(mock_post.call_count, 1)
        
        # Verify Adaptive Card payload
        call_args = mock_post.call_args
        payload = json.loads(call_args[1]['data'])
        self.assertEqual(payload["type"], "message")
        self.assertEqual(payload["attachments"][0]["contentType"], "application/vnd.microsoft.card.adaptive")
        self.assertIn("Test message", payload["attachments"][0]["content"]["body"][1]["text"])
    
    @patch('teams_webhook.requests.post')
    def test_send_alert_success(self, mock_post):
        """Test sending an alert message successfully"""
        webhook_url = "https://test-webhook-url"
        mock_post.return_value.status_code = 200
        
        webhook = TeamsWebhook(webhook_url)
        result = webhook.send_alert(
            "Test Alert", 
            "This is a test alert message", 
            SeverityLevel.HIGH
        )
        
        self.assertTrue(result)
        self.assertEqual(mock_post.call_count, 1)
        
        # Verify Adaptive Card payload structure
        call_args = mock_post.call_args
        payload = json.loads(call_args[1]['data'])
        self.assertEqual(payload["type"], "message")
        self.assertEqual(payload["attachments"][0]["contentType"], "application/vnd.microsoft.card.adaptive")
        
        card_content = payload["attachments"][0]["content"]
        self.assertEqual(card_content["type"], "AdaptiveCard")
        self.assertIn("🔴 Test Alert", card_content["body"][0]["text"])
        self.assertIn("This is a test alert message", card_content["body"][1]["text"])
    
    @patch('teams_webhook.requests.post')
    def test_send_message_failure(self, mock_post):
        """Test behavior when request fails"""
        webhook_url = "https://test-webhook-url"
        mock_post.return_value.status_code = 500
        
        webhook = TeamsWebhook(webhook_url, max_retries=1)  # Set to 1 retry to match expectation
        result = webhook.send_simple_message("Test message")
        
        self.assertFalse(result)
        self.assertEqual(mock_post.call_count, 1)
    
    @patch('teams_webhook.requests.post')
    def test_severity_emoji_mapping(self, mock_post):
        """Test that different severity levels use correct emojis"""
        webhook_url = "https://test-webhook-url"
        mock_post.return_value.status_code = 200
        
        webhook = TeamsWebhook(webhook_url)
        
        # Test each severity level
        severities_and_emojis = [
            (SeverityLevel.LOW, "🟢"),     # Green
            (SeverityLevel.MEDIUM, "🟡"),  # Yellow
            (SeverityLevel.HIGH, "🔴")     # Red
        ]
        
        for i, (severity, expected_emoji) in enumerate(severities_and_emojis):
            webhook.send_alert("Test", "Message", severity)
            
            # Get the call for this specific invocation
            call_args = mock_post.call_args_list[i]
            payload = json.loads(call_args[1]['data'])
            card_content = payload["attachments"][0]["content"]
            title_text = card_content["body"][0]["text"]
            self.assertIn(expected_emoji, title_text)


if __name__ == "__main__":
    unittest.main()