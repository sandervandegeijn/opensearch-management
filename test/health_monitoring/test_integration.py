"""Health monitoring integration tests"""
from unittest.mock import Mock, patch
from teams_webhook import SeverityLevel, TeamsWebhook
from .base_health_test import BaseHealthTest


class TestHealthMonitoringIntegration(BaseHealthTest):
    """Tests for health monitoring integration and orchestration"""
    
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
        # Force an exception in one of the checks
        with patch.object(self.health_monitor, 'check_data_snapshots', side_effect=Exception("Test error")):
            alerts = self.health_monitor.run_daily_checks()
            
            # Should still return alerts for the error
            self.assertGreaterEqual(len(alerts), 1)
            error_alerts = [alert for alert in alerts if "health_monitor_error" in alert.check_name]
            self.assertEqual(len(error_alerts), 1)
            self.assertEqual(error_alerts[0].severity, SeverityLevel.HIGH)
            self.assertIn("Daily health monitoring system failed", error_alerts[0].message)

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


# Import HealthAlert here to avoid circular import
from health_monitor import HealthAlert