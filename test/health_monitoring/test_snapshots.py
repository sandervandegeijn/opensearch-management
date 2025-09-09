"""Snapshot monitoring tests"""
from teams_webhook import SeverityLevel
from .base_health_test import BaseHealthTest


class TestSnapshotMonitoring(BaseHealthTest):
    """Tests for snapshot monitoring functionality"""
    
    def test_data_snapshots_success(self):
        """Test data snapshots check with all successful snapshots"""
        mock_response = self.create_mock_response([
            {"id": "snapshot-1", "status": "SUCCESS"},
            {"id": "snapshot-2", "status": "SUCCESS"},
            {"id": "snapshot-3", "status": "SUCCESS"}
        ])
        self.session.get.return_value = mock_response
        
        alerts = self.health_monitor.check_data_snapshots()
        
        self.assertEqual(len(alerts), 0)
        self.session.get.assert_called_once_with(
            "https://test-opensearch:9200/_cat/snapshots/data?v&s=endEpoch&format=json"
        )
    
    def test_data_snapshots_failed(self):
        """Test data snapshots check with failed snapshots"""
        mock_response = self.create_mock_response([
            {"id": "snapshot-1", "status": "SUCCESS"},
            {"id": "snapshot-2", "status": "FAILED"},
            {"id": "snapshot-3", "status": "FAILED"}
        ])
        self.session.get.return_value = mock_response
        
        alerts = self.health_monitor.check_data_snapshots()
        
        self.assertEqual(len(alerts), 2)
        for alert in alerts:
            self.assertEqual(alert.check_name, "data_snapshots")
            self.assertEqual(alert.severity, SeverityLevel.HIGH)
            self.assertIn("FAILED", alert.message)
    
    def test_data_snapshots_partial(self):
        """Test data snapshots check with partial snapshots"""
        mock_response = self.create_mock_response([
            {"id": "snapshot-1", "status": "SUCCESS"},
            {"id": "snapshot-2", "status": "PARTIAL", "startEpoch": "1234567890", "endEpoch": "1234567900"},
            {"id": "snapshot-3", "status": "PARTIAL", "startEpoch": "1234567800", "endEpoch": "1234567850"}
        ])
        self.session.get.return_value = mock_response
        
        alerts = self.health_monitor.check_data_snapshots()
        
        self.assertEqual(len(alerts), 2)
        for alert in alerts:
            self.assertEqual(alert.check_name, "data_snapshots")
            self.assertEqual(alert.severity, SeverityLevel.MEDIUM)
            self.assertIn("PARTIAL", alert.message)
    
    def test_data_snapshots_mixed_statuses(self):
        """Test data snapshots check with mixed failed and partial snapshots"""
        mock_response = self.create_mock_response([
            {"id": "snapshot-1", "status": "SUCCESS"},
            {"id": "snapshot-2", "status": "FAILED", "startEpoch": "1234567890", "endEpoch": "1234567900"},
            {"id": "snapshot-3", "status": "PARTIAL", "startEpoch": "1234567800", "endEpoch": "1234567850"}
        ])
        self.session.get.return_value = mock_response
        
        alerts = self.health_monitor.check_data_snapshots()
        
        self.assertEqual(len(alerts), 2)
        # Check that we have one HIGH and one MEDIUM severity alert
        severities = [alert.severity for alert in alerts]
        self.assertIn(SeverityLevel.HIGH, severities)
        self.assertIn(SeverityLevel.MEDIUM, severities)
        # Find the failed and partial alerts
        failed_alert = next(alert for alert in alerts if "FAILED" in alert.message)
        partial_alert = next(alert for alert in alerts if "PARTIAL" in alert.message)
        self.assertIn("snapshot-2", failed_alert.message)
        self.assertIn("snapshot-3", partial_alert.message)
    
    def test_disaster_recovery_snapshots_failed(self):
        """Test DR snapshots check with failed snapshots"""
        mock_response = self.create_mock_response([
            {"id": "dr-snapshot-1", "status": "FAILED"}
        ])
        self.session.get.return_value = mock_response
        
        alerts = self.health_monitor.check_disaster_recovery_snapshots()
        
        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts[0].severity, SeverityLevel.HIGH)
        self.assertIn("dr-snapshot-1", alerts[0].message)
        self.assertIn("disaster-recovery", alerts[0].message)
        self.session.get.assert_called_once_with(
            "https://test-opensearch:9200/_cat/snapshots/disaster-recovery?v&s=endEpoch&format=json"
        )
    
    def test_disaster_recovery_snapshots_partial(self):
        """Test DR snapshots check with partial snapshots"""
        mock_response = self.create_mock_response([
            {"id": "dr-snapshot-1", "status": "PARTIAL", "startEpoch": "1234567890", "endEpoch": "1234567900"}
        ])
        self.session.get.return_value = mock_response
        
        alerts = self.health_monitor.check_disaster_recovery_snapshots()
        
        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts[0].severity, SeverityLevel.MEDIUM)
        self.assertIn("dr-snapshot-1", alerts[0].message)
        self.assertIn("PARTIAL", alerts[0].message)
        self.assertIn("disaster-recovery", alerts[0].message)
    
    def test_disaster_recovery_snapshots_mixed_statuses(self):
        """Test DR snapshots check with mixed failed and partial snapshots"""
        mock_response = self.create_mock_response([
            {"id": "dr-snapshot-1", "status": "SUCCESS"},
            {"id": "dr-snapshot-2", "status": "FAILED", "startEpoch": "1234567890", "endEpoch": "1234567900"},
            {"id": "dr-snapshot-3", "status": "PARTIAL", "startEpoch": "1234567800", "endEpoch": "1234567850"}
        ])
        self.session.get.return_value = mock_response
        
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