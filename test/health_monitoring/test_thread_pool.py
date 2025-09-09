"""Thread pool monitoring tests"""
from unittest.mock import Mock
from teams_webhook import SeverityLevel
from .base_health_test import BaseHealthTest


class TestThreadPoolMonitoring(BaseHealthTest):
    """Tests for thread pool queue and rejection monitoring functionality"""
    
    def test_thread_pool_queue_normal(self):
        """Test thread pool queue check when all queues are normal (using real production data)"""
        # Using actual production data showing healthy thread pools
        mock_response = self.create_mock_response([
            {"n": "search", "name": "search", "active": "0", "queue": "0", "rejected": "0"},
            {"n": "write", "name": "write", "active": "0", "queue": "0", "rejected": "0"},
            {"n": "search", "name": "search", "active": "0", "queue": "0", "rejected": "0"},
            {"n": "write", "name": "write", "active": "0", "queue": "0", "rejected": "0"},
            {"n": "search", "name": "search", "active": "0", "queue": "0", "rejected": "0"},
            {"n": "write", "name": "write", "active": "0", "queue": "0", "rejected": "0"}
        ])
        self.session.get.return_value = mock_response
        
        alerts = self.health_monitor.check_thread_pool_queues()
        
        self.assertEqual(len(alerts), 0)
        self.session.get.assert_called_once_with(
            "https://test-opensearch:9200/_cat/thread_pool/search,write,bulk?v&h=n,name,active,queue,rejected&format=json"
        )
    
    def test_thread_pool_queue_warning(self):
        """Test thread pool queue check with warning level queue sizes"""
        mock_response = self.create_mock_response([
            {"n": "node-1", "name": "search", "active": "5", "queue": "75", "rejected": "0"},  # Warning level queue
            {"n": "node-1", "name": "write", "active": "2", "queue": "5", "rejected": "0"},
            {"n": "node-2", "name": "bulk", "active": "1", "queue": "60", "rejected": "0"}   # Warning level queue
        ])
        self.session.get.return_value = mock_response
        
        alerts = self.health_monitor.check_thread_pool_queues()
        
        self.assertEqual(len(alerts), 1)
        alert = alerts[0]
        self.assertEqual(alert.check_name, "thread_pool_queue_warning")
        self.assertEqual(alert.severity, SeverityLevel.MEDIUM)
        self.assertIn("High thread pool queue sizes detected on 2 pool(s)", alert.message)
        
        # Check details
        warning_queues = alert.details["warning_queues"]
        self.assertEqual(len(warning_queues), 2)
    
    def test_thread_pool_queue_critical(self):
        """Test thread pool queue check with critical level queue sizes"""
        mock_response = self.create_mock_response([
            {"n": "node-1", "name": "search", "active": "8", "queue": "150", "rejected": "0"},  # Critical queue
            {"n": "node-1", "name": "write", "active": "2", "queue": "5", "rejected": "0"},
            {"n": "node-2", "name": "bulk", "active": "1", "queue": "120", "rejected": "0"}   # Critical queue
        ])
        self.session.get.return_value = mock_response
        
        alerts = self.health_monitor.check_thread_pool_queues()
        
        self.assertEqual(len(alerts), 1)
        alert = alerts[0]
        self.assertEqual(alert.check_name, "thread_pool_queue_critical")
        self.assertEqual(alert.severity, SeverityLevel.HIGH)
        self.assertIn("Critical thread pool queue sizes detected on 2 pool(s)", alert.message)
        
        # Check details
        critical_queues = alert.details["critical_queues"]
        self.assertEqual(len(critical_queues), 2)
    
    def test_thread_pool_rejections(self):
        """Test thread pool queue check with rejections"""
        mock_response = self.create_mock_response([
            {"n": "node-1", "name": "search", "active": "5", "queue": "25", "rejected": "15"},  # Rejections
            {"n": "node-1", "name": "write", "active": "2", "queue": "5", "rejected": "0"},
            {"n": "node-2", "name": "bulk", "active": "1", "queue": "30", "rejected": "8"}   # Rejections
        ])
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
        mock_response = self.create_mock_response([
            {"n": "node-1", "name": "search", "active": "2", "queue": "10", "rejected": "5"},
        ])
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
        mock_response = self.create_mock_response([
            {"n": "node-1", "name": "search", "active": "8", "queue": "150", "rejected": "5"},  # Critical queue + rejections
            {"n": "node-1", "name": "write", "active": "3", "queue": "75", "rejected": "0"},   # Warning level queue
            {"n": "node-2", "name": "bulk", "active": "2", "queue": "30", "rejected": "12"}    # Rejections
        ])
        self.session.get.return_value = mock_response
        
        alerts = self.health_monitor.check_thread_pool_queues()
        
        # Should get alerts for both critical queues, warning queues, and rejections
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
        mock_response = self.create_mock_response([
            {"n": "node-1", "name": "search", "active": "5", "queue": "25", "rejected": "0"},
            {"n": "node-2", "name": "write", "active": "invalid", "queue": "invalid", "rejected": "5"},  # Invalid data
            {"n": "node-3", "name": "bulk", "active": "2", "queue": "150", "rejected": "0"}  # Critical queue
        ])
        self.session.get.return_value = mock_response
        
        alerts = self.health_monitor.check_thread_pool_queues()
        
        # Should still process valid data and report issues (invalid row skipped)
        self.assertEqual(len(alerts), 1)  # Only critical queue alert (invalid row skipped)
        alert_types = [alert.check_name for alert in alerts]
        self.assertIn("thread_pool_queue_critical", alert_types)