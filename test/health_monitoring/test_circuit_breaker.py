"""Circuit breaker monitoring tests"""
from unittest.mock import Mock
from teams_webhook import SeverityLevel
from .base_health_test import BaseHealthTest


class TestCircuitBreakerMonitoring(BaseHealthTest):
    """Tests for circuit breaker monitoring functionality"""
    
    def test_circuit_breaker_normal(self):
        """Test circuit breaker check when all breakers are normal"""
        mock_response = self.create_mock_response({
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
        })
        self.session.get.return_value = mock_response
        
        alerts = self.health_monitor.check_circuit_breakers()
        
        self.assertEqual(len(alerts), 0)
        self.session.get.assert_called_once_with(
            "https://test-opensearch:9200/_nodes/stats/breaker"
        )
    
    def test_circuit_breaker_production_data(self):
        """Test circuit breaker check with real production data (should trigger critical alerts)"""
        # Using actual production data showing trips - simplified sample of full data
        mock_response = self.create_mock_response({
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
        })
        self.session.get.return_value = mock_response
        
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
        mock_response = self.create_mock_response({
            "nodes": {
                "node1": {
                    "name": "test-node-1",
                    "breakers": {
                        "parent": {"tripped": 0, "limit_size_in_bytes": 1000000, "estimated_size_in_bytes": 950000, "limit_size": "1mb", "estimated_size": "950kb"},  # 95% usage, no trips
                        "fielddata": {"tripped": 0, "limit_size_in_bytes": 400000, "estimated_size_in_bytes": 380000, "limit_size": "400kb", "estimated_size": "380kb"}  # 95% usage, no trips
                    }
                }
            }
        })
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
        mock_response = self.create_mock_response({
            "nodes": {
                "node1": {
                    "name": "test-node-1",
                    "breakers": {
                        "parent": {"tripped": 5, "limit_size_in_bytes": 1000000, "estimated_size_in_bytes": 500000, "limit_size": "1mb", "estimated_size": "500kb"}
                    }
                }
            }
        })
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