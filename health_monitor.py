import requests
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
from loguru import logger
from teams_webhook import TeamsWebhook, SeverityLevel
from settings import Settings


class HealthAlert:
    def __init__(self, check_name: str, severity: SeverityLevel, message: str, details: Optional[Dict[str, Any]] = None):
        self.check_name = check_name
        self.severity = severity
        self.message = message
        self.details = details or {}


class OpenSearchHealthMonitor:
    def __init__(self, settings: Settings, webhook_url: Optional[str] = None):
        self.settings = settings
        self.session = settings.get_requests_object()
        self.webhook = TeamsWebhook(webhook_url) if webhook_url else None
        self._yellow_status_start_time = None
        
        # State tracking to prevent alert spam
        self._previous_circuit_breaker_trips = {}  # {node_id: {breaker_name: trip_count}}
        self._previous_thread_pool_rejections = {}  # {node_name: {pool_name: rejection_count}}
        
    def run_all_checks(self) -> List[HealthAlert]:
        """Run all health checks and return any alerts"""
        alerts = []
        
        try:
            alerts.extend(self.check_cluster_health())
            alerts.extend(self.check_disk_space())
            alerts.extend(self.check_jvm_heap_usage())
            alerts.extend(self.check_circuit_breakers())
            alerts.extend(self.check_thread_pool_queues())
            alerts.extend(self.check_data_snapshots())
            alerts.extend(self.check_disaster_recovery_snapshots())
            
        except Exception as e:
            logger.error(f"Health monitoring failed: {str(e)}")
            alerts.append(HealthAlert(
                "health_monitor_error",
                SeverityLevel.HIGH,
                f"Health monitoring system failed: {str(e)}"
            ))
        
        # Send alerts to Teams if webhook is configured
        if self.webhook and alerts:
            self._send_alerts_to_teams(alerts)
            
        return alerts
    
    def run_frequent_checks(self) -> List[HealthAlert]:
        """Run frequent health checks (cluster health, disk space, JVM heap, circuit breakers, and thread pools)"""
        alerts = []
        
        try:
            alerts.extend(self.check_cluster_health())
            alerts.extend(self.check_disk_space())
            alerts.extend(self.check_jvm_heap_usage())
            alerts.extend(self.check_circuit_breakers())
            alerts.extend(self.check_thread_pool_queues())
            
        except Exception as e:
            logger.error(f"Frequent health monitoring failed: {str(e)}")
            alerts.append(HealthAlert(
                "health_monitor_error",
                SeverityLevel.HIGH,
                f"Frequent health monitoring system failed: {str(e)}"
            ))
        
        # Send alerts to Teams if webhook is configured
        if self.webhook and alerts:
            self._send_alerts_to_teams(alerts)
            
        return alerts
    
    def run_daily_checks(self) -> List[HealthAlert]:
        """Run daily health checks (data snapshots and disaster recovery snapshots)"""
        alerts = []
        
        try:
            alerts.extend(self.check_data_snapshots())
            alerts.extend(self.check_disaster_recovery_snapshots())
            
        except Exception as e:
            logger.error(f"Daily health monitoring failed: {str(e)}")
            alerts.append(HealthAlert(
                "health_monitor_error",
                SeverityLevel.HIGH,
                f"Daily health monitoring system failed: {str(e)}"
            ))
        
        # Send alerts to Teams if webhook is configured
        if self.webhook and alerts:
            self._send_alerts_to_teams(alerts)
            
        return alerts
    
    def check_cluster_health(self) -> List[HealthAlert]:
        """Check OpenSearch cluster health status"""
        alerts = []
        
        try:
            logger.debug("Checking cluster health")
            response = self.session.get(f"{self.settings.url}/_cluster/health")
            logger.debug(f"Cluster health HTTP response: status_code={response.status_code}, content={response.text}")
            response.raise_for_status()
            data = response.json()
            
            status = data.get("status", "unknown")
            cluster_name = data.get("cluster_name", "unknown")
            
            if status != "green":
                now = datetime.now()
                
                if status == "yellow":
                    if self._yellow_status_start_time is None:
                        self._yellow_status_start_time = now
                        logger.debug(f"Yellow cluster status detected, starting 15-minute timer")
                    elif now - self._yellow_status_start_time >= timedelta(minutes=15):
                        severity = SeverityLevel.MEDIUM
                        alert = HealthAlert(
                            "cluster_health",
                            severity,
                            f"OpenSearch cluster '{cluster_name}' status is {status.upper()} (sustained for 15+ minutes)",
                            {
                                "cluster_name": cluster_name,
                                "status": status,
                                "active_shards": data.get("active_shards", 0),
                                "relocating_shards": data.get("relocating_shards", 0),
                                "initializing_shards": data.get("initializing_shards", 0),
                                "unassigned_shards": data.get("unassigned_shards", 0),
                                "duration_minutes": int((now - self._yellow_status_start_time).total_seconds() / 60)
                            }
                        )
                        alerts.append(alert)
                        logger.warning(f"Cluster health alert: {alert.message}")
                    else:
                        duration = int((now - self._yellow_status_start_time).total_seconds() / 60)
                        logger.debug(f"Cluster status is yellow for {duration} minutes, waiting for 15 minutes before alerting")
                else:
                    self._yellow_status_start_time = None
                    severity = SeverityLevel.HIGH
                    alert = HealthAlert(
                        "cluster_health",
                        severity,
                        f"OpenSearch cluster '{cluster_name}' status is {status.upper()}",
                        {
                            "cluster_name": cluster_name,
                            "status": status,
                            "active_shards": data.get("active_shards", 0),
                            "relocating_shards": data.get("relocating_shards", 0),
                            "initializing_shards": data.get("initializing_shards", 0),
                            "unassigned_shards": data.get("unassigned_shards", 0)
                        }
                    )
                    alerts.append(alert)
                    logger.warning(f"Cluster health alert: {alert.message}")
            else:
                self._yellow_status_start_time = None
                logger.debug(f"Cluster health OK: {status}")
                
        except Exception as e:
            logger.error(f"Failed to check cluster health: {str(e)}")
            alerts.append(HealthAlert(
                "cluster_health_error",
                SeverityLevel.HIGH,
                f"Failed to check cluster health: {str(e)}"
            ))
        
        return alerts
    
    def check_disk_space(self) -> List[HealthAlert]:
        """Check data node disk space usage"""
        alerts = []
        
        try:
            logger.debug("Checking disk space usage")
            response = self.session.get(f"{self.settings.url}/_cat/nodes?v&h=n,id,v,r,rp,dt,du,dup&format=json")
            logger.debug(f"Disk space HTTP response: status_code={response.status_code}, content={response.text}")
            response.raise_for_status()
            data = response.json()
            
            data_nodes = [node for node in data if "d" in node.get("r", "").lower()]
            
            if not data_nodes:
                logger.warning("No data nodes found for disk space check")
                return alerts
            
            total_usage = 0
            node_count = 0
            high_usage_nodes = []
            
            for node in data_nodes:
                try:
                    usage_percent = float(node.get("dup", "0").rstrip('%'))
                    total_usage += usage_percent
                    node_count += 1
                    
                    if usage_percent >= 90:
                        high_usage_nodes.append({
                            "name": node.get("n", "unknown"),
                            "usage": usage_percent
                        })
                        
                except (ValueError, TypeError):
                    logger.warning(f"Could not parse disk usage for node: {node}")
                    continue
            
            if node_count > 0:
                average_usage = total_usage / node_count
                
                if average_usage >= 93:
                    alert = HealthAlert(
                        "disk_space_critical",
                        SeverityLevel.HIGH,
                        f"Data nodes average disk usage is {average_usage:.1f}% (>= 93%)",
                        {"average_usage": average_usage, "high_usage_nodes": high_usage_nodes}
                    )
                    alerts.append(alert)
                    logger.warning(f"Critical disk space alert: {alert.message}")
                    
                elif average_usage > 90:
                    alert = HealthAlert(
                        "disk_space_warning",
                        SeverityLevel.MEDIUM,
                        f"Data nodes average disk usage is {average_usage:.1f}% (> 90%)",
                        {"average_usage": average_usage, "high_usage_nodes": high_usage_nodes}
                    )
                    alerts.append(alert)
                    logger.warning(f"Disk space warning: {alert.message}")
                else:
                    logger.debug(f"Disk space OK: {average_usage:.1f}% average usage")
                    
        except Exception as e:
            logger.error(f"Failed to check disk space: {str(e)}")
            alerts.append(HealthAlert(
                "disk_space_error",
                SeverityLevel.HIGH,
                f"Failed to check disk space: {str(e)}"
            ))
        
        return alerts
    
    def check_jvm_heap_usage(self) -> List[HealthAlert]:
        """Check JVM heap memory usage across nodes"""
        alerts = []
        
        try:
            logger.debug("Checking JVM heap memory usage")
            response = self.session.get(f"{self.settings.url}/_cat/nodes?v&h=n,hp,hm,hc&format=json")
            logger.debug(f"JVM heap HTTP response: status_code={response.status_code}, content={response.text}")
            response.raise_for_status()
            data = response.json()
            
            high_usage_nodes = []
            critical_usage_nodes = []
            
            for node in data:
                try:
                    # hp = heap.percent, hm = heap.max, hc = heap.current
                    heap_percent = int(node.get("hp", "0"))
                    heap_current = node.get("hc", "0")
                    heap_max = node.get("hm", "0")
                    node_name = node.get("n", "unknown")
                    
                    if heap_percent >= 95:  # Critical threshold
                        critical_usage_nodes.append({
                            "name": node_name,
                            "heap_percent": heap_percent,
                            "heap_current": heap_current,
                            "heap_max": heap_max
                        })
                    elif heap_percent >= 90:  # Warning threshold  
                        high_usage_nodes.append({
                            "name": node_name,
                            "heap_percent": heap_percent,
                            "heap_current": heap_current,
                            "heap_max": heap_max
                        })
                        
                except (ValueError, TypeError) as e:
                    logger.warning(f"Could not parse JVM heap data for node {node.get('n', 'unknown')}: {e}")
                    continue
            
            # Create critical alerts
            if critical_usage_nodes:
                alert = HealthAlert(
                    "jvm_heap_critical",
                    SeverityLevel.HIGH,
                    f"Critical JVM heap usage detected on {len(critical_usage_nodes)} node(s) (>= 95%)",
                    {"nodes": critical_usage_nodes}
                )
                alerts.append(alert)
                logger.warning(f"Critical JVM heap usage alert: {alert.message}")
            
            # Create warning alerts
            if high_usage_nodes:
                alert = HealthAlert(
                    "jvm_heap_warning", 
                    SeverityLevel.MEDIUM,
                    f"High JVM heap usage detected on {len(high_usage_nodes)} node(s) (>= 90%)",
                    {"nodes": high_usage_nodes}
                )
                alerts.append(alert)
                logger.warning(f"High JVM heap usage warning: {alert.message}")
            else:
                if not critical_usage_nodes:  # Only log OK if no critical alerts either
                    logger.debug("JVM heap usage OK on all nodes")
                    
        except Exception as e:
            logger.error(f"Failed to check JVM heap usage: {str(e)}")
            alerts.append(HealthAlert(
                "jvm_heap_check_error",
                SeverityLevel.HIGH,
                f"Failed to check JVM heap usage: {str(e)}"
            ))
        
        return alerts
    
    def check_circuit_breakers(self) -> List[HealthAlert]:
        """Check for circuit breaker trips across all nodes - only alerts on NEW trips"""
        alerts = []
        
        try:
            logger.debug("Checking circuit breaker status")
            response = self.session.get(f"{self.settings.url}/_nodes/stats/breaker")
            logger.debug(f"Circuit breaker HTTP response: status_code={response.status_code}, content_length={len(response.text)}")
            response.raise_for_status()
            data = response.json()
            
            current_trips = {}
            new_critical_trips = []
            new_warning_issues = []
            high_usage_warnings = []
            
            # Process each node's circuit breaker stats
            for node_id, node_data in data.get("nodes", {}).items():
                node_name = node_data.get("name", node_id)
                breakers = node_data.get("breakers", {})
                
                # Initialize tracking for new nodes
                if node_id not in self._previous_circuit_breaker_trips:
                    self._previous_circuit_breaker_trips[node_id] = {}
                
                current_trips[node_id] = {}
                node_new_trips = []
                node_high_usage = []
                
                for breaker_name, breaker_stats in breakers.items():
                    current_count = breaker_stats.get("tripped", 0)
                    previous_count = self._previous_circuit_breaker_trips[node_id].get(breaker_name, 0)
                    
                    # Update current count
                    current_trips[node_id][breaker_name] = current_count
                    
                    # Calculate usage percentage
                    limit_size = breaker_stats.get("limit_size_in_bytes", 0)
                    estimated_size = breaker_stats.get("estimated_size_in_bytes", 0)
                    usage_percent = (estimated_size / limit_size * 100) if limit_size > 0 else 0
                    
                    # Check for NEW trips (count increased)
                    new_trips = current_count - previous_count
                    if new_trips > 0:
                        trip_info = {
                            "breaker": breaker_name,
                            "new_trips": new_trips,
                            "total_trips": current_count,
                            "usage_percent": round(usage_percent, 1),
                            "limit_size": breaker_stats.get("limit_size", "unknown"),
                            "estimated_size": breaker_stats.get("estimated_size", "unknown")
                        }
                        
                        # Critical: NEW trips on parent breaker
                        if breaker_name == "parent":
                            trip_info["severity"] = "critical"
                            node_new_trips.append(trip_info)
                        else:
                            # Warning: NEW trips on non-parent breakers  
                            trip_info["severity"] = "warning"
                            node_new_trips.append(trip_info)
                    
                    # Also check for high usage (always alert, regardless of trips)
                    elif usage_percent >= 90:
                        node_high_usage.append({
                            "breaker": breaker_name,
                            "usage_percent": round(usage_percent, 1),
                            "limit_size": breaker_stats.get("limit_size", "unknown"),
                            "estimated_size": breaker_stats.get("estimated_size", "unknown"),
                            "total_trips": current_count
                        })
                
                # Categorize nodes with new trips
                critical_trips = [trip for trip in node_new_trips if trip["severity"] == "critical"]
                warning_trips = [trip for trip in node_new_trips if trip["severity"] == "warning"]
                
                if critical_trips:
                    new_critical_trips.append({
                        "node": node_name,
                        "node_id": node_id,
                        "new_trips": critical_trips,
                        "total_new_trips": sum(trip["new_trips"] for trip in critical_trips)
                    })
                elif warning_trips:
                    new_warning_issues.append({
                        "node": node_name, 
                        "node_id": node_id,
                        "new_trips": warning_trips,
                        "total_new_trips": sum(trip["new_trips"] for trip in warning_trips)
                    })
                
                if node_high_usage:
                    high_usage_warnings.append({
                        "node": node_name,
                        "node_id": node_id, 
                        "high_usage_breakers": node_high_usage
                    })
            
            # Update state tracking
            self._previous_circuit_breaker_trips = current_trips
            
            # Create alerts only for NEW issues
            if new_critical_trips:
                total_new_trips = sum(node["total_new_trips"] for node in new_critical_trips)
                node_names = [node["node"] for node in new_critical_trips]
                node_list = ", ".join(node_names[:3])  # Show first 3 nodes
                if len(node_names) > 3:
                    node_list += f" (and {len(node_names) - 3} more)"
                
                alert = HealthAlert(
                    "circuit_breaker_new_trips_critical",
                    SeverityLevel.HIGH,
                    f"NEW circuit breaker trips detected - {total_new_trips} new parent breaker trips on nodes: {node_list}",
                    {"nodes_with_new_trips": new_critical_trips}
                )
                alerts.append(alert)
                logger.warning(f"NEW circuit breaker trips alert: {alert.message}")
            
            if new_warning_issues:
                total_new_trips = sum(node["total_new_trips"] for node in new_warning_issues)
                node_names = [node["node"] for node in new_warning_issues]
                node_list = ", ".join(node_names[:3])  # Show first 3 nodes
                if len(node_names) > 3:
                    node_list += f" (and {len(node_names) - 3} more)"
                
                alert = HealthAlert(
                    "circuit_breaker_new_trips_warning",
                    SeverityLevel.MEDIUM,
                    f"NEW circuit breaker trips detected - {total_new_trips} new trips on non-parent breakers on nodes: {node_list}",
                    {"nodes_with_new_trips": new_warning_issues}
                )
                alerts.append(alert)
                logger.warning(f"Circuit breaker new trips warning: {alert.message}")
            
            if high_usage_warnings:
                alert = HealthAlert(
                    "circuit_breaker_high_usage",
                    SeverityLevel.MEDIUM,
                    f"High circuit breaker memory usage detected on {len(high_usage_warnings)} node(s) (≥90%)",
                    {"nodes_with_high_usage": high_usage_warnings}
                )
                alerts.append(alert)
                logger.warning(f"Circuit breaker high usage warning: {alert.message}")
            
            if not alerts:
                logger.debug("Circuit breakers OK - no new trips or high usage detected")
                
        except Exception as e:
            logger.error(f"Failed to check circuit breakers: {str(e)}")
            alerts.append(HealthAlert(
                "circuit_breaker_check_error",
                SeverityLevel.HIGH,
                f"Failed to check circuit breakers: {str(e)}"
            ))
        
        return alerts
    
    def check_thread_pool_queues(self) -> List[HealthAlert]:
        """Check thread pool queue sizes and rejections - only alerts on NEW rejections"""
        alerts = []
        
        try:
            logger.debug("Checking thread pool queue status")
            response = self.session.get(f"{self.settings.url}/_cat/thread_pool/search,write,bulk?v&h=n,name,active,queue,rejected&format=json")
            logger.debug(f"Thread pool HTTP response: status_code={response.status_code}, content_length={len(response.text)}")
            response.raise_for_status()
            data = response.json()
            
            high_queue_nodes = []
            new_rejection_nodes = []
            current_rejections = {}
            
            for pool_data in data:
                try:
                    node_name = pool_data.get("n", "unknown")
                    pool_name = pool_data.get("name", "unknown")
                    active = int(pool_data.get("active", 0))
                    queue = int(pool_data.get("queue", 0))
                    rejected = int(pool_data.get("rejected", 0))
                    
                    # Critical thread pools to monitor
                    if pool_name in ["search", "write", "bulk"]:
                        # Alert on high queue size (always check - queue sizes are transient)
                        if queue >= 100:  # Critical threshold
                            high_queue_nodes.append({
                                "node": node_name,
                                "pool": pool_name,
                                "queue_size": queue,
                                "active": active,
                                "rejected": rejected,
                                "severity": "critical"
                            })
                        elif queue >= 50:  # Warning threshold
                            high_queue_nodes.append({
                                "node": node_name,
                                "pool": pool_name,
                                "queue_size": queue,
                                "active": active,
                                "rejected": rejected,
                                "severity": "warning"
                            })
                        
                        # Track rejections for NEW rejections only
                        if node_name not in current_rejections:
                            current_rejections[node_name] = {}
                        current_rejections[node_name][pool_name] = rejected
                        
                        # Check for NEW rejections
                        previous_rejected = self._previous_thread_pool_rejections.get(node_name, {}).get(pool_name, 0)
                        new_rejections = rejected - previous_rejected
                        
                        if new_rejections > 0:
                            new_rejection_nodes.append({
                                "node": node_name,
                                "pool": pool_name,
                                "new_rejections": new_rejections,
                                "total_rejections": rejected,
                                "queue_size": queue,
                                "active": active
                            })
                            
                except (ValueError, TypeError) as e:
                    logger.warning(f"Could not parse thread pool data for node {pool_data.get('n', 'unknown')}: {e}")
                    continue
            
            # Update rejection tracking
            self._previous_thread_pool_rejections = current_rejections
            
            # Create alerts for high queues (always alert - queues are transient)
            critical_queues = [n for n in high_queue_nodes if n["severity"] == "critical"]
            warning_queues = [n for n in high_queue_nodes if n["severity"] == "warning"]
            
            if critical_queues:
                alert = HealthAlert(
                    "thread_pool_queue_critical",
                    SeverityLevel.HIGH,
                    f"Critical thread pool queue sizes detected on {len(critical_queues)} pool(s)",
                    {"critical_queues": critical_queues}
                )
                alerts.append(alert)
                logger.warning(f"Critical thread pool queue alert: {alert.message}")
            
            if warning_queues:
                alert = HealthAlert(
                    "thread_pool_queue_warning",
                    SeverityLevel.MEDIUM,
                    f"High thread pool queue sizes detected on {len(warning_queues)} pool(s)",
                    {"warning_queues": warning_queues}
                )
                alerts.append(alert)
                logger.warning(f"Thread pool queue warning: {alert.message}")
            
            # Create alerts ONLY for NEW rejections
            if new_rejection_nodes:
                total_new_rejections = sum(node["new_rejections"] for node in new_rejection_nodes)
                alert = HealthAlert(
                    "thread_pool_new_rejections",
                    SeverityLevel.HIGH,
                    f"NEW thread pool rejections detected on {len(new_rejection_nodes)} pool(s) - {total_new_rejections} new rejections",
                    {"nodes_with_new_rejections": new_rejection_nodes}
                )
                alerts.append(alert)
                logger.warning(f"NEW thread pool rejection alert: {alert.message}")
            
            if not alerts:
                logger.debug("Thread pool queues OK - no high queues or new rejections detected")
                
        except Exception as e:
            logger.error(f"Failed to check thread pool queues: {str(e)}")
            alerts.append(HealthAlert(
                "thread_pool_check_error",
                SeverityLevel.HIGH,
                f"Failed to check thread pool queues: {str(e)}"
            ))
        
        return alerts
    
    def check_data_snapshots(self) -> List[HealthAlert]:
        """Check data repository snapshot status"""
        return self._check_snapshots("data", "data_snapshots")
    
    def check_disaster_recovery_snapshots(self) -> List[HealthAlert]:
        """Check disaster recovery repository snapshot status"""
        return self._check_snapshots("disaster-recovery", "dr_snapshots")
    
    def _check_snapshots(self, repository: str, check_name: str) -> List[HealthAlert]:
        """Generic method to check snapshot status for a repository"""
        alerts = []
        
        try:
            logger.debug(f"Checking {repository} snapshots")
            response = self.session.get(f"{self.settings.url}/_cat/snapshots/{repository}?v&s=endEpoch&format=json")
            logger.debug(f"{repository} snapshots HTTP response: status_code={response.status_code}, content={response.text}")
            response.raise_for_status()
            data = response.json()
            
            failed_snapshots = [entry for entry in data if entry.get('status') == 'FAILED']
            partial_snapshots = [entry for entry in data if entry.get('status') == 'PARTIAL']
            
            # Handle failed snapshots with HIGH severity
            for snapshot in failed_snapshots:
                alert = HealthAlert(
                    check_name,
                    SeverityLevel.HIGH,
                    f"Snapshot '{snapshot.get('id', 'unknown')}' in {repository} repository has FAILED status",
                    {
                        "repository": repository,
                        "snapshot_id": snapshot.get('id'),
                        "status": snapshot.get('status'),
                        "start_time": snapshot.get('startEpoch'),
                        "end_time": snapshot.get('endEpoch')
                    }
                )
                alerts.append(alert)
                logger.warning(f"Failed snapshot alert: {alert.message}")
            
            # Handle partial snapshots with MEDIUM severity
            for snapshot in partial_snapshots:
                alert = HealthAlert(
                    check_name,
                    SeverityLevel.MEDIUM,
                    f"Snapshot '{snapshot.get('id', 'unknown')}' in {repository} repository has PARTIAL status",
                    {
                        "repository": repository,
                        "snapshot_id": snapshot.get('id'),
                        "status": snapshot.get('status'),
                        "start_time": snapshot.get('startEpoch'),
                        "end_time": snapshot.get('endEpoch')
                    }
                )
                alerts.append(alert)
                logger.warning(f"Partial snapshot alert: {alert.message}")
            
            if not failed_snapshots and not partial_snapshots:
                logger.debug(f"{repository} snapshots OK - no failed or partial snapshots")
                
        except Exception as e:
            logger.error(f"Failed to check {repository} snapshots: {str(e)}")
            alerts.append(HealthAlert(
                f"{check_name}_error",
                SeverityLevel.HIGH,
                f"Failed to check {repository} snapshots: {str(e)}"
            ))
        
        return alerts
    
    def _send_alerts_to_teams(self, alerts: List[HealthAlert]) -> None:
        """Send alerts to Teams webhook"""
        if not self.webhook:
            return
        
        # Group alerts by severity for better formatting
        high_alerts = [a for a in alerts if a.severity == SeverityLevel.HIGH]
        medium_alerts = [a for a in alerts if a.severity == SeverityLevel.MEDIUM]
        low_alerts = [a for a in alerts if a.severity == SeverityLevel.LOW]
        
        # Send high severity alerts immediately
        for alert in high_alerts:
            self.webhook.send_alert(
                "OpenSearch Health Alert - CRITICAL",
                alert.message,
                alert.severity
            )
        
        # Group medium and low alerts
        if medium_alerts:
            messages = [alert.message for alert in medium_alerts]
            combined_message = "\\n".join([f"• {msg}" for msg in messages])
            self.webhook.send_alert(
                f"OpenSearch Health Warnings ({len(medium_alerts)})",
                combined_message,
                SeverityLevel.MEDIUM
            )
        
        if low_alerts:
            messages = [alert.message for alert in low_alerts]
            combined_message = "\\n".join([f"• {msg}" for msg in messages])
            self.webhook.send_alert(
                f"OpenSearch Health Info ({len(low_alerts)})",
                combined_message,
                SeverityLevel.LOW
            )
    
    def test_all_checks(self) -> Dict[str, Any]:
        """Test all health checks and return summary"""
        results = {
            "cluster_health": False,
            "disk_space": False,
            "data_snapshots": False,
            "dr_snapshots": False,
            "webhook_test": False,
            "errors": []
        }
        
        try:
            # Test each check individually
            cluster_alerts = self.check_cluster_health()
            results["cluster_health"] = len([a for a in cluster_alerts if "error" not in a.check_name]) >= 0
            
            disk_alerts = self.check_disk_space()
            results["disk_space"] = len([a for a in disk_alerts if "error" not in a.check_name]) >= 0
            
            data_alerts = self.check_data_snapshots()
            results["data_snapshots"] = len([a for a in data_alerts if "error" not in a.check_name]) >= 0
            
            dr_alerts = self.check_disaster_recovery_snapshots()
            results["dr_snapshots"] = len([a for a in dr_alerts if "error" not in a.check_name]) >= 0
            
            # Test webhook if configured
            if self.webhook:
                results["webhook_test"] = self.webhook.test_connection()
            
            all_alerts = cluster_alerts + disk_alerts + data_alerts + dr_alerts
            results["errors"] = [alert.message for alert in all_alerts if "error" in alert.check_name]
            
        except Exception as e:
            results["errors"].append(f"Test failed: {str(e)}")
        
        return results