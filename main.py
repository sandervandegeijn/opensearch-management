from snapshot import Snapshot
from settings import Settings
from ilm import Ilm
from ingest_pipeline_manager import IngestPipelineManager
from template_manager import TemplateManager, TemplateType
from health_monitor import OpenSearchHealthMonitor
import argparse
import urllib3
import time
import os
import sys
from typing import Optional
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.executors.pool import ThreadPoolExecutor
from loguru import logger

# Configure loguru log level based on environment variable
log_level = os.getenv("LOG_LEVEL", "INFO").upper()
logger.remove()
logger.add(sys.stdout, level=log_level, format="{time:YYYY-MM-DD HH:mm:ss.SSS} | <level>{level: <8}</level> | {name}:{function}:{line} - {message}", colorize=True)

#Disaster recovery   
def disaster_recovery() -> None:
    logger.info("Starting disaster recovery process")
    print("Recovering cluster to latest snapshot!")
    snapshot = Snapshot(settings)
    latest_snapshot = snapshot.get_latest_snapshot()
    print(f"Latest snapshot: {latest_snapshot}")
    logger.info(f"Initiating restore of snapshot: {latest_snapshot}")
    snapshot.restore_snapshot(latest_snapshot)
    logger.info("Disaster recovery process completed")

#Snapshots
def snapshot_list() -> None:
    logger.info("Retrieving snapshot list")
    snapshot = Snapshot(settings)
    snapshot.get_snapshots()
    logger.info("Snapshot list operation completed")

def snapshot_restore(number_of_days: str) -> None:
    logger.info(f"Starting snapshot restore for: {number_of_days}")
    snapshot = Snapshot(settings)
    snapshot.restore_snapshot(number_of_days)
    logger.info(f"Snapshot restore completed for: {number_of_days}")

def snapshot_restore_latest() -> None:
    logger.info("Starting latest snapshot restore")
    snapshot = Snapshot(settings)
    latest = snapshot.get_latest_snapshot()
    logger.info(f"Restoring latest snapshot: {latest}")
    snapshot.restore_snapshot(latest)
    logger.info("Latest snapshot restore completed")

def ilm() -> None:
    """Run index lifecycle management"""
    logger.info("Initiating ILM process")
    print("Starting ILM")
    ilm_instance = Ilm(settings)
    logger.info("Running transition of old indices to snapshots")
    ilm_instance.transition_old_indices_to_snapshots()
    logger.info("Running cleanup of old data")
    ilm_instance.cleanup_old_data()
    logger.info("Running restore of missing searchable snapshots")
    ilm_instance.restore_missing_searchable_snapshots()
    logger.info("ILM process completed")

def sync_ingest_pipelines() -> None:
    """
        Ingest pipelines are used to process documents before they are indexed.
        The ingest node intercepts bulk and index requests, applies the pipeline, and then indexes the documents.
        Ingest pipelines are defined in JSON format.
        This script reads all ingest pipelines from the ingest-pipelines directory and uploads them to the OpenSearch cluster.
        They come from the filebeat github repository.
    """
    logger.info("Starting ingest pipeline synchronization")
    print(f"Syncing ingest pipelines")
    ingest_pipeline_manager = IngestPipelineManager(settings)
    path_to_pipelines = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ingest-pipelines")
    logger.info(f"Syncing pipelines from path: {path_to_pipelines}")
    ingest_pipeline_manager.sync_to_cluster(path_to_pipelines)
    logger.info("Ingest pipeline synchronization completed")

def sync_templates() -> None:
    """
        We are using index templates to create mappings and settings for our indices.
        We are using component templates to create reusable settings and mappings that can be used in multiple index templates.
        You first need to sync component templates before syncing index templates.
    """
    logger.info("Starting template synchronization")
    template_manager = TemplateManager(settings)
    
    #First sync component templates, these are needed for index templates

    #for each subdirectory in component-templates, sync the component template. The subdirectory name is the version
    path_to_component_templates = os.path.join(os.path.dirname(os.path.abspath(__file__)), "component-templates")
    logger.info(f"Syncing component templates from: {path_to_component_templates}")

    for version in os.listdir(path_to_component_templates):
        print(f"Syncing component templates for version {version}")
        logger.info(f"Processing component templates for version: {version}")
        path_to_component_templates = os.path.join(os.path.dirname(os.path.abspath(__file__)), "component-templates", version)
        template_manager.sync_to_cluster(path_to_component_templates, TemplateType.COMPONENT_TEMPLATE, version)

    #Then sync index templates
    path_to_index_templates = os.path.join(os.path.dirname(os.path.abspath(__file__)), "index-templates")
    logger.info(f"Syncing index templates from: {path_to_index_templates}")
    template_manager.sync_to_cluster(path_to_index_templates, TemplateType.INDEX_TEMPLATE)
    logger.info("Template synchronization completed")

def remove_searchable_snapshots() -> None:
    """Emergency cleanup of searchable snapshots"""
    logger.warning("Starting emergency cleanup of searchable snapshots")
    ilm_instance = Ilm(settings)
    ilm_instance.remove_searchable_snapshots()
    logger.warning("Emergency cleanup of searchable snapshots completed")

def size_rollover_job() -> None:
    """Check and rollover indices by size"""
    logger.info("Starting size-based rollover check")
    ilm_instance = Ilm(settings)
    ilm_instance.check_and_rollover_by_size()
    logger.info("Size-based rollover check completed")

# Global health monitor instance to preserve state across job executions
_global_health_monitor: Optional[OpenSearchHealthMonitor] = None

def get_health_monitor(settings_obj: Settings, webhook_url: Optional[str]) -> OpenSearchHealthMonitor:
    """Get or create the global health monitor instance"""
    global _global_health_monitor
    if _global_health_monitor is None:
        _global_health_monitor = OpenSearchHealthMonitor(settings_obj, webhook_url)
        logger.info("Created persistent HealthMonitor instance")
    return _global_health_monitor

def health_monitoring_job(settings_obj: Settings, webhook_url: Optional[str]) -> None:
    """Run OpenSearch health monitoring checks"""
    logger.info("Starting health monitoring checks")
    health_monitor = get_health_monitor(settings_obj, webhook_url)
    alerts = health_monitor.run_all_checks()
    
    if alerts:
        logger.warning(f"Health monitoring found {len(alerts)} alerts")
        for alert in alerts:
            logger.warning(f"Alert [{alert.severity.value}]: {alert.message}")
    else:
        logger.info("Health monitoring completed - no alerts")

def frequent_health_monitoring_job(settings_obj: Settings, webhook_url: Optional[str]) -> None:
    """Run frequent OpenSearch health monitoring checks (cluster health, disk space only)"""
    logger.info("Starting frequent health monitoring checks")
    health_monitor = get_health_monitor(settings_obj, webhook_url)
    alerts = health_monitor.run_frequent_checks()
    
    if alerts:
        logger.warning(f"Frequent health monitoring found {len(alerts)} alerts")
        for alert in alerts:
            logger.warning(f"Alert [{alert.severity.value}]: {alert.message}")
    else:
        logger.info("Frequent health monitoring completed - no alerts")

def daily_health_monitoring_job(settings_obj: Settings, webhook_url: Optional[str]) -> None:
    """Run daily OpenSearch health monitoring checks (data snapshots and DR snapshots)"""
    logger.info("Starting daily health monitoring checks (data snapshots and DR snapshots)")
    health_monitor = get_health_monitor(settings_obj, webhook_url)
    alerts = health_monitor.run_daily_checks()
    
    if alerts:
        logger.warning(f"Daily health monitoring found {len(alerts)} alerts")
        for alert in alerts:
            logger.warning(f"Alert [{alert.severity.value}]: {alert.message}")
    else:
        logger.info("Daily health monitoring completed - no alerts")

def test_health_monitoring(settings_obj: Settings, webhook_url: Optional[str]) -> None:
    """Test health monitoring system"""
    logger.info("Testing health monitoring system")
    health_monitor = get_health_monitor(settings_obj, webhook_url)
    results = health_monitor.test_all_checks()
    
    print("Health Monitoring Test Results:")
    print(f"  Cluster Health Check: {'✓' if results['cluster_health'] else '✗'}")
    print(f"  Disk Space Check: {'✓' if results['disk_space'] else '✗'}")
    print(f"  Data Snapshots Check: {'✓' if results['data_snapshots'] else '✗'}")
    print(f"  DR Snapshots Check: {'✓' if results['dr_snapshots'] else '✗'}")
    print(f"  Teams Webhook Test: {'✓' if results['webhook_test'] else '✗'}")
    
    if results['errors']:
        print("Errors encountered:")
        for error in results['errors']:
            print(f"  - {error}")
    
    logger.info("Health monitoring test completed")

if __name__ == "__main__":
    #Pesky self signed certs
    urllib3.disable_warnings()
    logger.info("Starting OpenSearch management script")
    parser = argparse.ArgumentParser(description="Scripting to modify and maintain Opensearch")

    choices={
             "snapshot-list",
             "snapshot-restore",
             "snapshot-restore-latest",
             "start-management",
             "start-health-monitoring",
             "ilm-now",
             "size-rollover",
             "remove-searchable-snapshots",
             "sync-ingest-pipelines-now",
             "sync-templates-now",
             "health-check-now",
             "test-health-monitoring"
             }

    parser.add_argument('-action',required=True,choices=choices,help="What do you want me to do?")
    parser.add_argument('-snapshotname', required=False, help="snapshot name from snapshot-list to restore a specific snapshot")
    args = parser.parse_args()
    

    url: Optional[str] = os.getenv("URL")
    bucket: Optional[str] = os.getenv("BUCKET")
    cert_file_path: Optional[str] = os.getenv("CERT_FILE_PATH")
    key_file_path: Optional[str] = os.getenv("KEY_FILE_PATH")
    number_of_days_on_hot_storage: int = int(os.getenv("NUMBER_OF_DAYS_ON_HOT_STORAGE") or "7")
    number_of_days_total_retention: int = int(os.getenv("NUMBER_OF_DAYS_TOTAL_RETENTION") or "90")
    rollover_size_gb: int = int(os.getenv("ROLLOVER_SIZE_GB", "50"))
    rollover_age_days: int = int(os.getenv("ROLLOVER_AGE_DAYS", "30"))
    managed_index_patterns_str: str = os.getenv("MANAGED_INDEX_PATTERNS", "log,alert")
    managed_index_patterns: tuple = tuple(pattern.strip() for pattern in managed_index_patterns_str.split(","))
    snapshotname: Optional[str] = args.snapshotname
    repository_data: Optional[str] = os.getenv("REPOSITORY_DATA")
    
    # Health monitoring configuration
    health_monitoring_enabled: bool = os.getenv("HEALTH_MONITORING_ENABLED", "false").lower() == "true"
    teams_webhook_url: Optional[str] = os.getenv("TEAMS_WEBHOOK_URL")
    health_check_interval: int = int(os.getenv("HEALTH_CHECK_INTERVAL", "300"))

    print(f"URL: {url}\nBucket: {bucket}\nCert file path: {cert_file_path}\nKey file path: {key_file_path}\nNumber of days on hot storage: {number_of_days_on_hot_storage}\nNumber of days total retention: {number_of_days_total_retention}\nRollover size: {rollover_size_gb}GB\nRollover age: {rollover_age_days} days\nManaged index patterns: {managed_index_patterns}\nSnapshot name: {snapshotname}")
    print(f"Health monitoring enabled: {health_monitoring_enabled}")
    if teams_webhook_url:
        print(f"Teams webhook configured: {teams_webhook_url[:50]}...")
    print(f"Health check interval: {health_check_interval}s")

    action: str = args.action

    # Validate required environment variables
    if not all([url, bucket, cert_file_path, key_file_path, repository_data]):
        logger.error("Missing required environment variables")
        print("Error: Missing required environment variables")
        exit(1)

    # Type assertion after validation
    settings = Settings(
        url=url,  # type: ignore
        bucket=bucket,  # type: ignore 
        cert_file_path=cert_file_path,  # type: ignore
        key_file_path=key_file_path,  # type: ignore
        number_of_days_on_hot_storage=number_of_days_on_hot_storage,
        number_of_days_total_retention=number_of_days_total_retention,
        repository=repository_data,  # type: ignore
        rollover_size_gb=rollover_size_gb,
        rollover_age_days=rollover_age_days,
        managed_index_patterns=managed_index_patterns
    )
    
    if not action:
        logger.error("No action specified")
        parser.print_usage()
        exit(1)
    
    elif action == "snapshot-list": 
        logger.info("Executing snapshot-list action")
        snapshot_list()
    elif action == "snapshot-restore": 
        if snapshotname:
            logger.info(f"Executing snapshot-restore action for: {snapshotname}")
            snapshot_restore(snapshotname)
        else:
            logger.error("Snapshot name required for restore but not provided")
            print("Error: snapshot name required for restore")
            exit(1)
    elif action == "snapshot-restore-latest": 
        logger.info("Executing snapshot-restore-latest action")
        snapshot_restore_latest()
    elif action == "remove-searchable-snapshots": 
        logger.info("Executing remove-searchable-snapshots action")
        remove_searchable_snapshots()
    elif action == "ilm-now": 
        logger.info("Executing ilm-now action")
        ilm()
    elif action == "size-rollover": 
        logger.info("Executing size-rollover action")
        ilm_instance = Ilm(settings)
        ilm_instance.check_and_rollover_by_size()
    elif action == "sync-ingest-pipelines-now": 
        logger.info("Executing sync-ingest-pipelines-now action")
        sync_ingest_pipelines()
    elif action == "sync-templates-now": 
        logger.info("Executing sync-templates-now action")
        sync_templates()
    elif action == "health-check-now": 
        logger.info("Executing health-check-now action")
        health_monitoring_job(settings, teams_webhook_url)
    elif action == "test-health-monitoring": 
        logger.info("Executing test-health-monitoring action")
        test_health_monitoring(settings, teams_webhook_url)
    elif action == "start-health-monitoring": 
        logger.info("Starting health monitoring only")
        scheduler = BackgroundScheduler(executors={'default': ThreadPoolExecutor(20)})
        
        # Create closures that capture the settings and webhook URL
        def frequent_job():
            frequent_health_monitoring_job(settings, teams_webhook_url)
        
        def daily_job():
            daily_health_monitoring_job(settings, teams_webhook_url)
        
        logger.info(f"Scheduling frequent health monitoring checks every {health_check_interval} seconds")
        scheduler.add_job(frequent_job, "interval", seconds=health_check_interval, max_instances=1)
        
        logger.info("Scheduling daily data snapshot checks at 08:00")
        scheduler.add_job(daily_job, "cron", hour=8, minute=0, max_instances=1)

        scheduler.start()
        logger.info("Health monitoring scheduler started successfully")
        try:
            while True:
                time.sleep(1)
        except (KeyboardInterrupt, SystemExit):
            logger.info("Received shutdown signal, stopping health monitoring")
            if scheduler:
                scheduler.shutdown()
            logger.info("Health monitoring shutdown completed")
    elif action == "start-management": 
        logger.info("Starting background scheduler for management tasks")
        scheduler = BackgroundScheduler(executors={'default': ThreadPoolExecutor(20)})
        
        logger.info("Scheduling ILM job for daily execution at 01:00")
        scheduler.add_job(ilm, "cron", hour=1, max_instances=1)

        logger.info("Scheduling size-based rollover checks every 15 minutes")
        scheduler.add_job(size_rollover_job, "cron", minute="*/15", max_instances=1)

        logger.info("Scheduling ingest pipeline sync for daily execution at 04:10")
        scheduler.add_job(sync_ingest_pipelines, "cron", hour=4, minute=10, max_instances=1)

        logger.info("Scheduling template sync for daily execution at 04:05")
        scheduler.add_job(sync_templates, "cron", hour=4, minute=5, max_instances=1)

        # Add health monitoring jobs if enabled
        if health_monitoring_enabled:
            # Create closures for the management scheduler too
            def frequent_mgmt_job():
                frequent_health_monitoring_job(settings, teams_webhook_url)
            
            def daily_mgmt_job():
                daily_health_monitoring_job(settings, teams_webhook_url)
            
            logger.info(f"Scheduling frequent health monitoring checks every {health_check_interval} seconds")
            scheduler.add_job(frequent_mgmt_job, "interval", seconds=health_check_interval, max_instances=1)
            
            logger.info("Scheduling daily data snapshot checks at 08:00")
            scheduler.add_job(daily_mgmt_job, "cron", hour=8, minute=0, max_instances=1)

        scheduler.start()
        logger.info("Background scheduler started successfully")
        try:
            while True:
                time.sleep(1)
        except (KeyboardInterrupt, SystemExit):
            logger.info("Received shutdown signal, stopping scheduler")
            if scheduler:
                scheduler.shutdown()
            logger.info("Scheduler shutdown completed")
    else:
        logger.error(f"Invalid action specified: {action}")
        parser.print_usage()
        exit(1)