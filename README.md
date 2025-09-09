# OpenSearch Management Container

This container manages OpenSearch indices and datastreams through automated Index Lifecycle Management (ILM). It provides advanced lifecycle management capabilities that extend beyond OpenSearch's native ILM features, specifically designed to support searchable snapshots and cost-effective data archival.

## Why This Project Exists

OpenSearch's native Index Lifecycle Management (ILM) has significant limitations for organizations requiring searchable snapshots:

1. **Limited Searchable Snapshot Support**: OpenSearch's native ILM doesn't provide robust searchable snapshot management
2. **Complex Lifecycle Requirements**: Managing the transition from hot storage → snapshots → searchable snapshots → deletion requires custom logic
3. **Cost Optimization**: Need for fine-grained control over storage costs by moving data through different tiers
4. **Operational Complexity**: Native ILM lacks comprehensive error handling and retry mechanisms for snapshot operations

This project fills these gaps by providing a complete ILM solution with:
- Automated searchable snapshot management
- Robust error handling and retry logic
- Flexible size-based rollover independent of time-based lifecycle
- Comprehensive monitoring and logging
- Support for complex multi-tier data lifecycle patterns

## High-Level Architecture

The system is built around several core components:

### Core Components

- **`ilm.py`**: Main ILM engine handling lifecycle transitions, snapshot management, and searchable snapshot operations
- **`main.py`**: CLI interface and scheduler orchestrating all operations 
- **`snapshot.py`**: Snapshot repository management and disaster recovery operations
- **`template_manager.py`**: Index and component template synchronization
- **`ingest_pipeline_manager.py`**: Ingest pipeline management for document processing
- **`health_monitor.py`**: Health monitoring with MS Teams integration for cluster status, disk space, and snapshot alerts
- **`settings.py`**: Configuration management and OpenSearch client setup

### Data Flow Architecture

```
[Vector/Data Sources] 
           ↓
[Write Aliases] (e.g., log-pattern-write)
           ↓
[Hot Storage Indices] (log-pattern-000001, log-pattern-000002...)
           ↓ (after NUMBER_OF_DAYS_ON_HOT_STORAGE)
[S3 Snapshots] (archived to S3 bucket)
           ↓ (restored as)
[Searchable Snapshots] (log-pattern-000001-snapshot)
           ↓ (after NUMBER_OF_DAYS_TOTAL_RETENTION)
[Deletion]
```

## ILM Lifecycle Phases

**Hot Storage → Snapshot → Searchable Snapshot → Deletion**

⚠️ **Special Case**: When `NUMBER_OF_DAYS_ON_HOT_STORAGE` equals `NUMBER_OF_DAYS_TOTAL_RETENTION`, the snapshot phase is **skipped** and data flows directly from **Hot Storage → Deletion**.

### Core Features

- **Size and Age-based Rollover**: Automatically rolls over indices when they reach either the size threshold (default: 75GB) or age threshold (default: 30 days)
- **Hot Storage Management**: Configurable retention period for active indices (default: 14 days)
- **Snapshot Transition**: Moves older indices to S3-backed snapshots with comprehensive error handling
- **Searchable Snapshots**: Automatically restores missing searchable snapshots for continued querying
- **Automated Cleanup**: Three-phase cleanup process removes indices/snapshots past retention period (default: 180 days)
- **Template Management**: Syncs component and index templates from local filesystem to OpenSearch
- **Pipeline Management**: Manages ingest pipelines for document preprocessing
- **Health Monitoring**: Comprehensive monitoring of cluster health, disk space, JVM heap usage, circuit breakers, thread pools, and snapshot status with MS Teams alerts and anti-spam logic
- **Robust Error Handling**: Multi-retry logic with exponential backoff for snapshot operations
- **Comprehensive Logging**: Structured logging with configurable levels (DEBUG, INFO, WARNING, ERROR)

### Automated Operations

The system runs scheduled tasks when using `start-management` mode:
- **ILM Processing**: Daily at 1:00 AM (runs `transition_old_indices_to_snapshots`, `cleanup_old_data`, `restore_missing_searchable_snapshots`)
- **Size Rollover Checks**: Every 15 minutes (checks write indices against `ROLLOVER_SIZE_GB` and `ROLLOVER_AGE_DAYS` thresholds)
- **Template Sync**: Daily at 4:05 AM (syncs component templates and index templates)
- **Pipeline Sync**: Daily at 4:10 AM (syncs ingest pipelines from `ingest-pipelines/` directory)
- **Health Monitoring**: Every 5 minutes (cluster health, disk space, JVM heap, circuit breakers, thread pools) and daily (data snapshots, DR snapshots)

### ILM Processing Details

The main ILM process executes three critical phases in sequence:

1. **Transition Phase**: Identifies indices older than `NUMBER_OF_DAYS_ON_HOT_STORAGE` and creates snapshots, then replaces them with searchable snapshots
2. **Cleanup Phase**: Three-phase cleanup process:
   - Phase 1: Delete searchable snapshot indices older than `NUMBER_OF_DAYS_TOTAL_RETENTION`
   - Phase 2: Delete regular indices older than retention and their corresponding snapshots
   - Phase 3: Delete orphaned snapshots older than retention period
3. **Restoration Phase**: Scans for missing searchable snapshots and automatically restores them from available snapshots

⚠️ **Security Warning**: Never commit certificates to version control!

## Health Monitoring

The system includes comprehensive health monitoring with automated alerting to Microsoft Teams. The health monitor tracks cluster status, disk usage, and snapshot health to ensure system reliability.

### Monitoring Components

**Cluster Health Monitoring:**
- **Green Status**: Normal operation, no alerts
- **Yellow Status**: Only alerts after sustained 15+ minutes (prevents false alarms during brief yellow states)
- **Red Status**: Immediate critical alerts

**Disk Space Monitoring:**
- **Normal**: < 90% average usage across data nodes
- **Warning**: 90-93% average usage (MEDIUM severity)
- **Critical**: ≥ 93% average usage (HIGH severity)

**JVM Heap Memory Monitoring:**
- **Normal**: < 90% heap usage across all nodes
- **Warning**: 90-94% heap usage (MEDIUM severity)
- **Critical**: ≥ 95% heap usage (HIGH severity)

**Circuit Breaker Monitoring:**
- **Memory Usage**: Warns when any breaker reaches ≥90% of limit (MEDIUM severity)
- **Trip Detection**: Alerts on NEW circuit breaker trips only (HIGH severity)
- **Anti-Spam Logic**: Tracks previous trip counts to prevent alert flooding
- **Breaker Types**: Parent, fielddata, request, in_flight_requests, accounting

**Thread Pool Monitoring:**
- **Queue Size Warning**: 50-99 queued tasks (MEDIUM severity)  
- **Queue Size Critical**: ≥100 queued tasks (HIGH severity)
- **Rejection Alerts**: NEW thread pool rejections only (HIGH severity)
- **Anti-Spam Logic**: Tracks previous rejection counts to prevent alert flooding
- **Pool Types**: Search, write, bulk thread pools

**Snapshot Health Monitoring:**
- **Data Snapshots**: Daily checks for failed or partial snapshots
- **Disaster Recovery Snapshots**: Daily checks for failed or partial snapshots
- **Failed Snapshots**: HIGH severity alerts
- **Partial Snapshots**: MEDIUM severity alerts

### Alert Severity Levels

- **🔴 HIGH (Critical)**: Red cluster status, disk space ≥93%, JVM heap ≥95%, new circuit breaker trips, thread pool rejections, failed snapshots, system errors
- **🟡 MEDIUM (Warning)**: Yellow cluster status (15+ min), disk space 90-93%, JVM heap 90-94%, circuit breaker high usage, thread pool queue buildup, partial snapshots  
- **🟢 LOW (Info)**: Informational messages and recoveries

### Health Monitoring Schedule

**Frequent Checks (Every 5 minutes):**
- Cluster health status
- Data node disk space usage  
- JVM heap memory usage across all nodes
- Circuit breaker status and trip detection
- Thread pool queue sizes and rejection monitoring

**Daily Checks (Daily at 8:00 AM):**
- Data repository snapshot validation
- Disaster recovery snapshot validation
- Comprehensive system health report

### Anti-Spam Logic

The health monitor includes sophisticated anti-spam logic to prevent alert fatigue:

**Circuit Breaker Anti-Spam:**
- Tracks previous trip counts per node and breaker type
- Only alerts when trip count increases (NEW trips detected)
- Prevents repeated alerts for the same trip count

**Thread Pool Anti-Spam:**
- Tracks previous rejection counts per node and pool type  
- Only alerts when rejection count increases (NEW rejections detected)
- Prevents spam during sustained high load periods

This ensures that operations teams receive actionable alerts rather than noise, maintaining alert effectiveness while preventing notification fatigue.

### Teams Integration

The health monitor sends alerts to Microsoft Teams with:
- **Color-coded alerts**: Red (critical), yellow (warning), green (info)
- **Detailed context**: Cluster names, usage percentages, affected snapshots, trip counts, rejection counts
- **Grouped notifications**: Multiple warnings combined into single messages
- **Retry logic**: Automatic retry with exponential backoff for webhook failures

## Component Templates

The system uses Elastic-compatible component templates for index mapping and settings management:

- **Directory Structure**: `component-templates/VERSION/file.json` 
- **Example**: `component-templates/8.0.0/user.json`
- **Version Usage**: The version directory name is incorporated into the component template name
- **Sync Process**: Templates are automatically synchronized daily at 4:05 AM or manually with `sync-templates-now`

## Environment Variables

### Required Variables

| Variable | Description | Example |
|----------|-------------|---------|
| `URL` | OpenSearch cluster endpoint URL | `https://localhost:9200` |
| `BUCKET` | S3 bucket name for snapshot storage | `my-opensearch-snapshots` |
| `CERT_FILE_PATH` | Path to SSL/TLS certificate for authentication | `../admin.pem` |
| `KEY_FILE_PATH` | Path to private key for SSL/TLS authentication | `../admin-key.pem` |
| `REPOSITORY_DATA` | OpenSearch snapshot repository name | `data` |

### Lifecycle Configuration Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `NUMBER_OF_DAYS_ON_HOT_STORAGE` | `14` | Days to keep indices on hot storage before transitioning to snapshots |
| `NUMBER_OF_DAYS_TOTAL_RETENTION` | `180` | Total retention period in days (includes hot storage + snapshot time) |
| `ROLLOVER_SIZE_GB` | `75` | Size threshold in GB for automatic index rollover |
| `ROLLOVER_AGE_DAYS` | `30` | Age threshold in days for automatic index rollover (prevents indefinitely growing indices in low-ingest scenarios) |
| `MANAGED_INDEX_PATTERNS` | `log,alert` | Comma-separated patterns of indices to manage (e.g., `log,alert,metrics`) |

### Health Monitoring Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `HEALTH_MONITORING_ENABLED` | `false` | Enable health monitoring in `start-management` mode (`true`/`false`) |
| `HEALTH_CHECK_INTERVAL` | `300` | Interval in seconds for frequent health checks (cluster health, disk space) |
| `TEAMS_WEBHOOK_URL` | _None_ | Microsoft Teams webhook URL for health alerts (optional) |

### Optional Variables  

| Variable | Default | Description |
|----------|---------|-------------|
| `LOG_LEVEL` | `INFO` | Logging level (`DEBUG`, `INFO`, `WARNING`, `ERROR`) |

### Configuration Notes

⚠️ **Special Configuration**: If `NUMBER_OF_DAYS_ON_HOT_STORAGE` >= `NUMBER_OF_DAYS_TOTAL_RETENTION`, indices will be **deleted directly** from hot storage without creating snapshots or searchable snapshots. This configuration:
- Saves storage costs (no snapshot storage required)
- Eliminates the searchable snapshot phase entirely
- Suitable for short-term data retention scenarios

**Example Configurations:**

```bash
# Standard configuration with searchable snapshots and dual rollover
export NUMBER_OF_DAYS_ON_HOT_STORAGE="14"    # 2 weeks on hot storage
export NUMBER_OF_DAYS_TOTAL_RETENTION="180"  # 6 months total retention
export ROLLOVER_SIZE_GB="75"                 # Roll over at 75GB
export ROLLOVER_AGE_DAYS="30"                # Roll over after 30 days (prevents low-ingest issues)

# Short retention without snapshots 
export NUMBER_OF_DAYS_ON_HOT_STORAGE="30"    # 1 month on hot storage
export NUMBER_OF_DAYS_TOTAL_RETENTION="30"   # Delete directly after 1 month
export ROLLOVER_AGE_DAYS="7"                 # Roll over weekly for manageable index sizes

# High-throughput environment (frequent size-based rollover)
export ROLLOVER_SIZE_GB="50"                 # Smaller indices for faster operations
export ROLLOVER_AGE_DAYS="90"                # Longer age limit (size will trigger first)
```

## Setting Up New Index Patterns

### Understanding the Write Alias Pattern

This ILM system uses a **write alias pattern** for size-based rollover, which is different from time-based index naming. Here's why:

**Traditional Time-Based Pattern** (❌ Not compatible with this ILM):
```
log-pattern-2025.01.15
log-pattern-2025.01.16
```

**Size and Age-Based Rollover Pattern** (✅ Required for this ILM):
```
log-pattern-000001 ← Write alias: log-pattern-write
log-pattern-000002 ← Write alias moves here when 000001 reaches size OR age limit
log-pattern-000003 ← Write alias moves here when 000002 reaches size OR age limit
```

**Rollover Triggers** (either condition triggers rollover):
- **Size Condition**: Index reaches `ROLLOVER_SIZE_GB` (default: 75GB) 
- **Age Condition**: Index reaches `ROLLOVER_AGE_DAYS` (default: 30 days) since creation

### Creating Write Aliases for New Index Patterns

Before your data sources (Vector, Filebeat, etc.) can write to new index patterns, you must create the initial write alias structure using OpenSearch Dev Tools.

**Step 1**: Go to OpenSearch Dashboard → Dev Tools

**Step 2**: Create the first numbered index with write alias:

```json
# Example: Setting up audit log pattern
PUT log-opensearch-auditlog-000001
{
  "aliases": {
    "log-opensearch-auditlog-write": {
      "is_write_index": true
    }
  }
}
```

**Step 3**: Configure your data sources to write to the **write alias**, not the numbered index:

```yaml
# Vector configuration example
sinks:
  opensearch:
    type: opensearch
    inputs: [auditlog_source]
    endpoint: https://opensearch.example.com:9200
    index: log-opensearch-auditlog-write  # ← Use write alias, not numbered index
```

### Write Alias Requirements

**⚠️ Critical**: Your data sources must write to the write alias (ending in `-write`), NOT directly to numbered indices. The ILM system:

1. **Monitors Write Aliases**: Only indices with `-write` aliases are considered for rollover
2. **Automatic Rollover**: When an index reaches either `ROLLOVER_SIZE_GB` OR `ROLLOVER_AGE_DAYS`, creates new numbered index and moves write alias
3. **Seamless Transitions**: Data writing continues uninterrupted during rollover
4. **Lifecycle Management**: Only processes indices matching `MANAGED_INDEX_PATTERNS`

### Example Setups for Common Patterns

```json
# Web access logs
PUT log-nginx-access-000001
{
  "aliases": {
    "log-nginx-access-write": {
      "is_write_index": true
    }
  }
}

# Security alerts
PUT alert-security-events-000001
{
  "aliases": {
    "alert-security-events-write": {
      "is_write_index": true
    }
  }
}

# Application metrics
PUT metrics-app-performance-000001
{
  "aliases": {
    "metrics-app-performance-write": {
      "is_write_index": true
    }
  }
}
```

### Verifying Write Alias Setup

Check that your write alias is correctly configured:

```json
# Verify write alias exists and is properly configured
GET _alias/*-write

# Check specific alias
GET _alias/log-nginx-access-write
```

### Adding New Patterns to ILM Management

If you're adding patterns beyond the default `log,alert`, update the environment variable:

```bash
# Add new patterns to management
export MANAGED_INDEX_PATTERNS="log,alert,metrics,traces"
```

The ILM system will then automatically manage all indices matching these patterns.

## Available Commands

Run with: `python main.py -action <command>`

### Core ILM Operations
- **`ilm-now`**: Run complete ILM process immediately (transition → cleanup → restore)
- **`size-rollover`**: Check write indices and rollover by size or age thresholds
- **`start-management`**: Start scheduled daemon with automated operations

### Snapshot Management
- **`snapshot-list`**: List all available snapshots with status and timestamps
- **`snapshot-restore`**: Restore specific snapshot (requires `-snapshotname parameter`)
- **`snapshot-restore-latest`**: Restore most recent snapshot for disaster recovery

### Template and Pipeline Management
- **`sync-templates-now`**: Sync component and index templates from filesystem
- **`sync-ingest-pipelines-now`**: Sync ingest pipelines from `ingest-pipelines/` directory

### Health Monitoring Operations
- **`start-health-monitoring`**: Start continuous health monitoring with Teams alerts
- **`health-check-now`**: Run all health checks once and report status
- **`health-test`**: Test health monitoring system and Teams webhook connectivity

### Emergency Operations
- **`remove-searchable-snapshots`**: Emergency cleanup of all searchable snapshot indices

### Example Usage

```bash
# Run ILM process once
python main.py -action ilm-now

# Start continuous management daemon
python main.py -action start-management

# Check for size and age-based rollover
python main.py -action size-rollover

# Restore specific snapshot
python main.py -action snapshot-restore -snapshotname log-2025-01-15

# Start health monitoring daemon
python main.py -action start-health-monitoring

# Test health monitoring and Teams webhook
python main.py -action health-test

# Run health checks once
python main.py -action health-check-now

# Emergency cleanup
python main.py -action remove-searchable-snapshots
```

## Development Setup

### Python Virtual Environment

```bash
# Create virtual environment
python3 -m venv venv

# Activate virtual environment
source venv/bin/activate  # Linux/macOS
# or
venv\Scripts\activate     # Windows

# Install dependencies
pip install -r requirements.txt
```

### Running the System

```bash
# Set environment variables (see Environment Variables section above)
export URL="https://localhost:9200"
export BUCKET="my-opensearch-snapshots"
export ROLLOVER_AGE_DAYS="30"  # Optional: customize rollover age threshold
export TEAMS_WEBHOOK_URL="https://your-org.webhook.office.com/..."  # Optional
# ... other required variables

# Run using convenience scripts
./start-ilm.sh            # Run ILM process once
./start-rollover.sh       # Check for size and age-based rollover
./start-health-monitoring.sh  # Start health monitoring daemon

# Or run directly
python main.py -action ilm-now
python main.py -action start-management
python main.py -action start-health-monitoring
```

### Microsoft Teams Webhook Setup

To enable Teams alerts, create a webhook in your Teams channel:

1. **In Microsoft Teams**: Go to your desired channel → Connectors → Incoming Webhook
2. **Configure Webhook**: Provide a name and optional image
3. **Copy URL**: Copy the generated webhook URL
4. **Set Environment Variable**: `export TEAMS_WEBHOOK_URL="<your_webhook_url>"`
5. **Test Connection**: Run `python main.py -action health-test` to verify

### Testing

```bash
# Run unit tests
./start-unittests.sh

# Or run directly with unittest
python -m unittest discover test/ -v

# Run specific test file
python -m unittest test.test_ilm -v
```

## Monitoring and Troubleshooting

### Logging

The system uses structured logging with configurable levels:

```bash
# Enable debug logging for detailed information
export LOG_LEVEL="DEBUG"

# Log levels: DEBUG, INFO, WARNING, ERROR
export LOG_LEVEL="INFO"  # Default
```

### Health Checks

**Automated Health Monitoring:**
- Use `python main.py -action health-check-now` for immediate comprehensive health check
- Use `python main.py -action start-health-monitoring` for continuous monitoring with Teams alerts
- Use `python main.py -action health-test` to verify monitoring system and Teams connectivity

**Manual Health Checks:**
- OpenSearch cluster status: `GET /_cluster/health`
- Node disk usage: `GET /_cat/nodes?v&h=n,id,v,r,rp,dt,du,dup`
- Snapshot repository: `GET /_snapshot/data`  
- Write aliases: `GET /_alias/*-write`
- Recent snapshots: `GET /_cat/snapshots/data?v&s=endEpoch`
- Failed snapshots: `GET /_cat/snapshots/data?v&s=status:desc`

### Common Issues

1. **Certificate Issues**: Ensure `CERT_FILE_PATH` and `KEY_FILE_PATH` point to valid certificates
2. **S3 Permissions**: Verify S3 bucket permissions for snapshot operations
3. **Disk Space**: Monitor OpenSearch cluster disk usage during snapshot operations
4. **Write Aliases**: Ensure data sources write to `-write` aliases, not numbered indices