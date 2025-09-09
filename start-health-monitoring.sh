#!/bin/bash

# Set environment variables
export URL="https://localhost:9200"
export BUCKET="your-opensearch-data-bucket"
export CERT_FILE_PATH="../admin.pem"
export KEY_FILE_PATH="../admin-key.pem"
export NUMBER_OF_DAYS_ON_HOT_STORAGE="14"
export NUMBER_OF_DAYS_TOTAL_RETENTION="180"
export REPOSITORY_DATA="data"
export HEALTH_MONITORING_ENABLED="true"
export HEALTH_CHECK_INTERVAL="10"
export LOG_LEVEL="DEBUG"
# export TEAMS_WEBHOOK_URL="https://your-teams-webhook-url"

# Define the action - runs health monitoring only (for testing)
ACTION="start-health-monitoring"

# Execute the Python script with the provided action
python3 main.py \
  -action "$ACTION" \