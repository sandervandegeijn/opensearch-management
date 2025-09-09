#!/bin/bash

# Set environment variables
export URL="https://localhost:9200"
export BUCKET="your-opensearch-data-bucket"
export CERT_FILE_PATH="../admin.pem"
export KEY_FILE_PATH="../admin-key.pem"
export NUMBER_OF_DAYS_ON_HOT_STORAGE="14"
export NUMBER_OF_DAYS_TOTAL_RETENTION="180"
export REPOSITORY_DATA="data"

# Define the action (default to "ilm" if not provided as an argument)
ACTION="remove-remote-backed-indices"
#ACTION="remove-remote-backed-indices"

# Execute the Python script with the provided action
python3 main.py \
  -action "$ACTION" \

