import os
import glob
import yaml
import json
import requests
from typing import Dict, Any
from settings import Settings

class IngestPipelineManager:

    """
        Class for managing ingest pipelines in OpenSearch.
    """

    def __init__(self, settings: Settings) -> None:
        self.requests = settings.get_requests_object()
        self.base_url: str = settings.url

    def _read_yaml(self, file_path: str) -> Dict[str, Any]:
        """Reads a YAML file and returns its contents as a Python dictionary."""
        with open(file_path, 'r') as file:
            return yaml.safe_load(file)

    def _upload_json(self, json_data: str, pipeline_name: str) -> int:
        """Uploads a JSON string to an OpenSearch pipeline."""
        headers = {'Content-Type': 'application/json'}
        # dry run, output to console
        print(f"Uploading pipeline {pipeline_name}")
        response = self.requests.put(f"{self.base_url}/_ingest/pipeline/{pipeline_name}", data=json_data)
        if response.status_code != requests.codes.ok:
            print('Error uploading pipeline to OpenSearch.')
            print('Status code:', response.status_code)
            print('Response text:', response.text)
        else:
            print('Pipeline uploaded successfully.')
        return response.status_code

    def sync_to_cluster(self, directory: str) -> None:
        """Processes all YAML files in the specified directory by converting and uploading them to OpenSearch."""

        print(f"Syncing ingest pipelines from {directory}")

        yml_files = glob.glob(os.path.join(directory, '*.yml'))
        for yml_file in yml_files:
            base_name = os.path.basename(yml_file)[:-4]  # Strip .yml extension
            yaml_data = self._read_yaml(yml_file)
            json_data = json.dumps(yaml_data)
            self._upload_json(json_data, base_name)