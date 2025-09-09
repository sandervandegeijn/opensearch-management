import json
import os
import glob
import requests
from typing import Dict, Any, Optional

# create enum for template types
from enum import Enum
from settings import Settings

class TemplateType(Enum):
    INDEX_TEMPLATE = 1
    COMPONENT_TEMPLATE = 2

class TemplateManager:
    def __init__(self, settings: Settings) -> None:
        self.requests = settings.get_requests_object()
        self.base_url: str = settings.url

    def _read_json(self, file_path: str) -> Dict[str, Any]:
        """Reads a JSON file and returns its contents as a Python dictionary."""
        with open(file_path, 'r') as file:
            return json.load(file)

    def _upload_json(self, json_data: Dict[str, Any], template_name: str, template_type: TemplateType, version: Optional[str] = None) -> int:
        print(f"Uploading template {template_name}")
        
        if template_type == TemplateType.INDEX_TEMPLATE:
            print(f"Uploading index template {template_name}")
            response = self.requests.put(f"{self.base_url}/_index_template/{template_name}", json=json_data)
        elif template_type == TemplateType.COMPONENT_TEMPLATE:
            if version is None:
                raise ValueError("Version is required for component templates")
            upload_template_name = f"ecs_{version}_{template_name}"
            response = self.requests.put(f"{self.base_url}/_component_template/{upload_template_name}", json=json_data)
        
        if response.status_code != requests.codes.ok:
            print(f'Error uploading template {template_name} to OpenSearch.')
            print('Status code:', response.status_code)
            print('Response text:', response.text)
        else:
            print(f'Template {template_name} uploaded successfully.')
        return response.status_code

    def sync_to_cluster(self, template_directory: str, template_type: TemplateType, version: Optional[str] = None) -> None:
        """Processes all JSON files in the specified directory by uploading them to OpenSearch."""

        print(f"Syncing index templates from {template_directory}")

        json_files = glob.glob(os.path.join(template_directory, '*.json'))
        for json_file in json_files:
            base_name = os.path.basename(json_file)[:-5]  # Strip .json extension
            json_data = self._read_json(json_file)
            self._upload_json(json_data, base_name, template_type, version)