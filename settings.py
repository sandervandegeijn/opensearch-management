import requests
from typing import Tuple

class Settings:

    def __init__(self, url: str, bucket: str, cert_file_path: str, key_file_path: str, number_of_days_on_hot_storage: int, number_of_days_total_retention: int, repository: str, rollover_size_gb: int = 50, rollover_age_days: int = 30, managed_index_patterns: Tuple[str, ...] = ("log", "alert")) -> None:
        self.url: str = url
        self.bucket: str = bucket
        self.number_of_days_on_hot_storage: int = number_of_days_on_hot_storage
        self.number_of_days_total_retention: int = number_of_days_total_retention
        self.cert_file_path: str = cert_file_path
        self.key_file_path: str = key_file_path
        self.repository: str = repository
        self.rollover_size_gb: int = rollover_size_gb
        self.rollover_age_days: int = rollover_age_days
        self.managed_index_patterns: Tuple[str, ...] = managed_index_patterns
    
    def get_requests_object(self) -> requests.Session:
        cert: Tuple[str, str] = (self.cert_file_path, self.key_file_path)
        s: requests.Session = requests.Session()
        s.cert = cert
        s.verify = False
        s.headers = {"content-type": "application/json", 'charset':'UTF-8'}
        return s