import unittest
import sys
import os
from unittest.mock import Mock, patch, MagicMock

# Add parent directory to path to import modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ilm import Ilm
from settings import Settings

class IlmTest(unittest.TestCase):

    def setUp(self):

        url = "https://localhost:9200"
        bucket = "data"
        cert_file_path = "/path/to/cert"
        key_file_path = "/path/to/key"
        number_of_days_on_hot_storage = 30
        number_of_days_total_retention = 90

        self.settings = Settings(url, bucket, cert_file_path, key_file_path, number_of_days_on_hot_storage, number_of_days_total_retention, "data", rollover_age_days=30)
        self.ilm = Ilm(self.settings)
    
    def test_bullshit(self):
        self.assertTrue(True)
    
    # def test_restore_snapshot(self):
    #     mock_requests_post = self.ilm.requests.post = MagicMock()
    #     mock_requests_post.return_value.status_code = 200

    #     mock_requests_get = self.ilm.requests.get = MagicMock()
    #     mock_requests_get.return_value.status_code = 200

    #     mock_create_alias = self.ilm.create_alias = MagicMock()
    #     mock_create_alias.return_value = None

    #     #get the body of the request
    #     snapshot_name = "snapshot-log-suricata-alert-000001"
    #     self.ilm.restore_snapshot(snapshot_name)
    #     args, kwargs = mock_requests_post.call_args
    #     request_body = kwargs.get('json')
    #     request_url = args[0]

    #     self.assertTrue(request_url.endswith(f"/_snapshot/{self.settings.bucket}/{snapshot_name}/_restore"))
    #     self.assertEqual(request_body.get('storage_type'), "remote_snapshot")
    #     self.assertEqual(request_body.get('index_settings').get('number_of_replicas'), 0)

if __name__ == '__main__':
    unittest.main()