# Unit Tests to the Azure Utils

from unittest import TestCase
from unittest import mock
from unittest.mock import patch, MagicMock

from app.utils.azure import AzureClient

class TestAzureClient(TestCase):
    
    @patch("app.utils.azure.DefaultAzureCredential")
    def test_azure_client__init__(self, mock_default_azure_credential):

        kv_id = "test_kv"

        azure_client = AzureClient(kv_id)
        mock_default_azure_credential.return_value = MagicMock()
        
        expected = f"https://test_kv.vault.azure.net"
        actual = azure_client.kv_client.vault_url

        self.assertEqual(actual, expected)

    @mock.patch("app.utils.azure.SecretClient")
    def test_get_kv_secret_basic(self, mock_secret_client):
        kv_id = "test_kv"
        secret_name = "test_secret"

        azure_client = AzureClient(kv_id)
        mock_secret_instance = mock_secret_client.return_value
        mock_secret_instance.get_secret.return_value.value = "test_value"

        expected = "test_value"
        actual = azure_client.get_kv_secret(secret_name)

        self.assertEqual(actual, expected)
    
    @mock.patch("app.utils.azure.SecretClient")
    def test_get_kv_secret_secret_not_exist(self, mock_secret_client):
        kv_id = "test_kv"
        secret_name = "test_secret"

        azure_client = AzureClient(kv_id)
        mock_secret_instance = mock_secret_client.return_value
        mock_secret_instance.get_secret.side_effect = Exception(f"Secret {secret_name} does not exist.")

        with self.assertRaises(Exception) as context:
            azure_client.get_kv_secret(secret_name)

        self.assertIn("does not exist", str(context.exception))

    def test_get_az_monitor_exporter_basic(self):
        pass

    def test_get_az_monitor_exporter_complex(self):
        pass
