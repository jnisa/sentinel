# Unit Tests to the Azure Utils

from unittest import TestCase
from unittest import mock
from unittest.mock import patch, MagicMock

from azure.monitor.opentelemetry.exporter import AzureMonitorTraceExporter

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
    def test_get_kv_secret_secret_complex(self, mock_secret_client):
        kv_id = "test_kv"
        secret_name = "test_secret"

        azure_client = AzureClient(kv_id)
        mock_secret_instance = mock_secret_client.return_value
        mock_secret_instance.get_secret.side_effect = Exception(f"Secret {secret_name} does not exist.")

        with self.assertRaises(Exception) as context:
            azure_client.get_kv_secret(secret_name)

        self.assertIn("does not exist", str(context.exception))

    @mock.patch("app.utils.azure.AzureMonitorTraceExporter")
    def test_get_az_monitor_exporter_basic(self, mock_azure_monitor_trace_exporter):
        app_insights_pk = "test_pk"
        azure_client = AzureClient("test_kv")

        # Create a MagicMock instance that mimics the behavior of AzureMonitorTraceExporter
        mock_exporter_instance = MagicMock(spec=AzureMonitorTraceExporter)
        mock_azure_monitor_trace_exporter.from_connection_string.return_value = mock_exporter_instance

        actual = azure_client.get_az_monitor_exporter(app_insights_pk)

        self.assertEqual(actual, mock_exporter_instance)

    @mock.patch("app.utils.azure.AzureMonitorTraceExporter")
    def test_get_az_monitor_exporter_complex(self, mock_azure_monitor_trace_exporter):
        
        app_insights_pk = "test_pk"
        azure_client = AzureClient("test_kv")
        mock_azure_monitor_trace_exporter.from_connection_string.side_effect = Exception("test_exception")

        with self.assertRaises(Exception) as context:
            azure_client.get_az_monitor_exporter(app_insights_pk)

        actual = str(context.exception)
        expected = "Could not connect to the application insights service."

        self.assertEqual(actual, expected)