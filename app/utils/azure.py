# Functions to interact with Azure services

from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential
from azure.monitor.opentelemetry.exporter import AzureMonitorTraceExporter


class AzureClient:
    """
    Class that will contain multiple functions to interact with some of the Azure services 
    that are leveraged by the platorm running in this cloud provider.

    Up to this point, the functions that we will be using are:
    1. get secrets from Azure Key Vault;
    2. create a credentials object to be used by the Azure Monitor Trace Exporter.
    3. setup the Azure Monitor Trace Exporter.

    TODO. consider the expansion of this module to encompass the following capabilities:
    1. set an external consumer and/or producer Azure Event Hub;
    2. connection to the application insights service;
    """

    def __init__(self, kv_id: str):
        """
        Initialize the AzureClient class.

        :param kv_id: id of the Azure Key Vault
        """

        kv_url = f"https://{kv_id}.vault.azure.net"
        self._credential = DefaultAzureCredential()

        self.kv_client = SecretClient(vault_url = kv_url, credential = self._credential)

    @property
    def credential(self) -> DefaultAzureCredential:
        """
        Retrieve the credential under usage.
        """

        return self._credential

    def get_kv_secret(self, secret_name: str) -> str:
        """
        Retrive the value of a secret from an Azure Key Vault secret.

        Additionally, this function will raise an exception if the secret - provided to this
        function - does not exist.  

        :param secret_name: name of the azure key vault secret
        :return: the value of the secret 
        """
        
        try:
            return self.kv_client.get_secret(secret_name).value
        except:
            raise Exception(f"Secret {secret_name} does not exist.")

    def get_az_monitor_exporter(self, app_insights_pk: str) -> AzureMonitorTraceExporter:
        """
        Create an instance of the Azure Monitor Trace Exporter.

        The connection to the application insights service is done through the primary key.

        :param app_insights_pk: the primary key of the application insights service
        :return: an instance of the Azure Monitor Trace Exporter
        """

        try:
            return AzureMonitorTraceExporter.from_connection_string(
                connection_string = f"InstrumentationKey={app_insights_pk}"
            )
        except:
            raise Exception("Could not connect to the application insights service.")