# Functions to interact with Azure services

class AzureClient:
    """
    Class that will contain multiple functions to interact with some of the Azure services 
    that we use throughout our platform.

    Up to this point, the functions that we will be using are:
    1. get secrets from Azure Key Vault;
    """

    def __init__(self) -> None:
        """
        Initialize the AzureClient class.
        """

        pass

    # TODO. there's probably internal methods that should be added to this class
    # to setup the connection to the Azure services.

    def get_kv_secret(secret_name: str) -> str:
        """
        Retrive the value of a secret from an Azure Key Vault secret.

        Additionally, this function will raise an exception if the secret - provided to this
        function - does not exist.  

        :param secret_name: name of the azure key vault secret
        :return: the value of the secret 
        """
        
        return None