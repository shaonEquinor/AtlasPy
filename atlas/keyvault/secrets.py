def getSecret(secretName, kvName=None):
	'''
	function for Initiating all variables for the connection, retriving Secrets from Key Vault
	Input:
		secretName: SecretName in KeyVault
		eks : KeyVaultConnection().getSecret("secretname")
	'''
	from azure.keyvault.secrets import SecretClient
	from azure.identity import DefaultAzureCredential
	if kvName is None:
		self.keyVaultName = dbutils.secrets.get(scope='env-secrets-scope', key="saas-kv-name")
	else:
		self.keyVaultName = kvName
	self.KVUri = f"https://{self.keyVaultName}.vault.azure.net"
	# Create a SecretClient using default Azure credentials
	self.credential = DefaultAzureCredential()
	self.client = SecretClient(vault_url=self.KVUri, credential=self.credential)
	secret = self.client.get_secret(secretName)
	return secret.value
