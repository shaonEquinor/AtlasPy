def getSecret(secretName, kvName=None):
	'''
	function for Initiating all variables for the connection, retriving Secrets from Key Vault
	Input:
		secretName: SecretName in KeyVault
		eks : KeyVaultConnection().getSecret("secretname")
	'''

	from pyspark.sql.session import SparkSession
	from azure.keyvault.secrets import SecretClient
	from azure.identity import DefaultAzureCredential
	from pyspark.dbutils import DBUtils
	import os

	spark = SparkSession.builder.getOrCreate()
	dbutils = DBUtils(spark)

	os.environ["AZURE_CLIENT_ID"] = dbutils.secrets.get("cmn-databricks-sp-scope", "clientid")
	os.environ["AZURE_TENANT_ID"] = dbutils.secrets.get("cmn-databricks-sp-scope", "tenantid")
	os.environ["AZURE_CLIENT_SECRET"] = dbutils.secrets.get("cmn-databricks-sp-scope", "secret")

	keyVaultName = dbutils.secrets.get(scope='env-secrets-scope', key="saas-kv-name")
	KVUri = f"https://{keyVaultName}.vault.azure.net"

	# Create a SecretClient using default Azure credentials
	credential = DefaultAzureCredential()
	client = SecretClient(vault_url=KVUri, credential=credential)
	secret = client.get_secret(secretName)
	return secret.value
