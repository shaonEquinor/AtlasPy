class SQLOnDemand():
	'''
	class used for interacting with Legacy SQL Server
	'''

	def __init__(self, path):
		'''
		Initiating all variables for the connection.
		'''
		from ..keyvault import getSecret
		from azure.identity import DefaultAzureCredential
		from pyspark.sql.session import SparkSession

		self.path = path
		self.spark = SparkSession.builder.getOrCreate()
		self.server = getSecret("atlas-synapse-sql-ondemand-endpoint")
		self.resource_app_id_url = "https://database.windows.net/.default"
		self.default_credential = DefaultAzureCredential()
		self.database = "atlas_ondemand"
		self.encrypt = "true"
		self.azure_sql_url = 'jdbc:sqlserver://' + self.server
		self.host_name_in_certificate = "*.database.windows.net"
		self.port = "1433"
		self.sql_url = "jdbc:sqlserver://{0}:{1};database={2};".format(self.server, self.port, self.database)

		typeMap = {'double': 'float', 'string': 'varchar(1000)', 'int': 'int', 'timestamp': 'datetime2(3)',
		           'datetime': 'datetime2(3)', 'date':'DATE', 'time':'TIME(7)'}
		df = self.spark.read.load(self.path)
		self.schema = {}
		for colname, coltype in df.dtypes:
			ncoltype = 'varchar(1000)'
			try:
				ncoltype = typeMap[coltype].upper()
			except Exception as e:
				print(f"Could not autodetect {coltype} type, setting {colname} to VARCHAR(1000)")
			finally:
				self.schema[colname] = ncoltype



	def __get_token(self) -> str:
		'''
		Azquires the access token towards the databse
		:returns:
			Access token
		:rtype:
			str
		'''

		token_response = self.default_credential.get_token(self.resource_app_id_url)
		if len(token_response) > 1:
			return token_response.token
		else:
			raise Exception('Access token could not be acquired')

	# Query Synaps
	def __query_table(self, query):
		# Get Access token
		access_token = self.__get_token()

		try:
			props = self.spark._sc._gateway.jvm.java.util.Properties()
			props.putAll({"accessToken": access_token})
			connection = self.spark._sc._gateway.jvm.java.sql.DriverManager.getConnection(self.sql_url, props)

			connection.prepareCall(query).execute()
			connection.commit()
			# Close Connection
			connection.close()
		except Exception as e:
			connection.close()
			raise  # Pass exception onwards for the caller to handle.

	# Read table from Synaps
	def read_table(self, table_name: str):
		'''
		Query the table or view and puts it into a Dataframe
		'''

		# Get Access token
		access_token = self.__get_token()

		# Access the data table or views
		df = self.spark.read \
			.format("com.microsoft.sqlserver.jdbc.spark") \
			.option("url", self.azure_sql_url) \
			.option("dbtable", table_name) \
			.option("databasename", self.database) \
			.option("accessToken", access_token) \
			.option("encrypt", self.encrypt) \
			.option("hostNameInCertificate", self.host_name_in_certificate) \
			.load()
		return df

	def get_schema(self):
		return self.schema

	def set_schema(self, new_schema):
		self.schema = new_schema

	def create_view(self, viewname, dataformat):
		path = self.path

		if self.path.startswith("/mnt/"):
			path = self.path.replace("/mnt/", "")

		if self.path.startswith("mnt/"):
			path = self.path.replace("mnt/", "")

		schema = 'with(' + ', '.join([f"{colname} {coltype.upper()}" for colname, coltype in self.schema.items()]) + ')'
		sql_query = f"execute [dbo].[proc_set_synapse_view] @viewName='{viewname}', @filePathRelative='{path}', @type='{dataformat}', @schema='{schema}'; "
		print("Running Query")
		print(sql_query)
		self.__query_table(sql_query)
