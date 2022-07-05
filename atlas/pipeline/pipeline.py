def process_ingest(ingest_path, raw_path, primary_keys=None, data_format='csv', process_func=None, delimiter=","):
    import os
    from pyspark.sql.session import SparkSession
    from pyspark.sql.functions import to_timestamp, current_timestamp
    from delta import DeltaTable

    spark = SparkSession.builder.getOrCreate()
    spark.sql("SET spark.databricks.delta.formatCheck.enabled = false")

    df = spark.readStream.format('cloudFiles') \
        .option("cloudFiles.schemaLocation", os.path.join(raw_path, '_meta/schema')) \
        .option("cloudFiles.inferColumnTypes", True) \
        .option('cloudFiles.format', data_format) \
        .option("delimiter", delimiter) \
        .load(ingest_path, header=True)

    if primary_keys is None:
        primary_keys = list(filter(lambda x: x.startswith('_') is False, df.columns))
        print("Primary keys not specified. using")
        print(primary_keys)
        print("columns to deduplicate")
    df = df.dropDuplicates(primary_keys)

    def normalize_colname(colname):
        illegal_chars = '<>*#,.%&;:\\+?/'
        name = ''.join(char for char in colname if char not in illegal_chars).strip()
        name = name.replace(" ", "_").replace("-", "_")
        return name

    df = df.withColumn('etl_createat', to_timestamp(current_timestamp()))

    if process_func is not None:
        df = process_func(df)

    for colname in df.columns:
        normalized_colname = normalize_colname(colname)
        if normalized_colname != colname:
            df = df.withColumnRenamed(colname, normalized_colname)
            print(f"Renamed column {colname} to {normalized_colname}")

    if not DeltaTable.isDeltaTable(spark, raw_path):
        emptyRDD = spark.sparkContext.emptyRDD()
        tdf = spark.createDataFrame(emptyRDD, df.schema)
        tdf.write.format('delta').save(raw_path)

    match_cond = " and ".join([f"data.{keystring} = newData.{keystring}" for keystring in primary_keys])

    def batch_func(batchDf, _):
        deltaTable = DeltaTable.forPath(spark, raw_path)
        (deltaTable.alias("data")
         .merge(batchDf.alias("newData"), match_cond)
         .whenNotMatchedInsertAll()
         .execute())

    streamHandler = (df.writeStream
                     .format("delta")
                     .outputMode("append")
                     .foreachBatch(batch_func)
                     .option("checkpointLocation", os.path.join(raw_path, '_meta/checkpoint'))
                     .option("cloudFiles.schemaEvolutionMode", "rescue")
                     .option("cloudFiles.schemaLocation", os.path.join(raw_path, '_meta/schema'))
                     .trigger(once=True)
                     .start())

    return streamHandler
