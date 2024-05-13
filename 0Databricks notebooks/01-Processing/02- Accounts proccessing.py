# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, ArrayType, BooleanType, MapType
from pyspark.sql.functions import from_json, col, explode, count, when

# COMMAND ----------

df= spark.read.load("/mnt/customertransaction/raw/fivetran/mongo_trans/accounts")

# COMMAND ----------

data_schema = \
    StructType(
        fields=[
            StructField("account_id", IntegerType(), True),
            StructField("limit", IntegerType(), True),
            StructField("products", ArrayType(StringType()), True)
        ]
    )

# COMMAND ----------

df_defined = df.withColumn("data", from_json("data", data_schema))

# COMMAND ----------

df_final = \
    df_defined\
        .select(
            col("_id").alias("id"),
            col("data.account_id").alias("account_id"),
            col("data.limit").alias("limit"),
            col("data.products").alias("products"),
            col("_fivetran_synced").alias("ingest_date")
        ).dropDuplicates(["account_id"])

# COMMAND ----------

df_final\
    .write\
    .mode("overwrite")\
    .format("delta")\
    .saveAsTable("processed.ct.account")