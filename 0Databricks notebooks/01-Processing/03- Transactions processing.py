# Databricks notebook source
from pyspark.sql.types import StructField, StructType, IntegerType, DecimalType, StringType, DateType, BooleanType, ArrayType, IntegralType
from pyspark.sql.functions import from_json, col, cast, explode, concat, to_timestamp, unix_timestamp, to_date, regexp_replace
from pyspark.sql.window import Window
from datetime import datetime

# COMMAND ----------

inner_transactions_schema = \
    StructType(
        fields= [
            StructField("date",StringType(), True),
            StructField("amount", IntegerType(), True),
            StructField("transaction_code", StringType(), True),
            StructField("symbol", StringType(), True),
            StructField("price", DecimalType(10, 2), True),
            StructField("total", DecimalType(10, 2), True)
        ]
    )

data_schema = \
    StructType(
        fields=[
            StructField("account_id", IntegerType(), True),
            StructField("transaction_count", IntegerType(), True),
            StructField("bucket_start_date", StringType(), True),
            StructField("bucket_end_date", StringType(), True),
            StructField("transactions", ArrayType(inner_transactions_schema), True)
        ]
    )

transactions_schema = \
    StructType(
        fields=[
            StructField("_id", StringType(), True),
            StructField("_fivetran_synced", StringType(), True),
            StructField("_fivetran_deleted", BooleanType(), True),
            StructField("data", data_schema, True)
        ]
    )


# COMMAND ----------

last_ingest_date = spark.sql(
    """
    SELECT MAX(ingest_date) as max
    FROM processed.ct.transaction
    """
).first()["max"]

if last_ingest_date == None:
    last_ingest_date = "1900-01-01T00:00:00"
else:
    last_ingest_date = last_ingest_date.isoformat()


# COMMAND ----------

print(last_ingest_date)

# COMMAND ----------


df = spark.read.load("/mnt/customertransaction/raw/fivetran/mongo_trans/transactions").filter(col("_fivetran_synced") > last_ingest_date)
display(df)

# COMMAND ----------

df_defined = df.withColumn("data", from_json("data", data_schema))


# COMMAND ----------

df_exploeded = df_defined.withColumn("transactions", explode(col("data.transactions")))


# COMMAND ----------

df_final = df_exploeded.select(
    concat(col("data.account_id"), unix_timestamp(col("transactions.date"), "yyyy-MM-dd'T'HH:mm:ss'Z'")).cast("bigint").alias("transaction_id"),
    col("data.account_id").alias("account_id"),
    to_date(regexp_replace(regexp_replace("transactions.date", "Z", "",), "T", " "), "yyyy-MM-dd HH:mm:ss").alias("transaction_date"),
    col("transactions.transaction_code").alias("transaction_type"),
    col("transactions.symbol").alias("symbol"),
    col("transactions.amount").alias("amount"),
    col("transactions.price").alias("unit_price"),
    col("transactions.total").alias("total_price"),
    to_date(regexp_replace(regexp_replace("data.bucket_start_date", "Z", "",), "T", " "), "yyyy-MM-dd HH:mm:ss").alias("bucket_start_date"),
     to_date(regexp_replace(regexp_replace("data.bucket_end_date", "Z", "",), "T", " "), "yyyy-MM-dd HH:mm:ss").alias("bucket_end_date"),
    col("data.transaction_count").alias("transaction_count"),
    col("_fivetran_synced").alias("ingest_date")

    ).filter("transaction_id is not null")


# COMMAND ----------

df_final\
    .write\
    .mode("append")\
    .format("delta")\
    .option("mergeSchema", "true")\
    .saveAsTable("processed.ct.transaction")