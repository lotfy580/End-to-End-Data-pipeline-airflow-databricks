# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, ArrayType, BooleanType, MapType
from pyspark.sql.functions import from_json, col, explode, count, when, to_date, regexp_replace, lit

# COMMAND ----------

tier_schema = \
    MapType(StringType(), 
            StructType(
                fields=[
                    StructField("tier", StringType(), True),
                    StructField("benefits", ArrayType(StringType()), True),
                    StructField("active", BooleanType(), True)
                ]
            )
    )   
        


data_schema = \
    StructType(
        fields= [
            StructField("username", StringType(), True),
            StructField("name", StringType(), True),
            StructField("address", StringType(), True),
            StructField("birthdate", StringType()),
            StructField("email", StringType(), True),
            StructField("active", StringType(), True),
            StructField("accounts", ArrayType(IntegerType()), True),
            StructField("tier_and_details", tier_schema, True)
        ]
    )



# COMMAND ----------

df = spark.read.load("/mnt/customertransaction/raw/fivetran/mongo_trans/customers")
#display(df.select("_id").distinct())

# COMMAND ----------

df_defined = df\
    .withColumn("data", from_json(col("data"), data_schema)) \
    .withColumn("account_id", explode("data.accounts"))

df_with_tier = df_defined.select("_id", explode("data.tier_and_details").alias("key", "tier_details"))

df_join_tier = df_defined.alias("d").join(df_with_tier.alias("t"), df_defined._id == df_with_tier._id, "outer")



# COMMAND ----------


df_final=df_join_tier.select(
        col("d._id").alias("id"),
        col("data.username").alias("user_name"),
        col("data.name").alias("name"),
        col("data.address").alias("address"),
        to_date(regexp_replace(regexp_replace("data.birthdate", "Z", "",), "T", " "), "yyyy-MM-dd HH:mm:ss").alias("birth_date"),
        col("data.email").alias("email"),
        col("data.active").alias("active_customer"),
        col('account_id').alias("account_id"),
        col("tier_details.tier").alias("tier"),
        col("tier_details.benefits").alias("benefits"),
        col("tier_details.active").alias("tier_active"),
        col("_fivetran_synced").alias("ingest_date")
        )


# COMMAND ----------

df_final\
    .write\
    .mode('overwrite')\
    .format("delta")\
    .option("overwriteSchema", "true")\
    .saveAsTable("processed.ct.customer")