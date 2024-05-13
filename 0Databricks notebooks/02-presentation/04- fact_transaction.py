# Databricks notebook source
from datetime import datetime
from pyspark.sql.functions import col, regexp_replace, current_date, lit
from pyspark.sql.types import IntegerType

# COMMAND ----------

last_transform_date = spark.sql(
    """
        SELECT max(create_date) AS max
        FROM presentation.ct.fact_transaction;
    """
).first()["max"]

if last_transform_date == None:
    last_transform_date = "1900-01-01T00:00:00"
else:
    last_transform_date = last_transform_date.isoformat()

# COMMAND ----------

df_new_transactions = spark.sql(
    """
        SELECT *
        FROM processed.ct.transaction
        WHERE 
            transaction_id IS NOT NULL
            AND create_date > '{}';
    """.format(last_transform_date)
)


df_customer_account_lookup = spark.sql(
    """
        SELECT DISTINCT id AS customer_bk, account_id
        FROM processed.ct.customer;
    """
)

df_customer_sk_lookup = spark.sql(
    """
        SELECT customer_sk, customer_bk
        FROM presentation.ct.dim_customer
        WHERE is_current = 1;
    """
)

df_account_sk_lookup = spark.sql(
    """
        SELECT account_sk, account_id
        FROM presentation.ct.dim_account
        WHERE is_current = 1;
    """
)

df_transaction_details_lookup = spark.sql(
    """
        SELECT transaction_details_pk, transaction_type, symbol
        FROM presentation.ct.dim_transaction_details
    """
)

# COMMAND ----------

df_new_transactions_lookup = df_new_transactions.alias("nt")\
    .join(df_customer_account_lookup, df_new_transactions.account_id == df_customer_account_lookup.account_id, "left")\
    .join(df_customer_sk_lookup.alias("c"), df_customer_account_lookup.customer_bk == df_customer_sk_lookup.customer_bk, "left")\
    .join(df_account_sk_lookup, df_new_transactions.account_id == df_account_sk_lookup.account_id, "left")\
    .join(df_transaction_details_lookup.alias("td"), 
          (df_new_transactions.transaction_type == df_transaction_details_lookup.transaction_type) &
          (df_new_transactions.symbol == df_transaction_details_lookup.symbol), "left")\
    .select(
        col("transaction_id"), 
        col("c.customer_sk").alias("customer_fk"),
        col("account_sk").alias("account_fk"),
        col("td.transaction_details_pk").alias("trancation_details_fk"),
        regexp_replace(col("transaction_date"), "-", "").cast(IntegerType()).alias("transaction_date_fk"),
        col("nt.account_id").alias("account_bk"),
        col("c.customer_bk").alias("customer_bk"),
        col("amount"),
        col("unit_price")
        )\
    .withColumn("create_date", lit(current_date()))
    
    
df_new_transactions_lookup.createOrReplaceTempView("new_transactions")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC INSERT INTO presentation.ct.fact_transaction(
# MAGIC     transaction_bk,
# MAGIC     customer_fk,
# MAGIC     account_fk,
# MAGIC     trancation_details_fk,
# MAGIC     transaction_date_fk,
# MAGIC     account_bk,
# MAGIC     customer_bk,
# MAGIC     amount,
# MAGIC     unit_price,
# MAGIC     create_date
# MAGIC     )
# MAGIC SELECT *
# MAGIC FROM new_transactions    
# MAGIC