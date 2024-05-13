# Databricks notebook source
from pyspark.sql.functions import lit

# COMMAND ----------

df_transactions_details = spark.sql(
    """
    SELECT typ.transaction_type, sym.symbol
    FROM 
        (SELECT DISTINCT transaction_type FROM processed.ct.transaction) AS typ 
        CROSS JOIN
        (SELECT DISTINCT symbol FROM processed.ct.transaction) AS sym
    """
)
df_final = df_transactions_details.withColumn("source_system", lit('mongoDB_1'))
df_final.createOrReplaceTempView("source_data")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO presentation.ct.dim_transaction_details AS trgt
# MAGIC USING source_data AS src
# MAGIC ON trgt.transaction_type = src.transaction_type AND
# MAGIC    trgt.symbol = src.symbol
# MAGIC WHEN MATCHED THEN UPDATE 
# MAGIC SET 
# MAGIC   transaction_type = src.transaction_type,
# MAGIC   symbol = src.symbol,
# MAGIC   source_system = src.source_system
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT(
# MAGIC   transaction_type,
# MAGIC   symbol,
# MAGIC   source_system
# MAGIC )
# MAGIC VALUES(
# MAGIC   src.transaction_type,
# MAGIC   src.symbol,
# MAGIC   src.source_system
# MAGIC )
# MAGIC