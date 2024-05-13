# Databricks notebook source
from pyspark.sql.functions import col, array_contains, when, count, col, lit, current_date

# COMMAND ----------

df= spark.sql(
  """
  SELECT * FROM processed.ct.account
  """
)


# COMMAND ----------

df_defined = df\
    .withColumn("has_investment_stock", array_contains(col("account.products"), "InvestmentStock"))\
    .withColumn("has_brokerage", array_contains(col("account.products"), "Brokerage"))\
    .withColumn("has_commodity", array_contains(col("account.products"), "Commodity"))\
    .withColumn("has_investment_fund", array_contains(col("account.products"), "InvestmentFund"))\
    .withColumn("has_currency_service", array_contains(col("account.products"), "CurrencyService"))\
    .withColumn("has_dervatives", array_contains(col("account.products"), "has_dervatives"))\
    .select(col("account_id"), col("limit"), col("has_investment_stock"), col("has_brokerage"), col("has_commodity"),
            col("has_investment_fund"), col("has_currency_service"), col("has_dervatives")
            )
    

# COMMAND ----------

df_updated = df_defined\
    .withColumn("start_date", lit(current_date()))\
    .withColumn("end_date", lit(None))\
    .withColumn("is_current", lit(True))\
    .withColumn("source_system", lit("mongoDB_1"))

df_current = spark.sql(
    """
    SELECT * EXCEPT (account_sk)
    FROM presentation.ct.dim_account
    WHERE is_current = 1
    """
)

if df_current.count() == 0:
    df_final = df_updated
    df_final.createOrReplaceTempView("source_data")
else:
    df_upd_j_cur = df_updated.alias("upd").join(df_current.alias("cur"), df_updated.account_id == df_current.account_id, "outer")

    df_hist_records = df_upd_j_cur\
        .filter(col("cur.account_id").isNotNull())\
        .filter(
            (col("upd.has_investment_stock") != col("cur.has_investment_stock")) |
            (col("upd.has_brokerage") != col("cur.has_brokerage")) |
            (col("upd.has_commodity") != col("cur.has_commodity")) |
            (col("upd.has_investment_fund") != col("cur.has_investment_fund")) |
            (col("upd.has_currency_service") != col("cur.has_currency_service")) |
            (col("upd.has_dervatives") != col("cur.has_dervatives"))
            )\
        .select("cur.*")\
        .withColumn("is_current", lit(False))\
        .withColumn("end_date", lit(current_date()))
        

    id_list = [row.account_id for row in df_hist_records.select("cur.account_id").collect()]

    df_new_records = df_upd_j_cur\
        .withColumn("upd.start_date", when((~col("upd.account_id").isin(id_list)) & (col("cur.start_date") != 'null') , col("cur.start_date")))\
        .select("upd.*")
    df_final = df_new_records.union(df_hist_records)

    df_final.createOrReplaceTempView("source_data")


# COMMAND ----------

display(df_updated.filter(col("account_id") == 627788))

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO presentation.ct.dim_account AS trgt
# MAGIC USING source_data AS src
# MAGIC ON trgt.account_id = src.account_id AND src.is_current = 1
# MAGIC WHEN MATCHED THEN UPDATE 
# MAGIC SET
# MAGIC   account_id = src.account_id,
# MAGIC   limit = src.limit,
# MAGIC   has_investment_stock = src.has_investment_stock,
# MAGIC   has_brokerage = src.has_brokerage,
# MAGIC   has_commodity = src.has_commodity,
# MAGIC   has_investment_fund = src.has_investment_fund,
# MAGIC   has_currency_service = src.has_currency_service,
# MAGIC   has_dervatives = src.has_dervatives,
# MAGIC   start_date = src.start_date,
# MAGIC   end_date = src.end_date,
# MAGIC   is_current = src.is_current,
# MAGIC   source_system = src.source_system
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT(
# MAGIC   account_id,
# MAGIC   limit,
# MAGIC   has_investment_stock,
# MAGIC   has_brokerage,
# MAGIC   has_commodity,
# MAGIC   has_investment_fund,
# MAGIC   has_currency_service,
# MAGIC   has_dervatives,
# MAGIC   start_date,
# MAGIC   end_date,
# MAGIC   is_current,
# MAGIC   source_system
# MAGIC )
# MAGIC VALUES(
# MAGIC   src.account_id,
# MAGIC   src.limit,
# MAGIC   src.has_investment_stock,
# MAGIC   src.has_brokerage,
# MAGIC   src.has_commodity,
# MAGIC   src.has_investment_fund,
# MAGIC   src.has_currency_service,
# MAGIC   src.has_dervatives,
# MAGIC   src.start_date,
# MAGIC   src.end_date,
# MAGIC   src.is_current,
# MAGIC   src.source_system
# MAGIC )
# MAGIC
# MAGIC