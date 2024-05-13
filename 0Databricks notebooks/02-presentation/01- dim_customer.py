# Databricks notebook source
from pyspark.sql.functions import col, explode, when, count, col, lit, current_date
from pyspark.sql.types import BooleanType, IntegerType

# COMMAND ----------

df= spark.sql(
  """
  SELECT * FROM processed.ct.customer
  """
)

# COMMAND ----------

df_with_benefits = df.withColumn("benefit", explode("benefits"))
df_joined = df.alias("d").join(df_with_benefits.alias("b"), df.id == df_with_benefits.id, "outer")



# COMMAND ----------

df_grouped = df_joined\
    .groupby(col("d.id").alias("customer_bk"), col("d.user_name"), col("d.name"), col("d.address"), col("d.birth_date"), col("d.email"), col("d.active_customer").cast(BooleanType()))\
    .agg(
        when(count(when(col("d.tier") == "Bronze", True)) > 0, True).otherwise(False).alias("bronze_tier"),
        when(count(when(col("d.tier") == "Silver", True)) > 0, True).otherwise(False).alias("silver_tier"),
        when(count(when(col("d.tier") == "Gold", True)) > 0, True).otherwise(False).alias("gold_tier"),
        when(count(when(col("d.tier") == "Platinum", True)) > 0, True).otherwise(False).alias("platinum_tier"),
        when(count(when(col("benefit")=="24 hour dedicated line", True))>0, True).otherwise(False).alias("24_hour_dedicated_line_benefit"),
        when(count(when(col("benefit") == "concert tickets", True)) > 0, True).otherwise(False).alias("concert_ticket_benefit"),
        when(count(when(col("benefit") == "travel insurance", True)) > 0, True).otherwise(False).alias("travel_insurance_benefit"),
        when(count(when(col("benefit") == "financial planning assistance", True)) > 0, True).otherwise(False).alias("financial_planning_assistance_benefit"),
        when(count(when(col("benefit") == "shopping discounts", True)) > 0, True).otherwise(False).alias("shopping_discounts_benefit"),
        when(count(when(col("benefit") == "sports tickets", True)) > 0, True).otherwise(False).alias("sports_tickets_benefit"),
        when(count(when(col("benefit") == "concierge services", True)) > 0, True).otherwise(False).alias("concierge_service_benefit"),
        when(count(when(col("benefit") == "car rental insurance", True)) > 0, True).otherwise(False).alias("car_rental_benefit"),
        when(count(when(col("benefit") == "airline lounge access", True)) > 0, True).otherwise(False).alias("airline_lounge_access_benefit"),
        when(count(when(col("benefit") == "dedicated account representative", True)) > 0, True).otherwise(False).alias("dedicated_account_representative")
        
        )   


# COMMAND ----------

# MAGIC %md
# MAGIC #### Managing Slowly Changing Dimension Type 2!

# COMMAND ----------

 
df_updated = df_grouped\
    .withColumn("start_date", lit(current_date()))\
    .withColumn("end_date", lit(None))\
    .withColumn("is_current", lit(True))\
    .withColumn("source_system", lit("mongoDB_1"))
df_current = spark.sql(
    """
    SELECT * EXCEPT (customer_sk)
    FROM presentation.ct.dim_customer
    WHERE is_current = 1
    """
)

if df_current.count() != 0:
    df_upd_j_cur = df_updated.alias("upd").join(df_current.alias("cur"), df_updated.customer_bk == df_current.customer_bk, "outer")

    df_hist_records = df_upd_j_cur\
        .filter(col("cur.customer_bk").isNotNull())\
        .filter(col("upd.address") != col("cur.address"))\
        .select("cur.*")\
        .withColumn("is_current", lit(False))\
        .withColumn("end_date", lit(current_date()))
        

    id_list = [row.customer_bk for row in df_hist_records.select("cur.customer_bk").collect()]

    df_new_records = df_upd_j_cur\
        .withColumn("upd.start_date", when((~col("upd.customer_bk").isin(id_list)) & (col("cur.start_date") != 'null') , col("cur.start_date")))\
        .select("upd.*")

    df_final = df_new_records.union(df_hist_records)

    df_final.createOrReplaceTempView("source_data")
else:
    df_final = df_updated
    df_final.createOrReplaceTempView("source_data")



# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO presentation.ct.dim_customer AS trg
# MAGIC USING source_data AS src
# MAGIC ON src.customer_bk = trg.customer_bk AND src.is_current = 1
# MAGIC WHEN MATCHED THEN UPDATE
# MAGIC SET
# MAGIC   customer_bk = src.customer_bk,
# MAGIC   name = src.name,
# MAGIC   user_name = src.user_name,
# MAGIC   address = src.address,
# MAGIC   birth_date = src.birth_date,
# MAGIC   email = src.email,
# MAGIC   active_customer = src.active_customer,
# MAGIC   bronze_tier = src.bronze_tier,
# MAGIC   silver_tier = src.silver_tier,
# MAGIC   gold_tier = src.gold_tier,
# MAGIC   platinum_tier = src.platinum_tier,
# MAGIC   24_hour_dedicated_line_benefit = src.24_hour_dedicated_line_benefit,
# MAGIC   concert_ticket_benefit = src.concert_ticket_benefit,
# MAGIC   travel_insurance_benefit = src.travel_insurance_benefit,
# MAGIC   financial_planning_assistance_benefit = src.financial_planning_assistance_benefit,
# MAGIC   shopping_discounts_benefit = src.shopping_discounts_benefit,
# MAGIC   sports_tickets_benefit = src.sports_tickets_benefit,
# MAGIC   concierge_service_benefit = src.concierge_service_benefit,
# MAGIC   car_rental_benefit = src.car_rental_benefit,
# MAGIC   airline_lounge_access_benefit = src.airline_lounge_access_benefit,
# MAGIC   dedicated_account_representative = src.dedicated_account_representative,
# MAGIC   start_date = src.start_date,
# MAGIC   end_date = src.end_date,
# MAGIC   is_current = src.is_current,
# MAGIC   source_system = src.source_system
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC INSERT(
# MAGIC   customer_bk,
# MAGIC   name,
# MAGIC   user_name,
# MAGIC   address,
# MAGIC   birth_date,
# MAGIC   email,
# MAGIC   active_customer,
# MAGIC   bronze_tier,
# MAGIC   silver_tier,
# MAGIC   gold_tier,
# MAGIC   platinum_tier,
# MAGIC   24_hour_dedicated_line_benefit,
# MAGIC   concert_ticket_benefit,
# MAGIC   travel_insurance_benefit,
# MAGIC   financial_planning_assistance_benefit,
# MAGIC   shopping_discounts_benefit,
# MAGIC   sports_tickets_benefit,
# MAGIC   concierge_service_benefit,
# MAGIC   car_rental_benefit,
# MAGIC   airline_lounge_access_benefit,
# MAGIC   dedicated_account_representative,
# MAGIC   start_date,
# MAGIC   end_date,
# MAGIC   is_current,
# MAGIC   source_system
# MAGIC )
# MAGIC VALUES(
# MAGIC   src.customer_bk,
# MAGIC   src.name,
# MAGIC   src.user_name,
# MAGIC   src.address,
# MAGIC   src.birth_date,
# MAGIC   src.email,
# MAGIC   src.active_customer,
# MAGIC   src.bronze_tier,
# MAGIC   src.silver_tier,
# MAGIC   src.gold_tier,
# MAGIC   src.platinum_tier,
# MAGIC   src.24_hour_dedicated_line_benefit,
# MAGIC   src.concert_ticket_benefit,
# MAGIC   src.travel_insurance_benefit,
# MAGIC   src.financial_planning_assistance_benefit,
# MAGIC   src.shopping_discounts_benefit,
# MAGIC   src.sports_tickets_benefit,
# MAGIC   src.concierge_service_benefit,
# MAGIC   src.car_rental_benefit,
# MAGIC   src.airline_lounge_access_benefit,
# MAGIC   src.dedicated_account_representative,
# MAGIC   src.start_date,
# MAGIC   src.end_date,
# MAGIC   src.is_current,
# MAGIC   src.source_system
# MAGIC )
# MAGIC