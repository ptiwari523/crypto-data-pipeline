# Databricks notebook source
# MAGIC %md
# MAGIC ###Build Gold layer (business logic)

# COMMAND ----------

# MAGIC %run "../utils/common_utils"

# COMMAND ----------

print("🚀 Gold job started")
silver_df = spark.read.table("crypto_silver")
if silver_df.count() == 0:
    raise Exception("❌ Silver is empty — stopping Gold")
gold_df = silver_df\
    .select("market_cap_rank",
            "name",
            "symbol", 
            "current_price", 
            "market_cap",
            "price_change_percentage_24h",
            "total_volume"
            )\
    .orderBy(col("market_cap_rank"))\
    .limit(50)    

# COMMAND ----------

# MAGIC %md
# MAGIC ###Save Data into Gold Layer

# COMMAND ----------

gold_df.write\
  .format("delta")\
  .mode("overwrite")\
  .saveAsTable("crypto_gold")

print("✅ Gold completed successfully")

# COMMAND ----------

