# Databricks notebook source
# MAGIC %md
# MAGIC ###Build Silver (latest snapshot)

# COMMAND ----------

print("🚀 Silver job started")
from pyspark.sql.window import Window #Used to define grouping + ordering logic
from pyspark.sql.functions import row_number, col # row_number, assigns ranking (1, 2, 3…) inside each group, col Refers to a column safely

bronze_df = spark.read.table("crypto_bronze")
if bronze_df.count() == 0:
    raise Exception("❌ Bronze is empty — stopping Silver")
window = Window.partitionBy("id").orderBy(col("ingestion_time").desc())
silver_df = bronze_df.withColumn("rank", row_number().over(window))\
    .filter("rank = 1")\
    .drop("rank")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Write Silver Data
# MAGIC In Silver layer:
# MAGIC
# MAGIC 👉 `We` want:
# MAGIC - only latest snapshot
# MAGIC - no duplicates
# MAGIC - clean state

# COMMAND ----------

silver_df.write\
  .format("delta")\
  .mode("overwrite")\
  .saveAsTable("crypto_silver")
  
print("✅ Silver completed successfully")