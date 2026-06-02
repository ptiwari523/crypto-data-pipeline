# Databricks notebook source
# MAGIC %run "../utils/common_utils"

# COMMAND ----------

# Data Ingetion
print("🚀 Ingestion started")

url = "https://api.coingecko.com/api/v3/coins/markets"  # This is the API endpoint I want to hit.
all_data = []
page = 1
MAX_PAGES_PER_RUN = 20

MAX_RATE_LIMIT_RETRIES = 10

while page <= MAX_PAGES_PER_RUN:
    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": 30,
        "page": page
    } # Each key-value pair is a filter: "vs_currency": "usd" → Show prices in USD

    rate_limit_count = 0
    while True:
        response = requests.get(url, params=params)
        if response.status_code == 200:
            data = response.json()

            if not data:
                print("No more data")
                break

            all_data.extend(data)
            print(f"Page {page} success")

            page += 1
            time.sleep(15) # slow but reliable
            break

        elif response.status_code == 429:
            rate_limit_count += 1

            print(f"Rate limited...waiting 60s ( {rate_limit_count} )")
            time.sleep(60)

            if rate_limit_count >= MAX_RATE_LIMIT_RETRIES:
                print(f"⚠️ Skipping page {page}")
                page += 1
                break
        
        else:
            print(f"Error {response.status_code}")
            page += 1
            break

    # exit outer loop if no more data
    if response.status_code == 200 and not data:
        break

print(len(all_data))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Convert to Spark DataFrame

# COMMAND ----------

if len(all_data) == 0: # ✅ Validate data
    raise Exception("❌ No data fetched")
else:
    # Normalize data before Spark
    # Use pandas as a bridge (very practical in Databricks)
    # Pandas handles mixed types (int, float) better, then Spark converts cleanly.
    pdf = pd.DataFrame(all_data)
    print(f"Pandas records: {len(pdf)}")

    # Convert to Spark DataFrame
    df = spark.createDataFrame(pdf)
    print(f"Spark records: {df.count()}")
    # Adding a new column called ingestion_time
    # withColumn() → creates or replaces a column
    # "ingestion_time" → name of new column
    # current_timestamp() → value for every row

    # ✅ Add ingestion timestamp
    df = df.withColumn("ingestion_time", current_timestamp())

    # ✅ Fix data types
    df = df.withColumn("last_updated", to_timestamp("last_updated")) # updating string column to date type

    # Write to Bronze layer
    df.write\
    .format("delta")\
    .mode("append")\
    .saveAsTable("crypto_bronze")

    print("✅ Data saved to Bronze")
    print("✅ Ingestion completed successfully")
