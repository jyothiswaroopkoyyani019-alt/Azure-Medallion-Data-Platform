# Databricks notebook source
from pyspark.sql.functions import col, upper, abs, coalesce, lit, when, count, sum

# ==========================================================
# 🟫 STEP 1: THE BRONZE LAYER (Reading Direct via SAS Token)
# ==========================================================
storage_account_name = "datalakebriksetl99"

# 🔑 Live SAS Token with the starting question mark '?' included
sas_token = "?sv=2026-02-06&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2026-11-26T12:16:03Z&st=2026-10-26T04:01:03Z&spr=https&sig=fX4HLtF2VnWOwfkVkoWt%2BpuuFdOu3H0TkQRKz8GmxUo%3D" 

# The production ABFSS secure file system path link
abfss_path = f"abfss://landing-zone@{storage_account_name}.dfs.core.windows.net/bronze-raw/daily_transactions.csv"

# Ingest the live CSV file using the native SAS token provider configuration
df_raw = (spark.read
          .format("csv")
          .option("header", "true")       
          .option("inferSchema", "true")  
          .option(f"fs.azure.account.auth.type.{storage_account_name}.dfs.core.windows.net", "SAS")
          .option(f"fs.azure.sas.token.provider.type.{storage_account_name}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
          .option(f"fs.azure.sas.fixed.token.{storage_account_name}.dfs.core.windows.net", sas_token)
          .load(abfss_path))

print("🟫 1. Raw Cloud Data Ingested Successfully Into Bronze!")


# ==========================================================
# 🥈 STEP 2: THE SILVER LAYER (Automated Data Cleaning Loops)
# ==========================================================
df_bronze = df_raw
transformation_rules = []

for field in df_bronze.schema.fields:
    col_name = field.name
    col_type = field.dataType.simpleString()
    
    if col_name == "OrderID":
        transformation_rules.append(col(col_name))
    elif col_type == "string":
        clean_txt = when((col(col_name) == "Null") | col(col_name).isNull(), lit("UNKNOWN")).otherwise(col(col_name))
        transformation_rules.append(upper(clean_txt).alias(col_name))
    elif col_type in ["double", "float", "long", "int", "integer"]:
        transformation_rules.append(abs(coalesce(col(col_name), lit(0))).alias(col_name))

df_silver_clean = df_bronze.select(*transformation_rules)
df_silver_clean.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("databricks_workspace.default.silver_orders")
print("🥈 2. Silver Layer Processed and Cleaned!")


# ==========================================================
# 🥇 STEP 3: THE GOLD LAYER + PRODUCTION FILE EXPORT
# ==========================================================
df_silver = spark.read.table("databricks_workspace.default.silver_orders")

# Calculate Gold Metrics for your internal Lakehouse catalog
df_gold_summary = (df_silver.groupBy("ItemName")
                   .agg(
                       count("OrderID").alias("TotalOrders"),
                       sum("Amount").alias("TotalSalesRevenue")
                   ))

df_gold_summary.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("databricks_workspace.default.gold_orders_summary")
print("🥇 3. Gold Layer Metrics Calculated and Saved Successfully!")

print("💾 Exporting clean processed records as raw Parquet blocks for ADF flattening...")

# Clean, single-statement write specifically designed for your ADF Parquet dataset layout

#  FIXED: Saving to a separate folder so it NEVER overwrites your raw source data!
(df_silver.coalesce(1).write
 .format("parquet") 
 .mode("overwrite")
 .option(f"fs.azure.account.auth.type.{storage_account_name}.dfs.core.windows.net", "SAS")
 .option(f"fs.azure.sas.token.provider.type.{storage_account_name}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
 .option(f"fs.azure.sas.fixed.token.{storage_account_name}.dfs.core.windows.net", sas_token)
 .save(f"abfss://landing-zone@{storage_account_name}.dfs.core.windows.net/silver-processed/"))

print("✅ Raw Parquet folder data successfully exported to silver-processed!")
display(df_gold_summary)
