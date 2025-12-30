# Databricks notebook source
# MAGIC %md
# MAGIC ### data ingestion from adls gen 2 to bronze delta table 

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *


# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog fra_catalog;
# MAGIC use schema bronze_schema;

# COMMAND ----------

transaction_schema = StructType([

    StructField("id", StringType(), True),                 # transaction id
    StructField("date", DateType(), True),                 # transaction date (YYYY-MM-DD)
    StructField("client_id", StringType(), True),
    StructField("card_id", StringType(), True),

    StructField("amount", StringType(), True),              # transaction amount
    StructField("use_chip", StringType(), True),            # Y / N (keep string for now)

    StructField("merchant_id", StringType(), True),
    StructField("merchant_city", StringType(), True),
    StructField("merchant_state", StringType(), True),

    StructField("zip", StringType(), True),                 # zip codes should be STRING
    StructField("mcc", StringType(), True),                 # merchant category code

    StructField("errors", StringType(), True),              # error messages / flags

    StructField("dateT", TimestampType(), True),            # transaction timestamp
    StructField("year_month", StringType(), True)           # e.g. 2018-03
])

# COMMAND ----------

import csv

# COMMAND ----------

df_raw_text=spark.read.format('text')\
                .load('abfss://bronze-data@fraudanalyticsdata.dfs.core.windows.net/2010/data_2010-01.csv')
print(df_raw_text.count())

# COMMAND ----------

df_bronze=spark.read.format('csv')\
            .option('header','true')\
            .option('inferSchema','true')\
            .load('abfss://source-data@fraudanalyticsdata.dfs.core.windows.net/transactions_data.csv')

df_bronze.count()


# COMMAND ----------

def write_to_delta_table(df,table_name):
    df.write.format('delta')\
        .option('overwriteSchema','true')\
        .mode('overwrite')\
        .option('overwriteSchema','true')\
        .saveAsTable(f"fra_catalog.bronze_schema.{table_name}")
write_to_delta_table(df_bronze,'transactions_bronze')

# COMMAND ----------

df_bronze.write.format('delta')\
                .option('overwriteSchema','true')\
                .option('mergeSchema','true')\
                .option('path','abfss://databricksmeta@fraudanalyticsdata.dfs.core.windows.net/bronze/transaction_bronze')\
                .save()

# COMMAND ----------

