# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df_silver=spark.table('fra_catalog.bronze_schema.transactions_bronze')
df_silver.display(10)

# COMMAND ----------

df_silver.printSchema()

# COMMAND ----------

df_silver=df_silver.dropDuplicates()

# COMMAND ----------

df_silver=df_silver.dropna(subset=["id","date","client_id","card_id","amount","zip"])

# COMMAND ----------

print(df_silver.display())

# COMMAND ----------

df_silver=df_silver.withColumn("amount_cleaned",regexp_replace(col('amount'),"[$,]","").cast("decimal(12,2)"))

# COMMAND ----------

df_silver.printSchema()

# COMMAND ----------

df_silver=df_silver.drop("amount")

# COMMAND ----------

df_silver = df_silver.withColumnRenamed("id", "transaction_id")\
                .withColumnRenamed("date", "transaction_date")\
                .withColumnRenamed("amount_cleaned", "amount")

# COMMAND ----------

df_silver.printSchema()

# COMMAND ----------

df_silver.display()

# COMMAND ----------

df_silver=df_silver.drop("errors","mcc","use_chip")
df_silver=df_silver.dropna(subset=["zip"])

# COMMAND ----------

df_silver.printSchema()

# COMMAND ----------

df=df_silver.filter(col('zip').isNull())

# COMMAND ----------

df.count()

# COMMAND ----------

df_silver.count()

# COMMAND ----------

df_silver.write.format('delta')\
        .option('overwriteSchema','true')\
        .mode('overwrite')\
        .saveAsTable(f"fra_catalog.silver_schema.transactions_silver")

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED fra_catalog.silver_schema.transactions_silver

# COMMAND ----------

df_silver.write.format('delta')\
                .option('overwriteSchema','true')\
                .option('mergeSchema','true')\
                .option('path','abfss://databricksmeta@fraudanalyticsdata.dfs.core.windows.net/silver/transaction_silver')\
                .save()

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists fra_catalog.silver_schema.transactions_silver_external
# MAGIC using delta
# MAGIC as 
# MAGIC select * from fra_catalog.silver_schema.transactions_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended fra_catalog.silver_schema.transactions_silver_external

# COMMAND ----------

# MAGIC %sql
# MAGIC create table fra_catalog.silver_schema.transactions_external
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table fra_catalog.silver_schema.transactions_external;

# COMMAND ----------

