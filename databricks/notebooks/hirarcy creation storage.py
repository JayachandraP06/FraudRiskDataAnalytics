# Databricks notebook source
# MAGIC %md
# MAGIC ### external location creation
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC create external location bronze_ext_loc
# MAGIC url 'abfss://databricksmeta@fraudanalyticsdata.dfs.core.windows.net/bronze'
# MAGIC with (storage credential frastoragecred);
# MAGIC
# MAGIC create external location silver_ext_loc
# MAGIC url 'abfss://databricksmeta@fraudanalyticsdata.dfs.core.windows.net/silver'
# MAGIC with (storage credential frastoragecred);
# MAGIC
# MAGIC create external location gold_ext_loc
# MAGIC url 'abfss://databricksmeta@fraudanalyticsdata.dfs.core.windows.net/gold'
# MAGIC with (storage credential frastoragecred);

# COMMAND ----------

# MAGIC %md
# MAGIC ### ##    catalog creation

# COMMAND ----------

# MAGIC %sql
# MAGIC create catalog if not exists fra_catalog;

# COMMAND ----------

# MAGIC %md
# MAGIC create schemas

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists fra_catalog.bronze_schema;
# MAGIC create schema if not exists fra_catalog.silver_schema;
# MAGIC create schema if not exists fra_catalog.gold_schema;

# COMMAND ----------

# MAGIC %sql
# MAGIC create external location bronze_external
# MAGIC url 'abfss://bronze-data@fraudanalyticsdata.dfs.core.windows.net/'
# MAGIC with (storage credential frastoragecred);

# COMMAND ----------

# MAGIC %sql
# MAGIC create external location source_loc
# MAGIC url 'abfss://source-data@fraudanalyticsdata.dfs.core.windows.net/'
# MAGIC with (storage credential frastoragecred);