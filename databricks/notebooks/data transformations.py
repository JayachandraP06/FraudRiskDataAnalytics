# Databricks notebook source
# MAGIC %md
# MAGIC ### data transformations for gold layer and fraud - risk KPI's

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df_gold=spark.read.table('fra_catalog.silver_schema.transactions_silver_external')
df_gold.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists fra_catalog.gold_schema.high_velocity_activity
# MAGIC using delta 
# MAGIC as 
# MAGIC select client_id , card_id, time_window.start as start_time, time_window.end as end_time, transaction_count,high_velocity from (
# MAGIC select client_id, card_id, window(transaction_date,'30 minutes') as time_window , count(*) as transaction_count,
# MAGIC case when count(*) >5 then true else false end as high_velocity
# MAGIC from fra_catalog.silver_schema.transactions_silver_external
# MAGIC group by card_id, window(transaction_date,'30 minutes'), client_id )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS fra_catalog.gold_schema.high_velocity_activity (
# MAGIC   client_id INT,
# MAGIC   card_id INT,
# MAGIC   start_time TIMESTAMP,
# MAGIC   end_time TIMESTAMP,
# MAGIC   transaction_count BIGINT,
# MAGIC   high_velocity BOOLEAN
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO fra_catalog.gold_schema.high_velocity_activity
# MAGIC SELECT
# MAGIC   client_id,
# MAGIC   card_id,
# MAGIC   time_window.start AS start_time,
# MAGIC   time_window.end   AS end_time,
# MAGIC   transaction_count,
# MAGIC   high_velocity
# MAGIC FROM (
# MAGIC   SELECT
# MAGIC     client_id,
# MAGIC     card_id,
# MAGIC     window(transaction_date, '30 minutes') AS time_window,
# MAGIC     COUNT(*) AS transaction_count,
# MAGIC     CASE WHEN COUNT(*) > 5 THEN true ELSE false END AS high_velocity
# MAGIC   FROM fra_catalog.silver_schema.transactions_silver_external
# MAGIC   GROUP BY
# MAGIC     client_id,
# MAGIC     card_id,
# MAGIC     window(transaction_date, '30 minutes')
# MAGIC ) t;

# COMMAND ----------

# MAGIC %sql
# MAGIC describe fra_catalog.gold_schema.high_velocity_activity

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from fra_catalog.gold_schema.high_velocity_activity

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from fra_catalog.gold_schema.high_velocity_activity 
# MAGIC where high_velocity=true;

# COMMAND ----------

df_gold=spark.read.table('fra_catalog.gold_schema.high_velocity_activity')

# COMMAND ----------

df_gold.display()

# COMMAND ----------

df_gold.write.format('delta')\
            .mode('overwrite')\
            .option('overwriteSchema','true')\
            .save('abfss://databricksmeta@fraudanalyticsdata.dfs.core.windows.net/gold/high_velocity_activity')

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists fra_catalog.gold_schema.high_velocity_activity_external
# MAGIC using delta 
# MAGIC location 'abfss://databricksmeta@fraudanalyticsdata.dfs.core.windows.net/gold/high_velocity_activity'

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended fra_catalog.gold_schema.high_velocity_activity_external

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CREATE TABLE fra_catalog.gold_schema.amount_anomaly_risk
# MAGIC -- USING DELTA
# MAGIC -- AS
# MAGIC WITH client_avg AS (
# MAGIC   SELECT
# MAGIC     client_id,
# MAGIC     AVG(amount) AS avg_amount
# MAGIC   FROM fra_catalog.silver_schema.transactions_silver_external
# MAGIC   GROUP BY client_id
# MAGIC )
# MAGIC SELECT
# MAGIC   t.client_id,
# MAGIC   t.transaction_id,
# MAGIC   t.transaction_date,
# MAGIC   t.amount,
# MAGIC   a.avg_amount,
# MAGIC   CASE
# MAGIC     WHEN t.amount > a.avg_amount * 5 THEN true
# MAGIC     ELSE false
# MAGIC   END AS amount_anomaly_flag
# MAGIC FROM fra_catalog.silver_schema.transactions_silver_external t
# MAGIC JOIN client_avg a
# MAGIC ON t.client_id = a.client_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from fra_catalog.gold_schema.amount_anomaly_risk where amount_anomaly_flag=true
# MAGIC     

# COMMAND ----------

df_amount_anomaly=spark.read.table('fra_catalog.gold_schema.amount_anomaly_risk')
df_amount_anomaly.display()

# COMMAND ----------

df_amount_anomaly.write.format('delta')\
            .mode('overwrite')\
            .option('overwriteSchema','true')\
            .save('abfss://databricksmeta@fraudanalyticsdata.dfs.core.windows.net/gold/amount_anomaly_risk')

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists fra_catalog.gold_schema.amount_anomaly_risk_external
# MAGIC using delta 
# MAGIC location 'abfss://databricksmeta@fraudanalyticsdata.dfs.core.windows.net/gold/amount_anomaly_risk'
# MAGIC     

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended fra_catalog.gold_schema.amount_anomaly_risk_external;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE fra_catalog.gold_schema.merchant_risk_summary
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC   merchant_id,
# MAGIC   merchant_city,
# MAGIC   merchant_state,
# MAGIC   COUNT(*) AS total_transactions,
# MAGIC   SUM(amount) AS total_amount,
# MAGIC   AVG(amount) AS avg_transaction_amount,
# MAGIC   CASE
# MAGIC     WHEN COUNT(*) > 10000 THEN 'HIGH'
# MAGIC     WHEN COUNT(*) > 3000 THEN 'MEDIUM'
# MAGIC     ELSE 'LOW'
# MAGIC   END AS merchant_risk_score
# MAGIC FROM fra_catalog.silver_schema.transactions_silver_external
# MAGIC GROUP BY merchant_id, merchant_city, merchant_state;

# COMMAND ----------

df_mer=spark.read.table('fra_catalog.gold_schema.merchant_risk_summary')
df_mer.write.format('delta')\
            .mode('overwrite')\
            .option('overwriteSchema','true')\
            .save('abfss://databricksmeta@fraudanalyticsdata.dfs.core.windows.net/gold/merchant_risk_summary')

# COMMAND ----------

df_mer.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists fra_catalog.gold_schema.merchant_risk_summary_external
# MAGIC using delta 
# MAGIC location 'abfss://databricksmeta@fraudanalyticsdata.dfs.core.windows.net/gold/merchant_risk_summary'

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended fra_catalog.gold_schema.amount_anomaly_risk_external

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists fra_catalog.gold_schema.geo_risk_anomaly
# MAGIC using delta
# MAGIC as 
# MAGIC select 
# MAGIC card_id,
# MAGIC   distinct_locations,
# MAGIC   w.start as start_time,
# MAGIC   w.end as end_time,
# MAGIC   total_txns
# MAGIC   from (
# MAGIC SELECT
# MAGIC   card_id,
# MAGIC   COUNT(DISTINCT zip) AS distinct_locations,
# MAGIC   window(transaction_date,'1 hour') as w,
# MAGIC   COUNT(*) AS total_txns
# MAGIC FROM fra_catalog.silver_schema.transactions_silver_external
# MAGIC GROUP BY card_id,w
# MAGIC HAVING COUNT(DISTINCT zip) > 3);

# COMMAND ----------

df_geo=spark.read.table('fra_catalog.gold_schema.geo_risk_anomaly')
df_geo.write.format('delta')\
            .mode('overwrite')\
            .option('overwriteSchema','true')\
            .save('abfss://databricksmeta@fraudanalyticsdata.dfs.core.windows.net/gold/geo_risk_summary')


# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists fra_catalog.gold_schema.geo_risk_anomaly_external
# MAGIC using delta
# MAGIC location 'abfss://databricksmeta@fraudanalyticsdata.dfs.core.windows.net/gold/geo_risk_anomaly'

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended fra_catalog.gold_schema.geo_risk_anomaly_external

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from fra_catalog.gold_schema.geo_risk_anomaly

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table fra_catalog.gold_schema.geo_risk_anomaly_external

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists fra_catalog.gold_schema.geo_risk_anomaly_external
# MAGIC using delta
# MAGIC location 'abfss://databricksmeta@fraudanalyticsdata.dfs.core.windows.net/gold/geo_risk_summary'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from fra_catalog.gold_schema.geo_risk_anomaly_external

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended fra_catalog.gold_schema.geo_risk_anomaly_external

# COMMAND ----------

