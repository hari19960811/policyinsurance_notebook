# Databricks notebook source
# MAGIC %sql
# MAGIC create table silverlayer.policy (
# MAGIC     policy_id integer,
# MAGIC     policy_type string,
# MAGIC     customer_id integer,
# MAGIC     start_date timestamp,
# MAGIC     end_date timestamp,
# MAGIC     premium double,
# MAGIC     coverage_amount double,
# MAGIC     merged_timestamp timestamp
# MAGIC ) using delta location '/mnt/silverlayer/Policy'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silverlayer.policy
