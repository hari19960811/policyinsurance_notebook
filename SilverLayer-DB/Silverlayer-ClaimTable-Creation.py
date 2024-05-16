# Databricks notebook source
# MAGIC %sql
# MAGIC create table silverlayer.claim (
# MAGIC   claim_id int,
# MAGIC   policy_id int,
# MAGIC   date_of_claim timestamp,
# MAGIC   claim_amount decimal(18,8),
# MAGIC   claim_status string,
# MAGIC   LastUpdatedTimeStamp timestamp,
# MAGIC   merged_timestamp timestamp
# MAGIC ) using delta location '/mnt/silverlayer/Claim'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silverlayer.claim
