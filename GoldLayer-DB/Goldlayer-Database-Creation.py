# Databricks notebook source
# MAGIC %sql
# MAGIC create database goldlayer;

# COMMAND ----------

# MAGIC %md
# MAGIC <b> Sales By Policy Type and Month </b>

# COMMAND ----------

# MAGIC %sql
# MAGIC create table goldlayer.sales_by_policy_type_and_month(
# MAGIC   policy_type string,
# MAGIC   sale_month string,
# MAGIC   total_premium integer,
# MAGIC   updated_timestamp timestamp
# MAGIC ) using delta location '/mnt/goldlayer/sales_by_policy_type_and_month'

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Claims by policy type and Status</b>

# COMMAND ----------

# MAGIC %sql
# MAGIC create table goldlayer.claims_by_policy_type_and_status(
# MAGIC   policy_type string,
# MAGIC   claim_status string,
# MAGIC   total_claims integer,
# MAGIC   total_claim_mount integer,
# MAGIC   updated_timestamp timestamp
# MAGIC ) using delta location  '/mnt/goldlayer/claims_by_policy_type_and_status'

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Analyze the claim data based on the policy type like AVG,MAX,MIN,Count of claim </b>

# COMMAND ----------

# MAGIC %sql
# MAGIC create table goldlayer.claims_analysis(
# MAGIC   policy_type string,
# MAGIC   claim_status string,
# MAGIC   avg_claim_amount integer,
# MAGIC   max_claim_amount integer,
# MAGIC   min_claim_integer integer,
# MAGIC   total_claims integer,
# MAGIC   update_timestamp timestamp
# MAGIC ) using delta location '/mnt/goldlayer/claims_analysis'
