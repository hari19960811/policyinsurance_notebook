# Databricks notebook source
# MAGIC %sql
# MAGIC create table silverlayer.customer(
# MAGIC   customer_id int,
# MAGIC   first_name string,
# MAGIC   last_name string,
# MAGIC   email string,
# MAGIC   phone string,
# MAGIC   country string,
# MAGIC   city string,
# MAGIC   registration_date timestamp,
# MAGIC   date_of_birth timestamp,
# MAGIC   gender string,
# MAGIC   merged_timestamp timestamp
# MAGIC ) using delta location '/mnt/silverlayer/Customer/'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silverlayer.customer
