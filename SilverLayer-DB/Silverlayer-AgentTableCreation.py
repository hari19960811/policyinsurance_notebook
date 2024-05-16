# Databricks notebook source
# MAGIC %sql
# MAGIC create database silverlayer;

# COMMAND ----------

# MAGIC %sql
# MAGIC create table silverlayer.Agent (
# MAGIC   agent_id integer ,
# MAGIC   agent_name string , 
# MAGIC   agent_email string , 
# MAGIC   agent_phone string , 
# MAGIC   branch_id integer , 
# MAGIC   created_timestamp timestamp
# MAGIC ) using delta location '/mnt/silverlayer/Agent'

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Add a column merged timestamp to the agent table </b>

# COMMAND ----------

# MAGIC %sql
# MAGIC alter table silverlayer.agent add columns (merged_timestamp timestamp)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silverlayer.agent 
