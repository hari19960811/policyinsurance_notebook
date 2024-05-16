# Databricks notebook source
# MAGIC %sql
# MAGIC select current_database()

# COMMAND ----------

# MAGIC %sql
# MAGIC create database bronzelayer;

# COMMAND ----------

# MAGIC %sql
# MAGIC use bronzelayer;
# MAGIC show tables;

# COMMAND ----------

spark.sql("create database bronzelayer")

# COMMAND ----------

spark.sql("drop database bronzelayer")
