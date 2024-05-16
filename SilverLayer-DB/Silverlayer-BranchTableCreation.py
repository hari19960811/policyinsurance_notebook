# Databricks notebook source
# MAGIC %sql
# MAGIC create table silverlayer.branch(
# MAGIC   branch_id int,
# MAGIC   branch_country string,
# MAGIC   branch_city string,
# MAGIC   merged_timestamp timestamp
# MAGIC ) using delta location '/mnt/silverlayer/Branch/'
