# Databricks notebook source
# MAGIC %md
# MAGIC <b>Remove all records where branch id is null </b>

# COMMAND ----------

df = spark.sql("select * from bronzelayer.branch where branch_id is not null and merge_flg=false")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Remove all the leading and trailing spaces in Branch country and convert it into UPPER CASE </b>

# COMMAND ----------

df_trim = spark.sql("select branch_id,upper(trim(branch_country)) as branch_country,branch_city from bronzelayer.branch where branch_id is not null and merge_flg=false")
display(df_trim)

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Merge into silver layer table </b>

# COMMAND ----------

#df_trim.createOrReplaceTempView("clean_branch")
spark.sql("MERGE into silverlayer.branch as t using clean_branch as s on t.branch_id=s.branch_id when matched then update set t.branch_country=s.branch_country,t.branch_city=s.branch_city when not matched then insert(branch_country,branch_city,branch_id,merged_timestamp) values(s.branch_country,s.branch_city,s.branch_id,current_timestamp())")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silverlayer.branch;

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Update the merged_flag in the bronzelayer table </b>

# COMMAND ----------

# MAGIC %sql
# MAGIC update bronzelayer.branch set merge_flg=true where merge_flg=false;
# MAGIC -- select * from bronzelayer.branch;
