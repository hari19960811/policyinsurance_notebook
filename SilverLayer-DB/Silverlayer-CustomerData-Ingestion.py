# Databricks notebook source
# MAGIC %md
# MAGIC <b>Remove all where customer id is not null</b>

# COMMAND ----------

df = spark.sql("select * from bronzelayer.customer where customer_id is not null and merge_flg=false")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Remove record where gender is other then Male/Female</b>

# COMMAND ----------

df_gender = spark.sql("select * from bronzelayer.customer where customer_id is not null and gender in ('Male','Female') and merge_flg=false")
display(df_gender)

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Outlier check at some registration date > DOB </b>

# COMMAND ----------

df_final = spark.sql("select * from bronzelayer.customer where customer_id is not null and gender in ('Male','Female')and registration_date > date_of_birth and merge_flg=false")
df_final.createOrReplaceTempView("clean_customer")

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Merge the data into silver layer while adding current timestamp </b>

# COMMAND ----------

spark.sql("MERGE into silverlayer.customer as t using clean_customer as s on t.customer_id=s.customer_id when matched then update set t.first_name=s.first_name,t.last_name=s.last_name,t.email=s.email,t.phone=s.phone,t.country=s.country,t.city=s.city,t.registration_date=s.registration_date,t.date_of_birth=s.date_of_birth,t.gender=s.gender,t.merged_timestamp=current_timestamp() when not matched then insert(first_name,last_name,email,phone,country,city,registration_date,date_of_birth,gender,merged_timestamp,customer_id) values(s.first_name,s.last_name,s.email,s.phone,s.country,s.city,s.registration_date,s.date_of_birth,s.gender,current_timestamp(),s.customer_id) ")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silverlayer.customer

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Update the flag in the bronze layer</b>

# COMMAND ----------

# MAGIC %sql
# MAGIC update bronzelayer.customer set merge_flg=true where merge_flg=false

# COMMAND ----------

# MAGIC %md
# MAGIC
