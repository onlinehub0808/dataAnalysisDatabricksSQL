# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC Resets the environment removing any directories and or tables created during course execution

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

DA = DBAcademyHelper(**helper_arguments)
user_db = DA.get_database_name()

rows = spark.sql("SHOW DATABASES").collect()
for row in rows:
    db_name = row[0]
    if db_name.startswith(user_db):
        print(f"Dropping database {db_name}")
        spark.sql(f"DROP DATABASE IF EXISTS {db_name} CASCADE")

result = dbutils.fs.rm(DA.paths.datasets, True)
print(f"Deleted {DA.paths.datasets}: {result}")

result = dbutils.fs.rm(DA.paths._working_dir_root, True)
print(f"Deleted {DA.paths._working_dir_root}: {result}")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
