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

# MAGIC %run ./_utility-functions

# COMMAND ----------

DA = DBAcademyHelper()
user_db = Step.to_db_name(DA.username, DA.naming_template, DA.naming_params)

rows = spark.sql("SHOW DATABASES").collect()
for row in rows:
    db_name = row[0]
    if db_name.startswith(user_db):
        print(f"Dropping database {db_name}")
        spark.sql(f"DROP DATABASE IF EXISTS {db_name} CASCADE")

datasets = f"dbfs:/mnt/dbacademy-datasets/{DA.data_source_name}"
dbutils.fs.rm(datasets, True)

result = dbutils.fs.rm(DA.working_dir_prefix, True)
print(f"Deleted {DA.working_dir_prefix}: {result}")

# COMMAND ----------

DA.install_datasets(reinstall=False)
print("Course environment succesfully reset")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
