# Databricks notebook source
# MAGIC %run ./_utility-functions

# COMMAND ----------

DA = DBAcademyHelper()

# COMMAND ----------

from dbacademy import dbgems

for username in DA.usernames:
    db_name = Step.to_db_name(username=username, naming_template=DA.naming_template, naming_params=DA.naming_params)
    spark.sql(f"GRANT ALL PRIVILEGES ON DATABASE `{db_name}` TO `{username}`")
    spark.sql(f"GRANT ALL PRIVILEGES ON ANY FILE TO `{username}`")
    spark.sql(f"ALTER DATABASE {db_name} OWNER TO `{username}`;\n")

