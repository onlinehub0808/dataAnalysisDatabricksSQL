# Databricks notebook source
# MAGIC %md
# MAGIC # Pre-Course Setup
# MAGIC 
# MAGIC Run this notebook just before class to ensure all assets are ready.
# MAGIC 
# MAGIC The steps include:
# MAGIC * Install the datasets to a common folder in the workspace
# MAGIC * Create 1 endpoint per user in the workspace
# MAGIC * Create 1 database per user in the workspace
# MAGIC * Grant all privileges on the database for the corresponding student (manual)
# MAGIC * Preload the table "flight_delays"

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup $instructions="true"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pre-Load Tables
# MAGIC Run the following cell to pre-load each database with the specified table

# COMMAND ----------

databases = [r[0] for r in spark.sql("SHOW DATABASES").collect()]

for username in usernames:
    db_name = Step.to_db_name(username, DA.naming_template, DA.naming_params)
    if db_name in databases:
        print(f"Creating the table \"{db_name}.flight_delays\" for \"{username}\"")
        spark.sql(f"""DROP TABLE IF EXISTS {db_name}.temp_delays;""")
        spark.sql(f"""DROP TABLE IF EXISTS {db_name}.flight_delays;""")
        spark.sql(f"""CREATE TABLE {db_name}.temp_delays USING CSV OPTIONS (path "dbfs:/mnt/dbacademy-datasets/{DA.data_source_name}/flights/departuredelays.csv", header "true", inferSchema "true");""")
        spark.sql(f"""CREATE TABLE {db_name}.flight_delays AS SELECT * FROM {db_name}.temp_delays;""")
        spark.sql(f"""DROP TABLE {db_name}.temp_delays;""")
