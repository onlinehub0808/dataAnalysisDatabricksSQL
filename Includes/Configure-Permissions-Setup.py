# Databricks notebook source
# MAGIC %run ./_common

# COMMAND ----------

@DBAcademyHelper.monkey_patch
def update_user_grants(self, username: str):
    db_name = self.to_dawd_database_name(username)
    
    if db_name not in self.workspace.existing_databases:
        print(f"Skipping update of grants for {username}, database {db_name} not found.")
    else:
        spark.sql(f"GRANT ALL PRIVILEGES ON DATABASE `{db_name}` TO `{username}`")
        spark.sql(f"GRANT ALL PRIVILEGES ON ANY FILE TO `{username}`")
        spark.sql(f"ALTER DATABASE {db_name} OWNER TO `{username}`")
    

# COMMAND ----------

DA = DBAcademyHelper(**helper_arguments)
# DA.reset_environment()
# DA.init(install_datasets=True, create_db=True)
DA.conclude_setup()

