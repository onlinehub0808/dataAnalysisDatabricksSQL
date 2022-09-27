# Databricks notebook source
# MAGIC %run ./_common

# COMMAND ----------

@DBAcademyHelper.monkey_patch
def create_user_databases(self, drop_existing=False):
    self.workspace.databases.create_databases(drop_existing=drop_existing, 
                                              post_create=self.populate_database)

# COMMAND ----------

@DBAcademyHelper.monkey_patch
def update_entitlements(self):
    group = self.client.scim.groups.get_by_name("users")
    self.client.scim.groups.add_entitlement(group.get("id"), "databricks-sql-access")
        

# COMMAND ----------

@DBAcademyHelper.monkey_patch
def setup_completed(self):
    print(f"\nSetup completed in {int(time.time())-setup_start} seconds")

# COMMAND ----------

DA = DBAcademyHelper(**helper_arguments)
DA.reset_environment()
# DA.init(install_datasets=False, create_db=True)
# DA.conclude_setup()

