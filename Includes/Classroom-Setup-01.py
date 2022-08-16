# Databricks notebook source
# MAGIC %run ./_common

# COMMAND ----------

configure_for_options = ["", "All Users", "Missing Users Only", "Current User Only"]

# COMMAND ----------

@DBAcademyHelper.monkey_patch
def enumerate_all_databases(self):
    self.all_databases = {d[0] for d in spark.sql("SHOW DATABASES").collect()}

# COMMAND ----------

def drop_all_user_dbs(self):
  
    actual = [u for u in DA.usernames if u in self.all_databases]
  
    print(f"Dropping databases for {len(actual)} users")
    self.do_for_all_users(self.drop_user_db)

    # Refresh the list of databases
    self.enumerate_all_databases()

DBAcademyHelper.monkey_patch(drop_all_user_dbs)

# COMMAND ----------

def drop_user_db(self, username: str) -> None:
    db_name = self.to_dawd_database_name(username)
    if db_name in self.all_databases:
        self.sql(f"DROP DATABASE IF EXISTS {db_name} CASCADE")
        
DBAcademyHelper.monkey_patch(drop_user_db)

# COMMAND ----------

def update_user_grants(self, username: str) -> None:
    self.enumerate_all_databases()
    db_name = self.to_dawd_database_name(username)
    
    if db_name not in self.all_databases:
        print(f"Skipping update of grants for {username}, database {db_name} not found.")
    else:
        spark.sql(f"GRANT ALL PRIVILEGES ON DATABASE `{db_name}` TO `{username}`")
        spark.sql(f"GRANT ALL PRIVILEGES ON ANY FILE TO `{username}`")
        spark.sql(f"ALTER DATABASE {db_name} OWNER TO `{username}`")
    
DBAcademyHelper.monkey_patch(update_user_grants)

# COMMAND ----------

def clone_table(self, db_name, table_name, location):
    name = f"{db_name}.{table_name}"
    table_location = f"{self.paths.datasets}/{location}"
    self.sql(f"CREATE TABLE IF NOT EXISTS {name} SHALLOW CLONE delta.`{table_location}`;")
    
DBAcademyHelper.monkey_patch(clone_table)

# COMMAND ----------

from typing import List
def create_student_database(self, i:int, username:str, drop_existing:bool):
    
    db_name = self.to_dawd_database_name(username)
    db_path = f"dbfs:/mnt/dbacademy-users/{username}/{self.course_name}/database.db"

    if db_name in self.all_databases and drop_existing:
        self.sql(f"DROP DATABASE {db_name} CASCADE;")

    self.sql(f"CREATE DATABASE IF NOT EXISTS {db_name} LOCATION '{db_path}';")

    self.clone_table(db_name, "flight_delays", "flights/departuredelays_delta")
    self.clone_table(db_name, "sales", "retail-org/sales/sales_delta")
    self.clone_table(db_name, "promo_prices", "retail-org/promo_prices/promo_prices_delta")
    self.clone_table(db_name, "sales_orders", "retail-org/sales_orders/sales_orders_delta")
    self.clone_table(db_name, "loyalty_segments", "retail-org/loyalty_segments/loyalty_segment_delta")
    self.clone_table(db_name, "customers", "retail-org/customers/customers_delta")
    self.clone_table(db_name, "suppliers", "retail-org/suppliers/suppliers_delta")
    self.clone_table(db_name, "source_suppliers", "retail-org/suppliers/suppliers_delta")
    self.clone_table(db_name, "gym_logs", "gym_logs/gym_logs_small_delta")

    return f"Created tables for {db_name}"

DBAcademyHelper.monkey_patch(create_student_database)

# COMMAND ----------

def create_student_databases(self, drop_existing:bool):
    results=self.do_for_all_users(lambda username: self.create_student_database(0, username, drop_existing))
    
    # Refresh the final set of databases
    self.enumerate_all_databases()

DBAcademyHelper.monkey_patch(create_student_databases)

# COMMAND ----------

def create_sql_warehouses(self):
    """Creates one warehouse per user"""
    
    from dbacademy.dbrest.sql.endpoints import CLUSTER_SIZE_2X_SMALL
    
    self.client.sql().endpoints().create_user_endpoints(naming_template=self.naming_template,      # Required
                                                        naming_params=self.naming_params,          # Required
                                                        cluster_size = CLUSTER_SIZE_2X_SMALL,      # Required
                                                        enable_serverless_compute = False,         # Required
                                                        tags = {                                     
                                                            "dbacademy.course": self.clean_string(self.course_name),  # Tag the name of the course
                                                            "dbacademy.source": self.clean_string(self.course_name),  # Tag the name of the course
                                                        },  
                                                        users=self.usernames)                      # Restrict to the specified list of users

DBAcademyHelper.monkey_patch(create_sql_warehouses)

# COMMAND ----------

def create_shared_sql_warehouse(self):
    import re
    from dbacademy.dbrest.sql.endpoints import COST_OPTIMIZED, RELIABILITY_OPTIMIZED, CHANNEL_NAME_CURRENT, CLUSTER_SIZE_2X_SMALL

    name = "Starter Warehouse"
    
    warehouse = self.client.sql.endpoints.create_or_update(
        name = name,
        cluster_size = CLUSTER_SIZE_2X_SMALL,
        enable_serverless_compute = False, # Due to a bug with Free-Trial workspaces
        min_num_clusters = 2,  #self.autoscale_min,
        max_num_clusters = 20, #self.autoscale_max,
        auto_stop_mins = 120,
        enable_photon = True,
        spot_instance_policy = RELIABILITY_OPTIMIZED,
        channel = CHANNEL_NAME_CURRENT,
        tags = {
            "dbacademy.event_name":     self.clean_string(self.event_name),
            "dbacademy.students_count": self.clean_string(self.students_count),
            "dbacademy.workspace":      self.clean_string(self.workspace),
            "dbacademy.org_id":         self.clean_string(self.org_id),
        })

    warehouse_id = warehouse.get("id")

    # # With the warehouse created, make sure that all users can attach to it.
    self.client.permissions.warehouses.update_group(warehouse_id, "users", "CAN_USE")

    print(f"Created \"{name}\"")
    
DBAcademyHelper.monkey_patch(create_shared_sql_warehouse)

# COMMAND ----------

def update_entitlements(self):
    group = self.client.scim.groups.get_by_name("users")
    self.client.scim.groups.add_entitlement(group.get("id"), "databricks-sql-access")
        
DBAcademyHelper.monkey_patch(update_entitlements)

# COMMAND ----------

def update_user_specific_grants(self):
    from dbacademy.dbrest import DBAcademyRestClient
    
    job_name = "DA-DAWD-Configure-Permissions"
    print(f"Starting job \"{job_name}\" to grant users access to their database")
    
    self.client.jobs().delete_by_name(job_name, success_only=False)

    if dbgems.get_notebook_dir().endswith("/Includes"):
        # For CloudLabs Setup
        notebook_path = f"{dbgems.get_notebook_dir()}/Configure-Permissions"
    else:
        # For Instructor setup
        notebook_path = f"{dbgems.get_notebook_dir()}/Includes/Configure-Permissions"

    params = {
        "name": job_name,
        "tags": {
            "dbacademy.course": self.clean_string(self.course_name),
            "dbacademy.source": self.clean_string(self.course_name)
        },
        "email_notifications": {},
        "timeout_seconds": 7200,
        "max_concurrent_runs": 1,
        "format": "MULTI_TASK",
        "tasks": [
            {
                "task_key": "Configure-Permissions",
                "description": "Configure all users's permissions for user-specific databases.",
                "libraries": [],
                "notebook_task": {
                    "notebook_path": notebook_path,
                    "base_parameters": []
                },
                "new_cluster": {
                    "num_workers": "0",
                    "spark_conf": {
                        "spark.master": "local[*]",
                        "spark.databricks.acl.dfAclsEnabled": "true",
                        "spark.databricks.repl.allowedLanguages": "sql,python",
                        "spark.databricks.cluster.profile": "serverless",
                    },
                    "runtime_engine": "STANDARD",
                    "spark_env_vars": {
                        "WSFS_ENABLE_WRITE_SUPPORT": "true"
                    },
                },
            },
        ],
    }
    cluster_params = params.get("tasks")[0].get("new_cluster")
    cluster_params["spark_version"] = self.client.clusters().get_current_spark_version()
    
    if self.client.clusters().get_current_instance_pool_id() is not None:
        cluster_params["instance_pool_id"] = self.client.clusters().get_current_instance_pool_id()
    else:
        cluster_params["node_type_id"] = self.client.clusters().get_current_node_type_id()
        if dbgems.get_cloud() == "AWS":
            cluster_params["aws_attributes"] = { "availability": "ON_DEMAND" }
        
    create_response = self.client.jobs().create(params)
    job_id = create_response.get("job_id")

    run_response = self.client.jobs().run_now(job_id)
    run_id = run_response.get("run_id")

    final_response = self.client.runs().wait_for(run_id)
    
    final_state = final_response.get("state").get("result_state")
    assert final_state == "SUCCESS", f"Expected the final state to be SUCCESS, found {final_state}"
    
    print()
    print("Update completed successfully.")
    return job_name

DBAcademyHelper.monkey_patch(update_user_specific_grants)

# COMMAND ----------

def get_widget_or_else(self, name, default=None):
    from py4j.protocol import Py4JJavaError
    try:
        result = dbutils.widgets.get(name)
        return result or default
    except Py4JJavaError as ex:
        if "InputWidgetNotDefined" not in ex.java_exception.getClass().getName():
            raise ex
        else:
            return default

DBAcademyHelper.monkey_patch(get_widget_or_else)

# COMMAND ----------

def initialize_workspace_setup(self):
    import re

    valid_configure_for_options = configure_for_options[1:] # all but empty-string
    
    # Special logic for when we are running under test.
    is_smoke_test = spark.conf.get("dbacademy.smoke-test", "false") == "true"
    
    if is_smoke_test:     self.configure_for = self.get_widget_or_else("configure_for", "Current User Only")
    elif dbgems.is_job(): self.configure_for = self.get_widget_or_else("configure_for", "Missing Users Only")
    else:                 self.configure_for = dbutils.widgets.get("configure_for")
    
    assert self.configure_for in ["All Users", "Missing Users Only", "Current User Only"], f"Who the workspace is being configured for must be specified, found \"{self.configure_for}\". Options include {valid_configure_for_options}"

    if self.configure_for == "Current User Only":
        # Override for the current user only
        self.usernames = [self.username]

    elif self.configure_for == "Missing Users Only":
        # The presumption here is that if the user doesn't have their own
        # database, then they are also missing the rest of their config.
        missing_users = []
        for user in self.usernames:
            db_name = self.to_dawd_database_name(user)
            if db_name not in self.all_databases:
                missing_users.append(user)

        self.usernames = missing_users

    self.students_count = dbutils.widgets.get("students_count").strip()
    user_count = len(self.client.scim.users.list())

    if self.students_count.isnumeric():
        self.students_count = int(self.students_count)
        self.students_count = max(self.students_count, user_count)
    else:
        self.students_count = user_count

    self.workspace = dbgems.get_browser_host_name()
    if not self.workspace: self.workspace = dbgems.get_notebooks_api_endpoint()
    self.org_id = dbgems.get_tag("orgId", "unknown")

    import math
    self.autoscale_min = 1 if is_smoke_test else math.ceil(self.students_count/20)
    self.autoscale_max = 1 if is_smoke_test else math.ceil(self.students_count/5)

    self.event_name = "Smoke Test" if is_smoke_test else dbutils.widgets.get("event_name")
    assert self.event_name is not None and len(self.event_name) >= 3, f"The event_name must be specified with min-length of 3, found \"{self.event_name}\"."
    self.event_name = re.sub("[^a-zA-Z0-9]", "_", self.event_name)        
    while "__" in self.event_name: self.event_name = self.event_name.replace("__", "_")
        
    print(f"Event Name:        {self.event_name}")
    print(f"Configured for:    {self.configure_for}")
    print(f"Student Count:     {self.students_count}")
    print(f"Provisioning:      {len(self.usernames)}")
    print(f"Autoscale minimum: {self.autoscale_min}")
    print(f"Autoscale maximum: {self.autoscale_max}")
    if is_smoke_test:
        print(f"Smoke Test:        {is_smoke_test} ")

DBAcademyHelper.monkey_patch(initialize_workspace_setup)

# COMMAND ----------

@DBAcademyHelper.monkey_patch
def setup_completed(self):
    print(f"\nSetup completed in {int(time.time())-setup_start} seconds")

# COMMAND ----------

DA = DBAcademyHelper(**helper_arguments)
DA.load_all_usernames()
DA.enumerate_all_databases()
DA.conclude_setup()

