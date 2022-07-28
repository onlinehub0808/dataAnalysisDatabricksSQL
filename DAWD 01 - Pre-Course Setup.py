# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Pre-Course Setup
# MAGIC 
# MAGIC Run this notebook just before class to ensure all assets are ready.
# MAGIC 
# MAGIC The steps include:
# MAGIC 1. Configure User Permissions
# MAGIC 2. Install the Datasets to the Workspace
# MAGIC 3. Create SQL Warehouse
# MAGIC 4. Create User-Specific Databases
# MAGIC 5. Pre-Load Tables
# MAGIC 6. Update User-Specific Grants

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup

# COMMAND ----------

# Setup the widgets to collect required parameters.
dbutils.widgets.dropdown("configure_for", "", configure_for_options, "Configure Workspace For")

# students_count is the reasonable estiamte to the maximum number of students
dbutils.widgets.text("students_count", "", "Number of Students")

# event_name is the name assigned to this event/class or alternatively its class number
dbutils.widgets.text("event_name", "", "Event Name/Class Number")

# COMMAND ----------

# Collect our parameters and create addtional parameters
DA.initialize_workspace_setup()

# COMMAND ----------

# MAGIC %md ## Setup Steps
# MAGIC The following steps document how to configure the workspace for this class. This includes adjusting permissions, installing datasets, creating SQL Warehouses and user-specific databases.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Configure User Entitlements
# MAGIC 
# MAGIC This task simply adds the "**databricks-sql-access**" entitlement to the "**users**" group ensuring that they can access the Databricks SQL view.
# MAGIC 
# MAGIC To complete this step, simply run the following command.

# COMMAND ----------

DA.update_entitlements()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Install the Datasets to the Workspace
# MAGIC This task will copy data from our Azure data repository into the workspace, creating a local copy that all students will share. 
# MAGIC 
# MAGIC This will be "installed" to **/mnt/dbacademy-datasets/data-analysis-with-databricks/v01**.
# MAGIC 
# MAGIC To complete this step, simply run the following command.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/icon_hint_24.png"/> Set `reinstall=True` to force deletion and reinstallation of the datasets for this one course.

# COMMAND ----------

DA.install_datasets(reinstall=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Create SQL Warehouse
# MAGIC This task will create one SQL Warehouse for the whole class. 
# MAGIC 
# MAGIC Upon completion the warehouse will be "**Running**"
# MAGIC 
# MAGIC To complete this step, simply run the following command.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/icon_warn_24.png"/> If the warehouse is created hours-to-days before a class, all warehouses should be stopped so as not to consume unnecissary charges between the point of creation and first usage by students.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/icon_warn_24.png"/> If the warehouse already exists but is not started, it will not be updated and it will not be started - the students or the instructor will be reponsible for starting the warehouse.

# COMMAND ----------

# Creates one per user - please do not students
# DA.create_sql_warehouses()

# Creates one shared warehouse for all students
DA.create_shared_sql_warehouse()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4: Create User-Specific Databases
# MAGIC This task will create one Database per user.
# MAGIC 
# MAGIC To complete this step, simply run the following command.

# COMMAND ----------

DA.create_user_specific_databases()

# COMMAND ----------

# MAGIC %md ### Step 5: Pre-Load Tables
# MAGIC 
# MAGIC This tasks will create tables in each user-specific database and load the prescribed data from **/mnt/dbacademy-datasets/data-analysis-with-databricks**.
# MAGIC 
# MAGIC Run the following cell to pre-load each database with the prescribed tables.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/icon_warn_24.png"/> This operation takes about 1 minute per user - for a class of 20, this could take as long as 20 minutes to complete.

# COMMAND ----------

DA.preload_student_databases()

# COMMAND ----------

# MAGIC %md ### Step 6: Update User-Specific Grants
# MAGIC 
# MAGIC Grant each user access to their personal databases.
# MAGIC 
# MAGIC To comlete this step, simply run the following command.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/icon_warn_24.png"/> This operation should only take 3 minutes.

# COMMAND ----------

DA.update_user_specific_grants()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
