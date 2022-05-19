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
# MAGIC 3. Create SQL Endpoints
# MAGIC 4. Create User-Specific Databases
# MAGIC 5. Create DB SQL Query for User Grants
# MAGIC 6. Update User Grants
# MAGIC 7. Pre-Load Tables

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup

# COMMAND ----------

# MAGIC %md ## DBAcademy Toolbox
# MAGIC Available from GitHub (see [databricks-academy/dbacademy-toolbox](dbacademy-toolbox)), the toolbox can be used to help manage the learning environment with tooling to support things like downloading the datasets, creating, deleting, starting endpoints, creating databsaes, and much more.
# MAGIC 
# MAGIC While optional, the following cell will install the toolbox and make it available to you in this workspace.

# COMMAND ----------

# Optionally install (and use) the toolbox to help manage the workspace.
# DA.install_toolbox()

# COMMAND ----------

# MAGIC %md ## Setup Steps
# MAGIC The following steps document how to configure the workspace for this class. This includes adjusting permissions, installing datasets, creating SQL Endpoints and user-specific databases.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Configure User Permissions
# MAGIC * Open the **Admin Console** from the **Data Science & Engineering** view
# MAGIC * Select the **Groups** tab
# MAGIC * Select the **users** group
# MAGIC * Select the **Entitlements** tab
# MAGIC * Select only the **Databricks SQL access** entitlement
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/icon_note_24.png"/> If you do not see the Admin Console, it is most likely due to the fact that you lack admin priviledges in the workspace which is required to complete the setup.

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
# MAGIC ### Step 3: Create SQL Endpoints
# MAGIC This task will create one SQL Endpoint per user. 
# MAGIC 
# MAGIC Upon completion the endpoint will be "**Running**"
# MAGIC 
# MAGIC To complete this step, simply run the following command.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/icon_warn_24.png"/> If the endpoint is created hours-to-days before a class, all endpoints should be stopped so as not to consume unnecissary charges between the point of creation and first usage by students.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/icon_warn_24.png"/> If the endpoint already exists but is not started, it will not be recreated and it will not be started - the students or the instructor will be reponsible for starting the endpoints.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/icon_hint_24.png"/> The **DBAcademy Toolbox** includes advanced batch features for creating, starting, stopping and deleting of endpoints should they be needed.

# COMMAND ----------

DA.create_sql_endpoints()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4: Create User-Specific Databases
# MAGIC This task will create one Database per user.
# MAGIC 
# MAGIC To complete this step, simply run the following command.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/icon_hint_24.png"/> The **DBAcademy Toolbox** includes advanced batch features for issuing grants to, creating and dropping user-specific databases should they be needed.

# COMMAND ----------

create_user_specific_databases(DA)

# COMMAND ----------

# MAGIC %md ### Step 5: Create DB SQL Query for User Grants
# MAGIC 
# MAGIC This task will create a Databricks SQL Query containing all the required grant statements which must be executed [manually] to effect the changes expressed in the query.
# MAGIC 
# MAGIC To complete this step simply run the following command.

# COMMAND ----------

DA.create_user_specific_grants()

# COMMAND ----------

# MAGIC %md ### Step 6: Update User Grants
# MAGIC 
# MAGIC With the query created, we now need to open the query in Databricks SQL and execute it.
# MAGIC 
# MAGIC To complete this step:
# MAGIC 1. Click the provided link above to open the query
# MAGIC 2. Attach the query to your cluster
# MAGIC 3. Execute the query updating each user's grants

# COMMAND ----------

# MAGIC %md ### Step 7: Pre-Load Tables
# MAGIC 
# MAGIC This tasks will create tables in each user-specific database and load the prescribed data from **/mnt/dbacademy-datasets/data-analysis-with-databricks**.
# MAGIC 
# MAGIC Run the following cell to pre-load each database with the prescribed tables.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/icon_warn_24.png"/> This operation takes about 1 minute per user - for a class of 20, this could take as long as 20 minutes to complete.

# COMMAND ----------

DA.preload_student_databases()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
