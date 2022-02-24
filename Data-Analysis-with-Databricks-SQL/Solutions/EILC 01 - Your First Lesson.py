# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Your First Lesson
# MAGIC Using Classroom-Setup
# MAGIC 
# MAGIC This lesson demonstrates the class "Classroom-Setup" strategy when a more complex/custom setup is required compared to the **`DBAcademyHelper`** installed from GitHub.
# MAGIC 
# MAGIC This **`Classroom-Setup`** provides the minimum setup for a course which includes:
# MAGIC * A custom **`DBAcademyHelper`** class that you can customize
# MAGIC * A copy of our **`install_datasets(..)`** utility method for downloading datasets from our repo in Azure and into the user's workspace
# MAGIC * A demonstration of how to use a **`Path`** object for better organization
# MAGIC * A demonstration of how to inject values into the spark-config for access via SQL commands

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup-01"

# COMMAND ----------

# MAGIC %md For better obfuscation, readability and teachability, everything is wrapped in a **`DBAcademyHelper`** object and two instances are provided by default, **`DBAcademy`** and **`DA`** for a shorter command string

# COMMAND ----------

print(DA)
print(DBAcademy)

# COMMAND ----------

print(f"DBAcademy.user_db:         {DBAcademy.user_db}")
print(f"DBAcademy.working_dir:     {DBAcademy.working_dir}")
print("-"*80)
print(f"DA.paths.working_dir:      {DA.paths.working_dir}")
print(f"DA.paths.people_with_dups: {DA.paths.people_with_dups}")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   '${da.user_db}' as user_db,
# MAGIC   '${da.working_dir}' as working_dir,
# MAGIC   '${da.paths.people_with_dups}' as some_path

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM text.`${da.paths.people_with_dups}`;

# COMMAND ----------

# MAGIC %md # Testing
# MAGIC The remainder of this notebook demonstrates different build patterns, processing commands or in general is added for the sake of testing the build and testing tools

# COMMAND ----------

# Arbitrary commment before directive
# DUMMY: Ya, that wasn't too smart. Then again, this is just a dummy-directive
print("This is Python from an Python Notebook")

# COMMAND ----------

# MAGIC %scala // Arbitrary commment after magic command
# MAGIC // DUMMY: Ya, that wasn't too smart. Then again, this is just a dummy-directive
# MAGIC println("This is Scala from a Python Notebook")

# COMMAND ----------

# MAGIC %r # Arbitrary commment after magic command
# MAGIC # DUMMY: Ya, that wasn't too smart. Then again, this is just a dummy-directive
# MAGIC print("This is R from a Python Notebook")

# COMMAND ----------

# MAGIC %sql -- Arbitrary commment after magic command
# MAGIC -- DUMMY: Ya, that wasn't too smart. Then again, this is just a dummy-directive
# MAGIC SELECT 'This is SQL from a Python notebook' AS comment

# COMMAND ----------

# MAGIC %md -- Arbitrary commment after magic command
# MAGIC -- DUMMY: Ya, that wasn't too smart. Then again, this is just a dummy-directive
# MAGIC 
# MAGIC This is Markdown from a Python notebook

# COMMAND ----------

# MAGIC %md
# MAGIC Remove our any temporary files and the database created in this lesson

# COMMAND ----------

DA.cleanup()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
