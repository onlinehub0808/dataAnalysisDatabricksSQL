-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Your Second Lesson
-- MAGIC ## SQL Notebook
-- MAGIC 
-- MAGIC Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.

-- COMMAND ----------

-- MAGIC %run ./Includes/Classroom-Setup-02

-- COMMAND ----------

-- Arbitrary commment before directive
-- DUMMY
SELECT 'This is SQL from an SQL Notebook' AS comment

-- COMMAND ----------

-- MAGIC %scala // Arbitrary commment after magic command
-- MAGIC // DUMMY
-- MAGIC println("This is Scala from a SQL Notebook")

-- COMMAND ----------

-- MAGIC %r # Arbitrary commment after magic command
-- MAGIC # DUMMY
-- MAGIC print("This is R from a SQL Notebook")

-- COMMAND ----------

-- MAGIC %python # Arbitrary commment after magic command
-- MAGIC # DUMMY
-- MAGIC print("This is Python from a SQL Notebook")

-- COMMAND ----------

-- MAGIC %md -- Arbitrary commment after magic command
-- MAGIC -- DUMMY
-- MAGIC 
-- MAGIC This is Markdown from a Python notebook

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
