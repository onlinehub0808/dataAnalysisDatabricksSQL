# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md 
# MAGIC # Demo: Schemas, Tables, and Views on Databricks SQL

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup

# COMMAND ----------

step = DA.publisher.add_step(False, instructions="""    

    <h2><img class="image-icon-inline" src="https://s3.us-west-2.amazonaws.com/files.training.databricks.com/images/step-icon_small.png" alt="heading-icon" />
    Lesson Objective</h2>
    <div class="instructions-div">
    <p>At the end of this lesson, you will be able to:</p>
    <ul>
    <li>Describe how to use Databricks SQL to create schemas, tables, and views</li>
    </ul></div>
    
    """, statements=None) 

step.render(DA.username)
step.execute(DA.username)

# COMMAND ----------

step = DA.publisher.add_step(False, instructions="""    

<h2><img class="image-icon-inline" src="https://s3.us-west-2.amazonaws.com/files.training.databricks.com/images/step-icon_small.png" alt="heading-icon" />
Create Schema</h2>
    <div class="instructions-div">
    <p>I want to show you how to create a new schema (database). In your organization, you may not have the proper permissions to do this, and in this course, you don't have the proper permission, but I wanted to show you how to do this just the same.</p>
    <ol>
        <li>Run the code below.</li>
    </ol>
    <p>The code runs three statements. The first drops the schema just in case we are running this twice. The second creates the schema. Note that there is no <span class="monofont">CATALOG</span> provided. Databricks is set up to use a default catalog, and this is set up by your Databricks Administrator.</p>
    <p>The second statement runs a <span class="monofont">DESCRIBE SCHEMA EXTENDED</span>, which gives us information about the schema, including the location where managed table data will be stored.</p>

    </div>
    
    """, statements=["DROP SCHEMA IF EXISTS {db_name}_schema CASCADE;",
                     "CREATE SCHEMA IF NOT EXISTS {db_name}_schema;", 
                     "DESCRIBE SCHEMA EXTENDED {db_name}_schema;"]) 

step.render(DA.username)
step.execute(DA.username)

# COMMAND ----------

step = DA.publisher.add_step(True, instructions="""    

<h2><img class="image-icon-inline" src="https://s3.us-west-2.amazonaws.com/files.training.databricks.com/images/step-icon_small.png" alt="heading-icon" />
Managed Tables</h2>
<h3>Create Table</h3>
    <div class="instructions-div">
    <p>From here on out, you have the proper permission level to complete what I'm demonstrating for you here.</p>
    <p>Let's create a managed table in our schema and insert some sample data.</p>
    <p>Note that I have "<span class="monofont">USING DELTA</span>" at the end of the <span class="monofont">CREATE</span> statment. This is optional because Delta is the default table type.</p>
    <ol start="3">
        <li>Run the code below.</li>
    </ol>
    <p></p>
    </div>
    
    """, statements=["CREATE OR REPLACE TABLE managed_table (width INT, length INT, height INT) USING DELTA;",
                     "INSERT INTO managed_table VALUES (3, 2, 1);",
                     "SELECT * FROM managed_table;"]) 

step.render(DA.username)
step.execute(DA.username)

# COMMAND ----------

step = DA.publisher.add_step(False, instructions="""    

<h3>View Table Information</h3>
    <div class="instructions-div">
    <ol start="4">
        <li>Run the code below to see information about the table we just created.</li>
    </ol>
    <p>Note that the table is a managed table. When we drop this table, our data will be deleted. Note also that this is a Delta table.</p>
    </div>
    
    """, statements="DESCRIBE EXTENDED managed_table;") 

step.render(DA.username)
step.execute(DA.username)

# COMMAND ----------

step = DA.publisher.add_step(False, instructions="""    

<h3>DROP the Table</h3>
    <div class="instructions-div">
    <ol start="5">
        <li>Run the code below to drop the table.</li>
    </ol>
    </div>
    
    """, statements="DROP TABLE IF EXISTS managed_table;") 

step.render(DA.username)
step.execute(DA.username)

# COMMAND ----------

step = DA.publisher.add_step(True, instructions="""    

<h2><img class="image-icon-inline" src="https://s3.us-west-2.amazonaws.com/files.training.databricks.com/images/step-icon_small.png" alt="heading-icon" />
External Tables</h2>
    <div class="instructions-div">
    <p>There is a dataset, called Sales, that is currently in an object store. In order to keep from having all of you access the dataset over the internet, all at the same time, it is currently available as a path in the file system. Running the command below creates an external table that is associated with this dataset.</p>
    <ol start="5">
        <li>Run the code below.</li>
    </ol>
    <p>The table's data is stored in the external location, but the table itself is registered in the metastore. We can query the data just like any other table in the schema.</p>
    </div>
    
    """, statements=["""CREATE TABLE external_table
    LOCATION 'dbfs:/mnt/dbacademy-datasets/data-analysis-with-databricks/v01/sales';""",
    "SELECT * FROM external_table;"]) 

step.render(DA.username)
step.execute(DA.username)

# COMMAND ----------

step = DA.publisher.add_step(True, instructions="""    

<h3>View Table Information</h3>
    <div class="instructions-div">
    <p>Let's look at the table's information</p>
    <ol start="6">
        <li>Run the code below.</li>
    </ol>
    <p>Note that the table's type is <span class="monofont">EXTERNAL</span>, and the table's location points to the file system. Dropping this table will not affect the data in this location.</p>
    </div>
    
    """, statements="DESCRIBE EXTENDED external_table;") 

step.render(DA.username)
step.execute(DA.username)

# COMMAND ----------

step = DA.publisher.add_step(True, instructions="""    

<h3>Dropping External Tables</h3>
    <div class="instructions-div">
    <p>The command below will drop the table from the schema.</p>
    <ol start="7">
        <li>Run the code below to drop the table.</li>
    </ol>
    <p>Note that we dropped the table, so we won't be able to query the data using the kind of <span class="monofont">SELECT</span> query you may be used to using.</p>
    </div>
    
    """, statements="DROP TABLE IF EXISTS external_table;") 

step.render(DA.username)
step.execute(DA.username)

# COMMAND ----------

step = DA.publisher.add_step(False, instructions="""    

<h3>Dropping External Tables Does Not Delete Data</h3>
    <div class="instructions-div">
    <ol start="8">
        <li>Run the code below.</li>
    </ol>
    <p>Even though we dropped the table, we are still able to query the data directly in the filesystem because it still exists in the object store.</p>
    <p>Now, this is one of the coolest features of Databricks SQL. We've talked about how the use of schemas and tables is just an organizational contruct. The data files located in this location can be queried directly, even though they are not part of a table or schema. We use tables and schemas simply to organize data in a way familiar to you.</p>
    </div>
    
    """, statements="SELECT * FROM delta.`dbfs:/mnt/dbacademy-datasets/data-analysis-with-databricks/v01/sales`;") 

step.render(DA.username)
step.execute(DA.username)

# COMMAND ----------

step = DA.publisher.add_step(include_use=True, instructions="""    

<h2><img class="image-icon-inline" src="https://s3.us-west-2.amazonaws.com/files.training.databricks.com/images/step-icon_small.png" alt="heading-icon" />
Views</h2>
    <div class="instructions-div">
    <p>Views can be created from other views, tables, or data files. In the code below, we are creating a view from the data in the data file we used above.</p>
    <ol start="9">
        <li>Run the code below.</li>
    </ol>
    <p>The view now gives us all the sales that totaled more than 10,000. If new rows are added to the Sales data file, the view will update every time it's run.</p>
    </div>
    
    """, statements=[
    """CREATE OR REPLACE VIEW high_sales AS
    SELECT * FROM delta.`dbfs:/mnt/dbacademy-datasets/data-analysis-with-databricks/v01/sales` 
        WHERE total_price > 10000;""",
    "SELECT * FROM high_sales;"]) 

step.render(DA.username)
step.execute(DA.username)

# COMMAND ----------

step = DA.publisher.add_step(True, instructions="""    

<h2><img class="image-icon-inline" src="https://s3.us-west-2.amazonaws.com/files.training.databricks.com/images/step-icon_small.png" alt="heading-icon" />
Common Table Expressions (CTEs)</h2>
    <div class="instructions-div">
    <p>Here is an example of a CTE. We are going to query the view we just created, but we are going to filter based on the <span class="monofont">total_price</span> being above 20000. We are calling that <span class="monofont">sales_below_20000 and then immediately querying the <span class="monofont">DISTINCT customer_name</span> from that CTE.</p>

    </div>
    
    """, statements="""WITH sales_below_20000 AS
    (SELECT *
     FROM high_sales
     WHERE total_price < 20000)
    SELECT DISTINCT customer_name FROM sales_below_20000;""") 

step.render(DA.username)
step.execute(DA.username)

# COMMAND ----------

DA.validate_datasets()
html = DA.publisher.publish()
displayHTML(html)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
