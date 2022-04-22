# Databricks notebook source
# MAGIC %md 
# MAGIC # Lab: Tables and Views on Databricks SQL

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup

# COMMAND ----------

step = DA.publisher.add_step(False, instructions="""    

    <h2><img class="image-icon-inline" src="https://s3.us-west-2.amazonaws.com/files.training.databricks.com/images/step-icon_small.png" alt="heading-icon" />
    Lab Objective</h2>
    <div class="instructions-div">
    <p>At the end of this lab, you will be able to:</p>
    <ul>
    <li>Use Databricks SQL to create tables and views</li>
    </ul></div>
    
    """, statements=None) 

step.render(DA.username)
step.execute(DA.username)

# COMMAND ----------

step = DA.publisher.add_validation(instructions="""

<h2><img class="image-icon-inline" src="https://s3.us-west-2.amazonaws.com/files.training.databricks.com/images/step-icon_small.png" alt="heading-icon" />
Create External Tables</h2>
    <div class="instructions-div">
    <p>In this part of the lab, you are going to create an external table using a dataset located at 'wasbs://courseware@dbacademy.blob.core.windows.net/data-analysis-with-databricks/v01/sales/'.</p>
    <p>Complete the following:</p>
    <ol>
        <li>Make the required changes to the query below</li>
        <li>Run the query in Databricks SQL</li>
        <li>Check your work by entering your answer to the question</li>
        <li>After pressing <span class="monofont">ENTER/RETURN</span>, green indicates a correct answer, and red indicates incorrect</li>
    </ol>
    <p><pre><span class="monofont">USE <span style="color:red;">FILL_IN</span>;
DROP TABLE IF EXISTS sales_external;
CREATE <span style="color:red;">FILL_IN</span> sales_external USING DELTA
    <span style="color:red;">FILL_IN</span> 'wasbs://courseware@dbacademy.blob.core.windows.net/data-analysis-with-databricks/v01/sales/';</span>
SELECT * FROM sales_external ORDER BY customer_id;</pre></p>
    </div>
""", statements=["DROP TABLE IF EXISTS sales_external;",
                 "CREATE TABLE sales_external USING DELTA LOCATION 'wasbs://courseware@dbacademy.blob.core.windows.net/data-analysis-with-databricks/v01/sales/';", 
                 "SELECT * FROM sales_external ORDER BY customer_id;"], 
     label="""What is the value of <span class="monofont">customer_id</span> in the first row? """, 
     expected="12096776", 
     length=10)

step.render(DA.username)
step.execute(DA.username)

# COMMAND ----------

step = DA.publisher.add_validation(instructions="""

<h2><img class="image-icon-inline" src="https://s3.us-west-2.amazonaws.com/files.training.databricks.com/images/step-icon_small.png" alt="heading-icon" />
Create Managed Table</h2>
    <div class="instructions-div">
    <p>The data for the table we created above is in a read-only object store. We want to copy the data into a local, managed table here in our workspace. In this part of the lab, we are going to create a managed table using a subquery, which we will learn about later in this course, to accomplish this.</p>
    <p>Complete the following:</p>
    <ol>
        <li>Make the required changes to the query below</li>
        <li>Run the query in Databricks SQL</li>
        <li>Check your work by entering your answer to the question</li>
        <li>After pressing <span class="monofont">ENTER/RETURN</span>, green indicates a correct answer, and red indicates incorrect</li>
    </ol>
    <p><pre><span class="monofont">USE <span style="color:red;">FILL_IN</span>;
<span style="color:red;">FILL_IN</span> TABLE sales USING DELTA AS
    SELECT * FROM sales_external;</span>
DESCRIBE <span style="color:red;">FILL_IN</span> sales;</pre></p>
    </div>


""", statements=["CREATE OR REPLACE TABLE sales AS SELECT * FROM sales_external;", "DESCRIBE EXTENDED sales;"], label="""What is the value of <span class="monofont">Owner</span>? """, expected="root", length=10)

step.render(DA.username)
step.execute(DA.username)

# COMMAND ----------

step = DA.publisher.add_validation(instructions="""

<h2><img class="image-icon-inline" src="https://s3.us-west-2.amazonaws.com/files.training.databricks.com/images/step-icon_small.png" alt="heading-icon" />
Use Data Explorer</h2>
    <div class="instructions-div">
    <p>In this part of the lab, you will use the Data Explorer to find the answer to the question below.</p>
    <p>Complete the following:</p>
    <ol>
        <li>Click "Data" in the sidebar menu to go to the Data Explorer</li>
        <li>Check to the right of 'hive_metastore'. If your schema isn't selected, click on "default" and in the drop down menu, and select it.</li>
        <li>Select the table, "sales" from the list</li>
        <li>Feel free to explore the tabs to see more information about this table</li>
        <li>When you are ready, enter your answer to the question below</li>
        <li>After pressing <span class="monofont">ENTER/RETURN</span>, green indicates a correct answer, and red indicates incorrect</li>
    </ol>
    </div>


""", statements=None, label="""Under History, what was the operation associated with Version 0 of the table? """, expected="create or replace table as select", length=10)

step.render(DA.username)
step.execute(DA.username)

# COMMAND ----------

step = DA.publisher.add_validation(instructions="""

<h2><img class="image-icon-inline" src="https://s3.us-west-2.amazonaws.com/files.training.databricks.com/images/step-icon_small.png" alt="heading-icon" />
Drop External Table, then create new one to confirm still have data files</h2>
    <div class="instructions-div">
    <p>We are going to keep the managed table, but we need to drop the external table.</p>
    <p>Complete the following:</p>
    <ol>
        <li>Make the required changes to the query below to drop the table, <span class="monofont">sales_external</span> from the schema</li>
        <li>Run the query in Databricks SQL</li>
        <li>Confirm can no longer view metadata via 'DESCRIBE' command</li>
        <li>Re-create the table with new name 'external_table2'</li>
        <li>Run 'SELECT' to confirm the data files return answer set</li>
        <li>Check your work by entering your answer to the question</li>
        <li>After pressing <span class="monofont">ENTER/RETURN</span>, green indicates a correct answer, and red indicates incorrect</li>
    </ol>
    <p><pre><span class="monofont">USE <span style="color:red;">FILL_IN</span>;
<span style="color:red;">FILL_IN</span> TABLE sales_external;
CREATE OR REPLACE TABLE {db_name}.external_table2
LOCATION 'dbfs:/mnt/dbacademy-datasets/data-analysis-with-databricks/v01/sales';
DESCRIBE EXTENDED {db_name}.external_table2;
SELECT * FROM markott_da_custom_loc.external_table2;

</pre></p>
    </div>


""", statements=["CREATE OR REPLACE TABLE sales_managed AS SELECT * FROM sales;"], label="""Where you able to point to data files from table you DROPPED earlier? (yes or no)""", expected="yes", length=10)

step.render(DA.username)
step.execute(DA.username)

# COMMAND ----------

step = DA.publisher.add_validation(instructions="""

<h2><img class="image-icon-inline" src="https://s3.us-west-2.amazonaws.com/files.training.databricks.com/images/step-icon_small.png" alt="heading-icon" />
Drop Tables</h2>
    <div class="instructions-div">
    <p>We are going to keep the managed table, but we need to drop the external table.</p>
    <p>Complete the following:</p>
    <ol>
        <li>Make the required changes to the query below to drop the table, <span class="monofont">sales_external</span> from the schema</li>
        <li>Run the query in Databricks SQL</li>
        <li>'DESCRIBE' command should fail since metadata has been removed</li>
        <li>Check your work by entering your answer to the question</li>
        <li>After pressing <span class="monofont">ENTER/RETURN</span>, green indicates a correct answer, and red indicates incorrect</li>
    </ol>
    <p><pre><span class="monofont">USE <span style="color:red;">FILL_IN</span>;
<span style="color:red;">FILL_IN</span> TABLE sales_external;
DESCRIBE <span style="color:red;">FILL_IN</span> sales_external;</pre></p>
    </div>


""", statements=["DROP TABLE sales_managed;"], label="""Did the 'DESCRIBE' command fail? (yes or no) """, expected="yes", length=10)
# The DESCRIBE command is not checked in the code above because it is expected to fail.

step.render(DA.username)
step.execute(DA.username)

# COMMAND ----------

DA.validate_datasets()
html = DA.publisher.publish()
displayHTML(html)

