# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md 
# MAGIC # Lab: Ingesting Data

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup

# COMMAND ----------

step = DA.publisher.add_step(False, instructions="""    

    <h2><img class="image-icon-inline" src="https://s3.us-west-2.amazonaws.com/files.training.databricks.com/images/step-icon_small.png" alt="heading-icon" />
    Lesson Objective</h2>
    <div class="instructions-div">
    <p>At the end of this lesson, you will be able to:</p>
    <ul>
    <li>Use Databricks SQL to ingest data</li>
    </ul></div>
    
    """, statements=None)

step.render(DA.username)
step.execute(DA.username)

# COMMAND ----------

step = DA.publisher.add_validation(instructions="""

<h2><img class="image-icon-inline" src="https://s3.us-west-2.amazonaws.com/files.training.databricks.com/images/step-icon_small.png" alt="heading-icon" />
Ingest Data in Databricks SQL</h2>
    <div class="instructions-div">
    <p>In the previous lesson, we ingested data from a .csv file in two steps to create a Delta table. In this lab, we are going to run a single query, with a handful of statements, to complete the same process.</p>
    <p>Databricks SQL will count the number of rows returned from a table automatically. However by default, only 1000 rows are returned. To have all rows returned from a table, deselect the checkbox <span class="monofont">LIMIT 1000</span> just below the query editor.</p>
    <p>Complete the following:</p>
    <ol>
        <li>Make the required changes to the query below</li>
        <li>Run the query in Databricks SQL</li>
        <li>Check your work by entering your answer to the question</li>
        <li>After pressing <span class="monofont">ENTER/RETURN</span>, green indicates a correct answer, and red indicates incorrect</li>
    </ol>
    <p><pre>USE <span style="color:red">FILL_IN</span>;
CREATE OR REPLACE TABLE customers_csv 
    USING csv 
    OPTIONS (
        path='wasb://courseware@dbacademy.blob.core.windows.net/data-analysis-with-databricks/v01/customers/customers.csv',
        header="true",
        inferSchema="true"
);
CREATE OR REPLACE TABLE customers AS
    SELECT * FROM customers_csv;
DROP TABLE customers_csv;
SELECT * FROM customers;</pre></p>
    </div>


""", statements=["""CREATE TABLE customers_csv 
    USING csv 
    OPTIONS (
        path='wasb://courseware@dbacademy.blob.core.windows.net/data-analysis-with-databricks/v01/customers/customers.csv',
        header="true",
        inferSchema="true"
    );""", """CREATE OR REPLACE TABLE customers AS
    SELECT * FROM customers_csv;""", "DROP TABLE customers_csv;", "SELECT * FROM customers;"], label="""How many rows are in the <span class="monofont">customers</span> table? (type only numbers) """, expected="28813", length=10)

step.render(DA.username)
step.execute(DA.username)

# COMMAND ----------

step = DA.publisher.add_validation(instructions="""

<h2><img class="image-icon-inline" src="https://s3.us-west-2.amazonaws.com/files.training.databricks.com/images/step-icon_small.png" alt="heading-icon" />
Ingest Data Using COPY INTO</h2>
    <div class="instructions-div">
    <p>COPY INTO is often used to ingest streaming data. Files that have previously been ingested are ignored, and new files are ingested every time COPY INTO is run. We can set this up by configuring COPY INTO to point to a directory and not specifying and patterns or files.</p>
    <p>In the query below, we are going to a create an empty Delta table, so we will specify the schema. We will then use COPY INTO to fill the table with data. After making changes to the query, run it more than once, and note that the number of rows does not change. As new .json files are added to the directory, they will be ingested into the table. Note: for this example, there are no new data files being placed in the directory, so the number of rows will not change.</p>
    <p>Make changes to the query below so that COPY INTO pulls data from the directory 'wasb://courseware@dbacademy.blob.core.windows.net/data-analysis-with-databricks/v01/sales_stream.json' and configure the file format as 'JSON'.</p>
    <p>Complete the following:</p>
    <ol>
        <li>Make the required changes to the query below</li>
        <li>Run the query in Databricks SQL</li>
        <li>Check your work by entering your answer to the question</li>
        <li>After pressing <span class="monofont">ENTER/RETURN</span>, green indicates a correct answer, and red indicates incorrect</li>
    </ol>
    <p><pre>USE <span style="color:red;">FILL_IN</span>;
CREATE OR REPLACE TABLE sales_stream (5minutes STRING, 
                                         clicked_items ARRAY&lt;ARRAY&lt;STRING&gt;&gt;, 
                                         customer_id STRING, 
                                         customer_name STRING, 
                                         datetime STRING, 
                                         hour BIGINT, 
                                         minute BIGINT, 
                                         number_of_line_items STRING, 
                                         order_datetime STRING, 
                                         order_number BIGINT, 
                                         ordered_products ARRAY&lt;ARRAY&lt;STRING&gt;&gt;, 
                                         sales_person DOUBLE, 
                                         ship_to_address STRING
);
COPY INTO sales_stream 
    FROM '<span style="color:red;">FILL_IN</span>'
    FILEFORMAT = <span style="color:red;">FILL_IN</span>;
SELECT * FROM sales_stream ORDER BY customer_id;</pre></p>
    </div>


""", statements=["""CREATE OR REPLACE TABLE sales_stream (5minutes STRING, 
                          clicked_items ARRAY<ARRAY<STRING>>, 
                          customer_id STRING, 
                          customer_name STRING, 
                          datetime STRING, 
                          hour BIGINT, 
                          minute BIGINT, 
                          number_of_line_items STRING, 
                          order_datetime STRING, 
                          order_number BIGINT, 
                          ordered_products ARRAY<ARRAY<STRING>>, 
                          sales_person DOUBLE, 
                          ship_to_address STRING
);""", """COPY INTO sales_stream 
    FROM 'wasb://courseware@dbacademy.blob.core.windows.net/data-analysis-with-databricks/v01/sales_stream.json'
    FILEFORMAT = JSON;""", "SELECT * FROM sales_stream ORDER BY customer_id;"], label="""What is the value of <span class="monofont">customer_id</span> in the first row? """, expected="10060379", length=10)

step.render(DA.username)
step.execute(DA.username)

# COMMAND ----------

step = DA.publisher.add_validation(instructions="""

<h2><img class="image-icon-inline" src="https://s3.us-west-2.amazonaws.com/files.training.databricks.com/images/step-icon_small.png" alt="heading-icon" />
Grant/Revoke Privileges Using SQL</h2>
    <div class="instructions-div">
    <p>In this portion of the lab, we are going to grant all privileges on our brand new table, <span class="monofont">sales_stream</span>, to all users in the workspace. We are then going to immediately revoke MODIFY from all users. By default, Databricks SQL has two groups: "admins" and "users", but Databricks admins can add more groups, as needed.</p>
    <p>Complete the following:</p>
    <ol>
        <li>Run the query below in Databricks SQL</li>
        <li>Enter your answer to the question</li>
        <li>After pressing <span class="monofont">ENTER/RETURN</span>, green indicates a correct answer, and red indicates incorrect</li>
    </ol>
    <p><pre>USE <span style="color:red">FILL_IN</span>;
GRANT SELECT ON TABLE `sales_stream` TO `users`;
SHOW GRANT ON `sales_stream`;</pre></p>
    </div>


""", statements=["GRANT SELECT ON TABLE `sales_stream` TO `users`;", 
                 "SHOW GRANT ON `sales_stream`;"], 
   label="""How many privileges does the group "users" have on <span class="monofont">sales_stream</span>? """, 
   expected="1", length=10)

step.render(DA.username)
# step.execute(DA.username)

# COMMAND ----------

step = DA.publisher.add_validation(instructions="""

<h2><img class="image-icon-inline" src="https://s3.us-west-2.amazonaws.com/files.training.databricks.com/images/step-icon_small.png" alt="heading-icon" />
Grant/Revoke Privileges Using the Data Explorer</h2>
    <div class="instructions-div">
    <p>We are now going to use the Data Explorer to perform the same tasks as above. Follow the instructions to find the answer to the question below.</p>
    <p>Complete the following:</p>
    <ol>
        <li>Click "Data" in the sidebar menu to go to the Data Explorer</li>
        <li>Click "Default" to open the drop down, and select your schema</li>
        <li>Select the table, "<span class="monofont">sales_stream</span>" from the list</li>
        <li>Select the "permissions" tab</li>
        <li>Click "Grant"</li>
        <li>Click inside the input box and select "All Users"</li>
        <li>Click inside the input box again to dismiss the list</li>
        <li>Select the checkbox next to "<span class="monofont">MODIFY</span>" in the list of permissions</li>
        <li>Click "OK"</li>
    </ol>
    <p>Note which privileges have now been granted on the table <span class="monofont">sales_stream</span>. To revoke these privileges, complete the following:</p>
    <ol start="10">
        <li>Select the checkboxes next to all privileges granted to "users"</li>
        <li>Click "Revoke"</li>
        <li>Read the warning, and click "Revoke"</li>
        <li>When you are ready, enter your answer to the question below</li>
        <li>After pressing <span class="monofont">ENTER/RETURN</span>, green indicates a correct answer, and red indicates incorrect</li>
    </ol>
    
    </div>


""", statements=None, 
     label="""How many privileges have you been granted to you on your schema? """, 
     expected="6", length=10)

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
