# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md 
# MAGIC # Lab: Notifying Stakeholders

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup

# COMMAND ----------

step = DA.publisher.add_step(False, instructions="""    

    <h2><img class="image-icon-inline" src="https://s3.us-west-2.amazonaws.com/files.training.databricks.com/images/step-icon_small.png" alt="heading-icon" />
    Lesson Objective</h2>
    <div class="instructions-div">
    <p>At the end of this lesson, you will be able to:</p>
    <ul>
    <li>Configure alerts</li>
    <li>Share queries and dashboards with stakeholders</li>
    <li>Refresh dashboards</li>
    </ul></div>
    
    """, statements=None) 

step.render(DA.username)
step.execute(DA.username)

# COMMAND ----------

step = DA.publisher.add_validation(instructions="""

<h2><img class="image-icon-inline" src="https://s3.us-west-2.amazonaws.com/files.training.databricks.com/images/step-icon_small.png" alt="heading-icon" />
Refreshing Queries</h2>
    <div class="instructions-div">
    <p>Putting a query on a refresh schedule only takes a few steps. In this portion of the lab, we are going to use a query we worked with earlier in the course that was designed to ingest new JSON files, containing new data. If we had to manually run the query each time we wanted to ingest new data, we might find our data growing stale in-between query runs. By configuring a Refresh Schedule, we can automatically pull new data into the <span class="monofont">sales_stream</span> table.</p>
    <p>Complete the following:</p>
    <ol>
        <li>Go to the Query Editor by clicking "SQL Editor" in the sidebar menu</li>
        <li>Start a new query by hovering your mouse over the "+" in the tabbed area of the Query Editor and clicking "Create new query"</li>
        <li>Run the query below</li>
        <li>Name the query by clicking "New Query" and typing “Ingest Sales Stream”</li>
        <li>Click "Save"</li>
        <li>Click "Never" next to "Refresh Schedule"</li>
        <li>Change the dropdown to something other than "Never"</li>
        <li>Optionally, change "End" to tomorrow's date</li>
        <li>Click "OK"</li>
        <li>Check your work by entering your answer to the question</li>
        <li>After pressing <span class="monofont">ENTER/RETURN</span>, green indicates a correct answer, and red indicates incorrect</li>
    </ol>
    <p><pre>USE <span style="color:red">FILL_IN</span>;
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
    FROM 'wasb://courseware@dbacademy.blob.core.windows.net/data-analysis-with-databricks/v01/sales_stream.json'
    FILEFORMAT = JSON;
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
    FILEFORMAT = JSON;""", "SELECT * FROM sales_stream ORDER BY customer_id;"], label="""What is the shortest amount of time we can configure for query refreshes? """, expected="1 minute", length=10)

step.render(DA.username)
step.execute(DA.username)

# COMMAND ----------

step = DA.publisher.add_validation(instructions="""

<h2><img class="image-icon-inline" src="https://s3.us-west-2.amazonaws.com/files.training.databricks.com/images/step-icon_small.png" alt="heading-icon" />
Alerts</h2>
    <div class="instructions-div">    
    <p>Now that we have configured a refresh schedule, let's set up an Alert that will notify us when the income generated from sales increases beyond a threshold.</p>
    <p>The goal of this portion of the lab is to generate a query that will calculate a sum of the income from all the sales in the <span class="monofont">sales_stream</span> table and configure an Alert that will notify us when the income amount surpasses $1,000,000.</p>
    <p>The <span class="monofont">sales_stream</span> table has a column called <span class="monofont">ordered_products</span>, which contains an array of additional arrays that each contain, among other things, the sales price of the product ordered. This value is in the third position in each array. The first step in the query is return each array of product information as its own row in the results. The <span class="monofont">explode()</span> function does just this. Since we want the third element in each of these arrays, we use a CTE to allow us to use the bracket operator (<span class="monofont">[]</span>) to index the third element. We then use the <span class="monofont">sum()</span> function to calculate a sum of the cost of the items sold. We then configure an Alert to notify us when the sum increases beyond 1000000.</p>
    <p>Complete the following:</p>
    <ol>
        <li>Make the required changes to the query below</li>
        <li>Run the query in Databricks SQL</li>
        <li>Save the query as "Total Sales Dollars"</li>
        <li>Click "Alerts" in the sidebar menu</li>
        <li>Click "Create Alert"</li>
        <li>From the Query dropdown, select our query: "Total Sales Dollars"</li>
        <li>Use the dropdown to change the "Value" column to <span class="monofont">total_sales</span> and change "Threshold" to 1000000</li>
        <li>Change "Refresh" to Every 1 minute</li>
        <li>Check your work by entering your answer to the question</li>
        <li>After pressing <span class="monofont">ENTER/RETURN</span>, green indicates a correct answer, and red indicates incorrect</li>
    </ol>
    <p><pre>USE <span style="color:red">FILL_IN</span>;
WITH all_sold_products AS
    (SELECT explode(ordered_products) AS products FROM sales_stream)
SELECT sum(products[3]) AS total_sales FROM <span style="color:red">FILL_IN</span>;</pre></p>
    </div>


""", statements="""WITH all_sold_products AS
    (SELECT explode(ordered_products) AS products FROM sales_stream)
SELECT sum(products[3]) AS total_sales FROM all_sold_products;""", label="""What is the total amount of income (in whole dollars) currently in the <span class="monofont">sales_stream</span> table? (type only numbers) """, expected="265759", length=10)

step.render(DA.username)
step.execute(DA.username)

# COMMAND ----------

step = DA.publisher.add_validation(instructions="""

<h2><img class="image-icon-inline" src="https://s3.us-west-2.amazonaws.com/files.training.databricks.com/images/step-icon_small.png" alt="heading-icon" />
Sharing Queries/Dashboards</h2>
    <div class="instructions-div">
    <p>Since the process for sharing queries and dashboards is identical, we are only going to work with dashboards. Let's share a dashboard with all other users.</p>
    <p>Complete the following:</p>
    <ol>
        <li>Click "Dashboards" in the sidebar menu</li>
        <li>Click on the name of your dashboard</li>
        <li>In the upper-right corner, click "Share"</li>
        <li>Click inside the input box, and select the group, "All Users"</li>
        <li>Click inside the input box again to dismiss the drop down</li>
    </ol>
    <p>Note that the only permission we can grant is "Can Run". If we want to grant "Can Edit", we first need to have the proper permissions. We can then change "Credentials" (under "Sharing Settings" in the "Manage permissions" dialog) to "Run as viewer". We do not want to make this change, here, though. Leave the Credentials set to "Run as owner" </p>
    <ol start="6">
        <li>Click "Add", and click the "X" to dismiss the "Manage permissions" dialog</li>
        <li>Check your work by entering your answer to the question</li>
        <li>After pressing <span class="monofont">ENTER/RETURN</span>, green indicates a correct answer, and red indicates incorrect</li>
    </ol>
    <p>To remove sharing permissions, click "Share" and "Revoke" for the user whose permission should be revoked.</p>
    </div>


""", statements=None, label="""After sharing the dashboard with the "All Users" group, how many rows of users and groups are in the "Manage permissions" dialog? """, expected="2", length=10)

step.render(DA.username)
step.execute(DA.username)

# COMMAND ----------

step = DA.publisher.add_validation(instructions="""

<h2><img class="image-icon-inline" src="https://s3.us-west-2.amazonaws.com/files.training.databricks.com/images/step-icon_small.png" alt="heading-icon" />
Refreshing Dashboards</h2>
    <div class="instructions-div">
    <p>To keep our dashboard up-to-date, we can set a Refresh Schedule that will run the dashboard at a set interval that ranges from 1 minute to 1 year. In addition, we can configure Subscribers, who will receive a report of our dashboard after the update runs.</p>
    <p>Complete the following:</p>
    <ol>
        <li>To drop down the schedule configuration window, click the button "Schedule" in the upper-right corner</li>
        <li>Change the "Refresh" to  "Every 1 minute"</li>
    </ol>
    <p>You can choose to run the queries that provide data to the dashboard on a specific endpoint, or leave the default</p>
    <ol start="3">
        <li>Leave "SQL Endpoint" on (optional)</li>
    </ol>
    <p>You can configure the Refresh Schedule to send the results of refreshing the dashboard to one, or more, subscribers. Those who are added as subscribers to the refresh schedule will be granted "Can Run" permissions on the dashboard.</p>
    <ol start="4">
        <li>If desired, click inside the "Subscribers" input box and select from the list of users</li>
    </ol>
    <p>Note that the Refresh Schedule can be enabled and disabled. If you wish to keep the Refresh Schedule's settings, but you do not wish to run the refresh schedule, click the switch to disable refreshing. Finally, to delete the refresh schedule, change "Refresh" to "Never".</p>
     <ol start="5">
        <li>Change the "Refresh" to "Never" so it won't be running indefinitely </li>
    </ol>
    <ol start="6">
        <li>Check your work by entering your answer to the question</li>
    </ol>
        <li>After pressing <span class="monofont">ENTER/RETURN</span>, green indicates a correct answer, and red indicates incorrect</li>
    </ol>
    </div>


""", statements=None, label="""When you click "Advanced" under the "Refresh" drop down, how many minutes are selected by default? (enter only numbers) """, expected="10", length=10)

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
