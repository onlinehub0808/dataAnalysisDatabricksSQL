# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md 
# MAGIC # Demo: Navigating Databricks SQL

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup

# COMMAND ----------

step = DA.publisher.add_step(False, instructions="""    

<h2><img class="image-icon-inline" src="https://s3.us-west-2.amazonaws.com/files.training.databricks.com/images/step-icon_small.png" alt="heading-icon" />
Lesson Objective</h2>
    <div class="instructions-div">
    <p>At the end of this lesson, you will be able to:</p>
    <ul>
    <li>Distinguish between Databricks SQL pages and their purposes</li>
    <li>Run a simple query in Databricks SQL</li>    
    </ul></div>
    
    """, statements=None) 

step.render(DA.username)
step.execute(DA.username) 

# COMMAND ----------

step = DA.publisher.add_step(False, instructions="""    <h2><img class="image-icon-inline" src="https://s3.us-west-2.amazonaws.com/files.training.databricks.com/images/step-icon_small.png" alt="heading-icon" />
The Landing Page</h2>
    <div class="instructions-div">
    <p>The landing page is the main page for Databricks SQL. Note the following features of the landing page:</p>
    <ul>
    <li>Shortcuts</li>
    <li>Recents</li>
    <li>Documentation Links</li>
    <li>Links to Release Notes</li>
    <li>Links to Blog Posts</li>
    </ul></div>""", statements=None) 

step.render(DA.username)
step.execute(DA.username) 

# COMMAND ----------

step = DA.publisher.add_step(False, instructions="""<h2><img class="image-icon-inline" src="https://s3.us-west-2.amazonaws.com/files.training.databricks.com/images/step-icon_small.png" alt="heading-icon" />
The Sidebar Menu and App Switcher</h2>
    <div class="instructions-div">
    <p>On the left side of the landing page is the sidebar menu and app switcher. </p>
        <ol>
            <li>Roll your mouse over the sidebar menu</li>
        </ol>
    <p>The menu expands. You can change this behavior using the "Menu Options" at the bottom of the menu.</p>
    <p>The app switcher allows you to change between Databricks SQL and the other apps available to you in Databricks. Your ability to access these apps is configured by your Databricks Administrator. Note that you can pin Databricks SQL as the default app when you login to Databricks.</p>
    </div>""", statements=None) 

step.render(DA.username)
step.execute(DA.username) 

# COMMAND ----------

step = DA.publisher.add_step(False, instructions="""

    <h2>
        <img class="image-icon-inline" src="https://s3.us-west-2.amazonaws.com/files.training.databricks.com/images/step-icon_small.png" alt="heading-icon" />
        SQL Endpoints</h2>
    <div class="instructions-div">
        <p>In order to work with data in Databricks SQL, you will need to have access to a SQL Endpoint</p>
        <ol start="2">
            <li>Click "SQL Endpoints" in the sidebar menu</li>
        </ol>
        <p>There should be at least one endpoint in the list. (If not, you will need to have a Databricks Administrator configure an endpoint for you). Note the following features of the "SQL Endpoints" page:</p>
        <ul>
            <li>Endpoint Name</li>
            <li>State</li>
            <li>Size</li>
            <li>Active/Max</li>
            <li>Start/Stop Button</li>
            <li>Actions Menu</li>
        </ul>
        <ol start="3">
            <li>Click the name of the endpoint</li>
        </ol>
        <p>Note the following features on the Endpoint's detail page:</p>
        <ul>
            <li>Overview</li>
            <li>Connection Details</li>
        </ul>
    </div>
    
    """, statements=None) 

step.render(DA.username)
step.execute(DA.username) 

# COMMAND ----------

step = DA.publisher.add_step(False, instructions="""

    <h2>
        <img class="image-icon-inline" src="https://s3.us-west-2.amazonaws.com/files.training.databricks.com/images/step-icon_small.png" alt="heading-icon" />
        The Query Editor</h2>
    <div class="instructions-div">
        <ol start="4">
            <li>Click Create --> Query in the sidebar menu</li>
        </ol>
        <p>This is the Query Editor in Databricks SQL. Note the following features of the Query Editor:</p>
        <ul>
            <li>Schema Browser</li>
            <li>Tabbed Interface</li>
            <li>Results View</li>
        </ul>
        <p>To run a query, double-check that you have an endpoint selected, type the query into the editor and click Run.</p>
        <p>We are going to run a simple query that will display our username</p>
        <ol start="5">
            <li>Paste the code below into the Query Editor, and click Run:</li>
        </ol>
        <p>We are going to use this username in future queries in a <span class="monofont">USE</span> statement.</p>
    </div>
    """, statements="""
SELECT current_user() AS Username;
""") 

step.render(DA.username)
step.execute(DA.username) 

# COMMAND ----------

step = DA.publisher.add_step(False, instructions="""

    <h2>
        <img class="image-icon-inline" src="https://s3.us-west-2.amazonaws.com/files.training.databricks.com/images/step-icon_small.png" alt="heading-icon" />
        Query Results</h2>
    <div class="instructions-div">
        <p>The query we executed above is pulling from a schema created just for you as part of this course. In the results window, we can see the data pulled from our schema.</p>
        <p>Note the following features of the results window:</p>
        <ul>
            <li>Number of Results Received</li>
            <li>Refreshed Time</li>
            <li>"Add Visualization" button</li>
        </ul>
        <p>When visualizations are added to your queries, they will also show up in the results window.</p>
    </div>
    
    """, statements=None) 

step.render(DA.username)
step.execute(DA.username) 

# COMMAND ----------

step = DA.publisher.add_step(False, instructions="""

    <h2>
        <img class="image-icon-inline" src="https://s3.us-west-2.amazonaws.com/files.training.databricks.com/images/step-icon_small.png" alt="heading-icon" />
        Query History</h2>
    <div class="instructions-div">
        <p>To see a list of queries that have been run, we can access Query History.</p>
    <ol start="6">
        <li>In the sidebar menu, click "Query History"</li>
    </ol>
        <p>Query History shows a list of all queries that have been run.</p>
    <ul>
        <li>Toolbar for changing user, time span, endpoint, and status filters</li>
        <li>"Refresh" button and scheduler</li>
        <li>Query list with informative columns</li>
    </ul>   
    <p>We can get more detailed information by clicking a query.</p>
    <ol start="7">
        <li>Click a query in the list</li>
    </ol>
    <p>A drawer opens that provides more detailed information about the selected query. If we want to view the query profile, we can do that, too.</p>
    <ol start="8">
        <li>Click "View query profile"</li>
    </ol>
    <p>This information can be used to help troubleshoot queries or to improve query performance.</p>
    </div>
    
    """, statements=None) 

step.render(DA.username)
step.execute(DA.username) 

# COMMAND ----------

DA.validate_datasets()
html = DA.publisher.publish(include_inputs=False)
displayHTML(html)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
