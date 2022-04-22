# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md 
# MAGIC # Demo: Ingesting Data

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup

# COMMAND ----------

step = DA.publisher.add_step(False, instructions="""    

    <h2><img class="image-icon-inline" src="https://s3.us-west-2.amazonaws.com/files.training.databricks.com/images/step-icon_small.png" alt="heading-icon" />
    Lesson Objective</h2>
    <div class="instructions-div">
    <p>At the end of this lesson, you will be able to:</p>
    <ul>
    <li>Describe how to ingest data</li>
    </ul></div>
    
    """, statements=None) 

step.render(DA.username)
step.execute(DA.username)

# COMMAND ----------

step = DA.publisher.add_step(True, instructions="""    

<h2><img class="image-icon-inline" src="https://s3.us-west-2.amazonaws.com/files.training.databricks.com/images/step-icon_small.png" alt="heading-icon" />
Ingest Data Using Databricks SQL</h2>
    <div class="instructions-div">
    <p>We can ingest data into Databricks SQL in a variety of ways. In this first example, we are going to start with a .csv file that exists in an object store and finish with a Delta table that contains all the data from that .csv file.</p>
    <ol>
        <li>Run the code below.</li>
    </ol>
    <p>There are several things to note in these commands:</p>
    <ul>
        <li>Since the file we are ingesting is a .csv file, we state <span class="monofont">USING csv</span></li>
        <li>We are setting three options:
            <ul>
                <li>path - the path to the object store</li>
                <li>header - whether or not the .csv file contains a header row</li>
                <li>inferSchema - whether or not Databricks should infer the schema from the contents of the file</li>
             </ul></li>
        <li>We are creating a second table, with default settings from the contents of the first table, using a CTAS statement.</li>
    </ul>
    <p>The reason we are creating a second table that is a copy of the first table is because we want the resulting table to be in Delta format, which gives us the most options. Because the second table uses default options, it will be a Delta table.</p>
    <p>We can see this in the Data Explorer</p>
    <ol start="2">
        <li>In the sidebar menu, click "Data"</li>
        <li>If needed, select your schema in the dropdown</li>
        <li>Select <span class="monofont">web_events_csv</span> and note it is a CSV table</li>
        <li>Select <span class="monofont">web_events</span> and note it is a Delta table</li>
    </ol>
    <p>Once we are finished with the <span class="monofont">web_events_csv</span> table, we can drop it from the schema.</p>
    </div>
    
    """, statements=["DROP TABLE IF EXISTS web_events_csv;", """CREATE TABLE web_events_csv 
    USING csv 
    OPTIONS (
        path='wasb://courseware@dbacademy.blob.core.windows.net/data-analysis-with-databricks/v01/web_events/web-events.csv',
        header="true",
        inferSchema="true"
    );""", "DROP TABLE IF EXISTS web_events;","""CREATE OR REPLACE TABLE web_events AS
    SELECT * FROM web_events_csv;"""]) 

step.render(DA.username)
step.execute(DA.username)

# COMMAND ----------

step = DA.publisher.add_step(False, instructions="""    

<h2><img class="image-icon-inline" src="https://s3.us-west-2.amazonaws.com/files.training.databricks.com/images/step-icon_small.png" alt="heading-icon" />
The LOCATION Keyword</h2>
    <div class="instructions-div">
    <p>We talked about the <span class="monofont">LOCATION</span> keyword in the last module. We can ingest data in-place using the <span class="monofont">LOCATION</span> keyword to create an external/unmanaged table.</p>

    </div>
    
    """, statements=None) 

step.render(DA.username)
step.execute(DA.username)

# COMMAND ----------

step = DA.publisher.add_step(True, instructions="""    

<h2><img class="image-icon-inline" src="https://s3.us-west-2.amazonaws.com/files.training.databricks.com/images/step-icon_small.png" alt="heading-icon" />
COPY INTO</h2>
    <div class="instructions-div">
    <p>We use <span class="monofont">COPY INTO</span> to load data from a file location into a Delta table. <span class="monofont">COPY INTO</span> is a re-triable and idempotent operation, so files in the source location that have already been loaded are skipped.</p>
    <ol start="6">
        <li>Run the code below.</li>
    </ol>
    <p>The first command creates an empty Delta table. Note that you must specify a schema when creating an empty Delta table.</p>
    <p>The second command copies data from an object store location into the <span class="monofont">web-events</span> table. Note that the file type for the files in the object store location is specified as "JSON". The last part of the <span class="monofont">COPY INTO</span> command is a file name, a list of file names, or a directory of files.</p>
    </div>
    
    """, statements=["CREATE OR REPLACE TABLE gym_logs (first_timestamp DOUBLE, gym Long, last_timestamp DOUBLE, mac STRING);", """COPY INTO gym_logs 
    FROM 'wasbs://courseware@dbacademy.blob.core.windows.net/data-analysis-with-databricks/v01/gym-logs'
    FILEFORMAT = JSON
    FILES = ('20191201_2.json');"""]) 

step.render(DA.username)
step.execute(DA.username)

# COMMAND ----------

step = DA.publisher.add_step(True, instructions="""    

    <div class="instructions-div">
    <p>We can run the same <span class="monofont">COPY INTO</span> command as above and add a second one. The first one will be skipped because the file has already been loaded.</p>
    <ol start="7">
        <li>Run the code below.</li>
    </ol>
    <p></p>
    </div>
    
    """, statements=["""COPY INTO gym_logs 
    FROM 'wasbs://courseware@dbacademy.blob.core.windows.net/data-analysis-with-databricks/v01/gym-logs'
    FILEFORMAT = JSON
    FILES = ('20191201_2.json');""", """COPY INTO gym_logs
    FROM 'wasbs://courseware@dbacademy.blob.core.windows.net/data-analysis-with-databricks/v01/gym-logs'
    FILEFORMAT = JSON
    FILES = ('20191201_3.json');
    """]) 

step.render(DA.username)
step.execute(DA.username)

# COMMAND ----------

step = DA.publisher.add_step(True, instructions="""    

    <div class="instructions-div">
    <p>Do a count on the table you just populated using COPY INTO. Then, rerun the COPY INTO code above, and do another count. Notice that 'COPY INTO' will not input data from files that have already been ingested using COPY INTO. The count stays the same. </p>
    <ol start="8">
        <li>Run the code below.</li>
    </ol>
    <p></p>
    </div>
    
    """, statements=["""SELECT count(*) FROM gym_logs;
    """]) 

step.render(DA.username)
step.execute(DA.username)

# COMMAND ----------

step = DA.publisher.add_step(True, instructions="""    

    <div class="instructions-div">
    <p>Next, let's add another <span class="monofont">COPY INTO</span> command, but this time, we will use the <span class="monofont">PATTERN</span> option. This allows us to load any file that fits a specific pattern.</p>
    <ol start="8">
        <li>Run the code below.</li>
    </ol>
    <p></p>
    </div>
    
    """, statements=["""COPY INTO gym_logs 
    FROM 'wasbs://courseware@dbacademy.blob.core.windows.net/data-analysis-with-databricks/v01/gym-logs'
    FILEFORMAT = JSON
    FILES = ('20191201_2.json');""", """COPY INTO gym_logs
    FROM 'wasbs://courseware@dbacademy.blob.core.windows.net/data-analysis-with-databricks/v01/gym-logs'
    FILEFORMAT = JSON
    FILES = ('20191201_3.json');""", """COPY INTO gym_logs 
    FROM 'wasbs://courseware@dbacademy.blob.core.windows.net/data-analysis-with-databricks/v01/gym-logs'
    FILEFORMAT = JSON
    PATTERN = '20191201_[0-9].json';"""]) 

step.render(DA.username)
step.execute(DA.username)

# COMMAND ----------

step = DA.publisher.add_step(False, instructions="""    

<h2><img class="image-icon-inline" src="https://s3.us-west-2.amazonaws.com/files.training.databricks.com/images/step-icon_small.png" alt="heading-icon" />
Privileges</h2>
    <div class="instructions-div">
    <p>Data object owners and Databricks administrators can grant and revoke a variety of privileges on securable objects. These objects include functions, files, tables, views, and more. These privileges can be granted using SQL or using the Data Explorer.</p>
    <ol start="9">
        <li>Run the code below.</li>
    </ol>
    <p>This statement returns the privileges granted on this schema.</p>
    </div>
    
    """, statements="SHOW GRANT ON SCHEMA {db_name};") 

step.render(DA.username)
# step.execute(DA.username)

# COMMAND ----------

step = DA.publisher.add_step(False, instructions="""    

<h3>GRANT</h3>
    <div class="instructions-div">
    <p>If we wish to grant privileges to a user, we can run <span, class="monofont">GRANT</span> commands for the specific privileges we wish to grant. The privileges available include <span, class="monofont">USAGE</span>, <span, class="monofont">SELECT</span>, <span, class="monofont">CREATE</span>, <span, class="monofont">READ FILES</span>, and more.</p>
    <ol start="10">
        <li>Run the code below.</li>
    </ol>
    <p>We need to start by granting <span, class="monofont">USAGE</span> to a user. We can then grant other privileges. If we want to grant all privileges, we can use <span, class="monofont">GRANT ALL PRIVILEGES</span>.</p>
    </div>
    
    """, statements=["GRANT USAGE ON SCHEMA {db_name} TO `{username}`;", 
                     "GRANT SELECT ON SCHEMA {db_name} TO `users`;"]) 

step.render(DA.username)
# step.execute(DA.username)

# COMMAND ----------

step = DA.publisher.add_step(False, instructions="""    

<h3>REVOKE</h3>
    <div class="instructions-div">
    <p>We can revoke privileges on securable objects in the exact same way.</p>
    <p>We don't want to actually run the command because we don't want to try to revoke our own privileges, but here is the command:</p>
    <p><span, class="monofont">REVOKE ALL PRIVILEGES ON SCHEMA `schema_name` from `user_name`;</span></p>
    </div>
    
    """, statements=None) 

step.render(DA.username)
step.execute(DA.username)

# COMMAND ----------

step = DA.publisher.add_step(False, instructions="""    

<h3>Data Explorer</h3>
    <div class="instructions-div">
    <p>While we can certainly grant and revoke privileges using SQL, we can also use the Data Explorer.</p>
    <ol start="11">
        <li>Click "Data" in the sidebar menu.</li>
        <li>If needed, select your schema in the dropdown</li>
        <li>Select the table, "gym_logs" from the list</li>
        <li>Click "Permissions"</li>
        <li>Use the "Grant" and "Revoke" buttons to change permission settings.</li>
    </ol>
    <p>Note that students can view and change permissions on this table because, since they created the table, they are the owner. Only owners and admins can view and change permissions.</p>
    <ol start="16">
        <li>Select the "flight_delays" table.</li>
        <li>Click "Permissions"</li>
    </ol>
    <p>Note that, since the student is not the owner of the table, they can't view and change permissions.</p>
    </div>
    
    """, statements=None) 

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
