# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md 
# MAGIC # Demo: Delta Commands in Databricks SQL

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup

# COMMAND ----------

step = DA.publisher.add_step(False, instructions="""    

    <h2><img class="image-icon-inline" src="https://s3.us-west-2.amazonaws.com/files.training.databricks.com/images/step-icon_small.png" alt="heading-icon" />
    Lesson Objective</h2>
    <div class="instructions-div">
    <p>At the end of this lesson, you will be able to:</p>
    <ul>
    <li>Describe how to write commands for working with Delta tables in Databricks SQL</li>
    </ul></div>
    
    """, statements=None) 

step.render(DA.username)
step.execute(DA.username) 

# COMMAND ----------

step = DA.publisher.add_step(True, instructions="""    

   <h3>SELECT on Delta Tables</h3>
    <div class="instructions-div">
    <p>So far, the SQL commands we have used are generic to most flavors of SQL. In the next few queries, we are going to look at commands that are specific to using <span class="monofont">SELECT</span> on Delta tables.</p>
    <p>Delta tables keep a log of changes that we can view by running the command below.</p>
    <ol start="1">
        <li>Run the code below.</li>
    </ol>
    <p>After running <span class="monofont">DESCRIBE HISTORY</span>, we can see that we are on version number 0 and we can see a timestamp of when this change was made.</p>
    </div>
    
    """, statements="DESCRIBE HISTORY customers;") 

step.render(DA.username)
step.execute(DA.username)

# COMMAND ----------

step = DA.publisher.add_step(True, instructions="""    

   <h3>SELECT on Delta Tables -- Updating the Table</h3>
    <div class="instructions-div">
    <p>We are going to make a change to the table.</p>
    <ol start="2">
        <li>Run the code below.</li>
    </ol>
    <p>The code uses an <span class="monofont">UPDATE</span> statement to make a change to the table. We will be discussing <span class="monofont">UPDATE</span> later on. For now, we just need to understand that a change was made to the table. We also reran our <span class="monofont">DESCRIBE HISTORY</span> command, and note that we have a new version in the log, with a new timestamp.</p>
    </div>
    
    """, statements=["UPDATE customers SET loyalty_segment = 10 WHERE loyalty_segment = 0;","DESCRIBE HISTORY customers;"]) 

step.render(DA.username)
step.execute(DA.username)

# COMMAND ----------

step = DA.publisher.add_step(True, instructions="""    

   <h3>SELECT on Delta Tables -- Updating the Table back to where it was</h3>
    <div class="instructions-div">
    <p>We are going to make a change to the table.</p>
    <ol start="3">
        <li>Run the code below.</li>
    </ol>
    <p>The code uses an <span class="monofont">UPDATE</span> statement to update the loyalty_segment back to its original value. We also reran our <span class="monofont">DESCRIBE HISTORY</span> command, and note that we have a new version in the log, with a new timestamp.</p>
    </div>
    
    """, statements=["UPDATE customers SET loyalty_segment = 0 WHERE loyalty_segment = 10;","DESCRIBE HISTORY customers;"]) 

step.render(DA.username)
step.execute(DA.username)

# COMMAND ----------

step = DA.publisher.add_step(True, instructions="""    

   <h3>SELECT on Delta Tables -- VERSION AS OF</h3>
    <div class="instructions-div">
    <p>We can now use a special predicate for use with Delta tables: <span class="monofont">VERSION AS OF</span></p>
    <ol start="4">
        <li>Run the code below.</li>
    </ol>
    <p>By using <span class="monofont">VERSION AS OF</span>, we can <span class="monofont">SELECT</span> from specific versions of the table. This feature of Delta tables is called "Time Travel," and is very powerful.</p>
    <p>We can also use <span class="monofont">TIMESTAMP AS OF</span> to <span class="monofont">SELECT</span>based on a table's state on a specific date, and you can find more information in the documentation.</p>
    </div>
    
    """, statements="SELECT loyalty_segment FROM customers VERSION AS OF 1;") 

step.render(DA.username)
step.execute(DA.username)

# COMMAND ----------

step = DA.publisher.add_step(True, instructions="""    

    <h3>MERGE INTO</h3>
    <div class="instructions-div">
    <p>Certainly, there are times when we want to insert new data but ensure we don't re-insert matched data. This is where we use <span class="monofont">MERGE INTO</span>. <span class="monofont">MERGE INTO</span> will merge two tables together, but you specify in which column to look for matched data and what to do when a match is found. Let's run the code and examine the command in more detail.</p>
    <ol start="5">
        <li>Run the code below.</li>
    </ol>
    <p>We are merging the <span class="monofont">source_suppliers</span> table into the <span class="monofont">suppliers</span> table. After <span class="monofont">ON</span> keyword, we provide the columns on which we want to look for matches. We then state what the command should do when a match is found. In this example, we are inserting the row when the two columns are not matched. Thus, if the columns match, the row is ignored. Notice that the count is the exact same as the original table. This is because the two tables have the exact same data, since we overwrote the <span class="monofont">suppliers</span> table with the <span class="monofont">source_suppliers</span> table earlier in the lesson.</p>
    </div>
    
    """, statements=["""MERGE INTO suppliers
    USING source_suppliers
    ON suppliers.SUPPLIER_ID = source_suppliers.SUPPLIER_ID
    WHEN NOT MATCHED THEN INSERT *;""", "SELECT count(*) FROM suppliers;"]) 

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
