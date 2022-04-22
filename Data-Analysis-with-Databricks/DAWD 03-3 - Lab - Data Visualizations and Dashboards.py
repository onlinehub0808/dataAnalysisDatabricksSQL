# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md 
# MAGIC # Lab: Data Visualizations and Dashboards

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup

# COMMAND ----------

step = DA.publisher.add_step(False, instructions="""    

    <h2><img class="image-icon-inline" src="https://s3.us-west-2.amazonaws.com/files.training.databricks.com/images/step-icon_small.png" alt="heading-icon" />
    Lesson Objective</h2>
    <div class="instructions-div">
    <p>At the end of this lesson, you will be able to:</p>
    <ul>
    <li>Create basic visualizations using Databricks SQL</li>
    <li>Create a dashboard using multiple existing visualizations</li>
    </ul></div>
    
    """, statements=None) 

step.render(DA.username)
step.execute(DA.username)

# COMMAND ----------

step = DA.publisher.add_validation(instructions="""

<h2><img class="image-icon-inline" src="https://s3.us-west-2.amazonaws.com/files.training.databricks.com/images/step-icon_small.png" alt="heading-icon" />
Create a Boxplot</h2>
    <div class="instructions-div">    
    <p>We can create a boxplot to highlight outliers in our data and to provide other information like quartiles, minimun, maximum, etc.</p>
    <p>Complete the following:</p>
    <ol>
        <li>Open a new query tab, and run the query below</li>
        <li>Save the query as "Boxplot"</li>
        <li>In the query results section, click "Add Visualization"</li>
        <li>Select "Box" as the visualization type</li>
        <li>For "X Column" select "product_category"</li>
        <li>For "Y Column" select "total_price"</li>
        <li>Click the "X Axis" tab</li>
        <li>Enter "Product Category" in the "Name" field to make more user friendly</li>
        <li>Click the "Y Axis" tab</li>
        <li>Enter "Price" in the "Name" field to make it more user friendly</li>
           </ol>
    <p>Note that there are many outliers. Although we would not want to ignore these, we can hide them by completing the following step:</p>
    <ol start="11">
        <li>Enter "30000" for the "Max Value" field</li>
        <li>Click "Save" in the lower-right corner</li>
        <li>Click inside the tab named "Box 1" and change to "Price by Product"</li>
        <li>Make sure the query is Saved</li>
        <li>Check your work by entering your answer to the question below</li>
        <li>After pressing <span class="monofont">ENTER/RETURN</span>, green indicates a correct answer, and red indicates incorrect</li>
    </ol>
    <p>Note that you can hover your mouse over the boxplot to get additional information about each plot.</p>
    <p><pre>USE <span style="color:red;">FILL_IN</span>;
SELECT * FROM sales;</pre></p>
    </div>


""", statements=["SELECT * FROM sales;"], label="""Which product category has the highest upper fence? """, expected="opple", length=10)

step.render(DA.username)
step.execute(DA.username) 

# COMMAND ----------

step = DA.publisher.add_validation(instructions="""

<h2><img class="image-icon-inline" src="https://s3.us-west-2.amazonaws.com/files.training.databricks.com/images/step-icon_small.png" alt="heading-icon" />
Create a Funnel</h2>
    <div class="instructions-div">    
    <p>The Funnel visualization shows the number of people as they progress from one step to another. In this portion of the lab, we are going to make a funnel that shows the number of customers who have progressed from sales orders to actual sales.</p>
    <p>Complete the following:</p>
    <ol>
        <li>Open a new query tab, and run the query below</li>
        <li>Save the query as "Funnel"</li>
        <li>In the query results section, click "Add Visualization"</li>
        <li>Select "Funnel" as the visualization type</li>
        <li>For "Step Column" select "Step"</li>
        <li>For "Value Column" select "Value"</li>
    </ol>
    <p>That's it! Most of the setup in this visualization is contained in the query. The query uses a CTE and two <span class="monofont">UNION ALL</span>s to genenerate the data. Really, all that's happening is we are selecting distinct customer IDs in each of the tables to determine which customers have been active in generating sales orders and sales.</p>
    <ol start="7">
        <li>Click "Save" in the lower-right corner</li>
        <li>Click inside the tab "Funnel 1" and change the name to "Customer Funnel"</li>
        <li>Make sure the query is Saved</li>
        <li>Check your work by entering your answer to the question below</li>
        <li>After pressing <span class="monofont">ENTER/RETURN</span>, green indicates a correct answer, and red indicates incorrect</li>
    </ol>
    <p></p>
    <p><pre>USE <span style="color:red;">FILL_IN</span>;
WITH funnel AS (
    SELECT DISTINCT "Customers" AS Step, string(customer_id) FROM customers
        UNION ALL
    SELECT DISTINCT "Orders" AS Step, customer_id FROM sales_orders
        UNION ALL
    SELECT DISTINCT "Sales" AS Step, customer_id FROM sales
)
SELECT Step, count(customer_id) as Value 
    FROM funnel
    GROUP BY Step;</pre></p>
    </div>


""", statements=["""WITH funnel AS (
    SELECT DISTINCT "Customers" AS Step, string(customer_id) FROM customers
        UNION ALL
    SELECT DISTINCT "Orders" AS Step, customer_id FROM sales_orders
        UNION ALL
    SELECT DISTINCT "Sales" AS Step, customer_id FROM sales
)
SELECT Step, count(customer_id) as Value 
    FROM funnel
    GROUP BY Step;"""], label="""What is the %Max for Orders? """, expected="6.77%", length=10)

step.render(DA.username)
step.execute(DA.username) 

# COMMAND ----------

step = DA.publisher.add_validation(instructions="""

<h2><img class="image-icon-inline" src="https://s3.us-west-2.amazonaws.com/files.training.databricks.com/images/step-icon_small.png" alt="heading-icon" />
Create a Dashboard</h2>
    <div class="instructions-div">    
    <p>Let's combine the visualizations we just made into a dashboard.</p>
    <p>Complete the following:</p>
    <ol>      
        <li>Click "Dashboards" in the sidebar menu</li>
        <li>Click the blue "Create Dashboard" button</li>
        <li>Name the dashboard "Company Information" and click the "Save" button</li>
        <li>Click the blue "Add Visualization" button</li>
        <li> In window, click "Boxplot" </li>
        <li>Drop down "Choose Visualization" and select "Price by Product" (which you created earlier)</li>
        <li>Change "Title" to "Prices"</li>
        <li>Click the blue "Add to dashboard" button and the Boxplot appears in your dashboard</li>
        <li>Repeat steps 4-8 with the "Funnel" query ("Customer Funnel")</li>
        <li>Click "Add Textbox" and type "# Miscellaneous Information"</li>
        <li>Optional: Move the visualizations around by clicking and dragging each one</li>
        <li>Optional: Resize each visualization by dragging the lower-right corner of the visualization</li>
        <li>Optional: Click "Colors" to change the color palette used by visualizations in the dashboard</li>
        <li>Click "Done Editing" in the upper-right corner</li>
        <li>Run every query and refresh all visualizations all at once by clicking "Refresh"</li>
        <li>Check your work by entering your answer to the question below</li>
        <li>Hint: Hover over each Product Category to get the statistics in the Boxplot Chart</li>
        <li>After pressing <span class="monofont">ENTER/RETURN</span>, green indicates a correct answer, and red indicates incorrect</li>
    </ol> 
    </div>


""", statements=None, label="""Which product category has the lowest minimum price? """, expected="rony", length=10)

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
