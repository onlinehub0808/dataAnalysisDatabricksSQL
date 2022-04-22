# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md 
# MAGIC # Demo: Data Visualizations and Dashboards

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup

# COMMAND ----------

step = DA.publisher.add_step(False, instructions="""    

    <h2><img class="image-icon-inline" src="https://s3.us-west-2.amazonaws.com/files.training.databricks.com/images/step-icon_small.png" alt="heading-icon" />
    Lesson Objective</h2>
    <div class="instructions-div">
   
    <p>At the end of this lesson, you will be able to:</p>
    <ul>
        <li>Describe how to create basic visualizations using Databricks SQL</li>
        <li>Describe how to create a dashboard using multiple existing visualizations from Databricks SQL Queries</li>
        <li>Describe how to parameterize queries and dashboards to customize results and visualizations</li>
    </ul></div>
    
    """, statements=None) 

step.render(DA.username)
step.execute(DA.username) 

# COMMAND ----------

step = DA.publisher.add_step(True, instructions="""    

<h2><img class="image-icon-inline" src="https://s3.us-west-2.amazonaws.com/files.training.databricks.com/images/step-icon_small.png" alt="heading-icon" />
The Counter</h2>
    <div class="instructions-div">
    <p>The Counter visualization is one of the simplest visualizations in Databricks SQL. It displays a single number by default, but it can also be configured to display a "goal" number. In this example, we are going to configure a sum of completed sales, along with a "Sales Goal." The query calculates a sum of total sales and also provides a hard-coded sales goal column.</p>
    <p>Complete the following:</p>
    <ol>
        <li>Open a new query tab, and run the query below</li>
        <li>Save the query as "Count Total Sales"</li>
    </ol>
    <p>Visualizations are stored with the queries that generate data for them. Although we probably could pick a better name than "Counter", this will help us when we build our dashboard later in this lesson. Note also that we can have multiple visualizations attached to a single query</p>
    <ol start="3">
        <li>In the query results section, click "Add Visualization"</li>
        <li>Select "Counter" as the visualization type</li>
        <li>For "Counter Label" type "Total Sales"</li>
        <li>For "Counter Value Column" make sure the column "Total_Sales" is selected</li>
        <li>For "Target Value Column" choose "Sales Goal"</li>
    </ol>
    <p>Note that we can configure the counter to count rows for us if we did not aggregate our data in the query itself.</p>
    <ol start="8">
        <li>Click the "Format" tab</li>
        <li>Optional: Change the decimal character and thousands separator</li>
        <li>"Total Sales" is a dollar figure, so add "$" to "Formatting String Prefix"</li>
        <li>Turn the switch, "Format Target Value" to on</li>
        <li>Click "Save" in the lower-right corner</li>
        <li>Click the text, "Visualization 1" and change the name to "Total Sales"</li>
        <li>Make sure the query is Saved</li>
    </ol>
    <p></p>
    </div>
    
    """, statements="""SELECT sum(total_price) AS Total_Sales, 3000000 AS Sales_Goal 
    FROM sales;""") 

step.render(DA.username)
step.execute(DA.username)

# COMMAND ----------

step = DA.publisher.add_step(True, instructions="""    

<h2><img class="image-icon-inline" src="https://s3.us-west-2.amazonaws.com/files.training.databricks.com/images/step-icon_small.png" alt="heading-icon" />
The Bar Chart</h2>
    <div class="instructions-div">
    <p>One of the most often used visualizations in data analytics is the Bar Chart. Databricks SQL supports an variety of customization options to make bar charts look beautiful. In this example, we are going to configure a bar chart </p>
    <p>Complete the following:</p>
    <ol>
        <li>Open a new query tab, and run the query below</li>
        <li>Save the query as "Sales Over Three Months"</li>
        <li>Click "Add Visualization"</li>
        <li>Select "Bar" as the visualization type</li>
        <li>For "X Column" choose "Month"</li>
        <li>For "Y Columns" click "Add column" and select "Total Sales" and "Sum"</li>
        <li>Click "Add column" again and select "Total Sales" and "Count"</li>
        <li>Click the "X Axis" tab and type "Dollars" in the "Name" field (Left Y Axis)</li>
        <li>Type " " (space character) in the "Name" field (Right Y Axis)</li>
        <li>Click the "Series" tab and type "Total Sales" in the first "Label" field</li>
        <li>Type "Number of Sales" in the second "Label" field and change "Type" to "Line"</li>
        <li>Click "Save" in the lower-right corner</li>
        <li>Click the text, "Visualization 1" and change the name to "Sales by Month"</li>
        <li>Make sure the query is Saved</li>
    </ol>
    <p>As we can see from the visualization, the number of sales in August and October was low, but the dollar amounts of those sales was high. The opposite is true in September.</p>
    </div>
    
    """, statements="""SELECT customer_name, total_price AS Total_Sales, date_format(order_date, "MM") AS Month, product_category 
    FROM sales 
    WHERE order_date >= to_date('2019-08-01') 
    AND order_date <= to_date('2019-10-31');""") 

step.render(DA.username)
step.execute(DA.username)

# COMMAND ----------

step = DA.publisher.add_step(False, instructions="""    

<h2><img class="image-icon-inline" src="https://s3.us-west-2.amazonaws.com/files.training.databricks.com/images/step-icon_small.png" alt="heading-icon" />
The Stacked Bar Chart</h2>
    <div class="instructions-div">
    <p>We can glean more data from the same query by adding a second visualization.</p>
    <p>Complete the following:</p>
    <ol>
        <li>Click "Add Visualization"</li>
        <li>Change "Visualization Type" to "Bar"</li>
        <li>For "X Column" choose "product_category"</li>
        <li>Add two Y Columns and change both to "Total_Sales". Change the first to "Average" and the second to "Min"</li>
        <li>Change "Stacking" to "Stack"</li>
        <li>On the "X Axis" tab, change the name to "Product Category"</li>
        <li>On the "Y Axis" tab, change the name to "Dollars"</li>
        <li>On the "Series" tab, change the first row Label to "Average Sales" and the second row to "Minimum Sales"</li>
        <li>Click "Save" in the lower-right corner</li>
        <li>Click the text, "Visualization 1" and change the name to "Sales by Product Category"</li>
        <li>Make sure the query is Saved</li>
    </ol>
    <p>This visualization shows that, although the "Reagate" category has the highest minimum sales figure, it has the lowest average.</p>
    </div>
    
    """, statements=None) 

step.render(DA.username)
step.execute(DA.username)

# COMMAND ----------

step = DA.publisher.add_step(True, instructions="""    

<h2><img class="image-icon-inline" src="https://s3.us-west-2.amazonaws.com/files.training.databricks.com/images/step-icon_small.png" alt="heading-icon" />
Maps - Choropleth</h2>
    <div class="instructions-div">
    <p>Databricks SQL has two map visualizations you can use to plot address and geolocation data: choropleth and markers. The choropleth map visualization uses color to show the count of a criterion within a specific geographic area. In this example, we are going to use customer address data to plot the number of customers in each U.S. state.</p> 
    <p>To make a choropleth map, complete the following:</p>
    <ol>
        <li>Open a new query tab, and run the query below</li>
        <li>Save the query as "Count Customers by State"</li>
        <li>Click "Add Visualization"</li>
        <li>Select "Map (Choropleth)" as the visualization type</li>
        <li>In the "General" tab, change "Map" to "USA", "Key Column" to state, "Target Field" to "USPS Abbreviation", and "Value Column" to "count(customer_id)"</li>
        <li>Click "Save" in the lower-right corner</li>
        <li>Click the text, "Visualization 1" and change the name to "Most Active States"</li>
        <li>Make sure the query is Saved</li>
    </ol>
    
    </div>
    
    """, statements="""SELECT state, count(customer_id) FROM customers
    GROUP BY state;""") 

step.render(DA.username)
step.execute(DA.username)

# COMMAND ----------

step = DA.publisher.add_step(True, instructions="""    

<h2><img class="image-icon-inline" src="https://s3.us-west-2.amazonaws.com/files.training.databricks.com/images/step-icon_small.png" alt="heading-icon" />
Maps - Markers</h2>
    <div class="instructions-div">
    <p>The Map (Markers) visualization type plots points on a map that signify a specific location. In this example, we have latitude and longitude data for our customer locations. We will use this to plot those locations on a map.</p> 
    <p>Complete the following:</p>
    <ol>
        <li>Open a new query tab, and run the query below</li>
        <li>Save the query as "All Customers"</li>
        <li>Click on "Add Visualization"</li>
        <li>Select "Map (Markers)" as the "Visualization Type"</li>
        <li>In the General tab, change "Latitude Column" to "lat", "Longitude Column" to "lon", and "Group By" to "state"</li>
        <li>On the "Format" tab, enable tooltips and type "&lcub;&lcub;customer_name&rcub;&rcub;" in the "Tooltip template" field</li>
    </ol>
    <p>Note: Because we are on a 2x-Small Endpoint, do not uncheck "Cluster Markers" in the "Styles" tab. The map refresh process will take a very long time to update.</p> 
    <ol>
        <li>Click "Save" in the lower-right corner</li>
        <li>Click the text, "Visualization 1" and change the name to "Customer Locations"</li>
        <li>Make sure the query is Saved</li>
    </ol> 
    
    </div>
    
    """, statements="""SELECT * FROM customers;""") 

step.render(DA.username)
step.execute(DA.username)

# COMMAND ----------

step = DA.publisher.add_step(False, instructions="""    

<h2><img class="image-icon-inline" src="https://s3.us-west-2.amazonaws.com/files.training.databricks.com/images/step-icon_small.png" alt="heading-icon" />
Dashboards</h2>
    <div class="instructions-div">
    <p>We are now going to combine all the visualizations we created above into a dashboard that will display them all at once and that we can put on a refresh schedule to keep the data that underlies each visualization up-to-date. In a future lesson, we will talk about setting a refresh schedule and subscribing stakeholders to the dashboard's output, so they can always have the newest information.</p>
    <p>Complete the following:</p>
    <ol>      
        <li>Click "Dashboards" in the sidebar menu</li>
        <li>Click "Create Dashboard"</li>
        <li>Name the dashboard "Retail Organization"</li>
        <li>Click "Add Visualization"</li>
    </ol>
    <p>We are presented with a list of queries that have been saved. Although we named our queries based on the type of visualization we made, it makes more sense to name a query based on what it does.</p>
    <ol start="5">
        <li>Click "Count Total Sales"</li>
        <li>Drop down "Choose Visualization" and note we have the results table from our query and our counter visualization, "Total Sales" available to us. Select "Total Sales"</li>
        <li>Change "Title" to "Total Sales"</li>
        <li>Optional: write a description</li>
        <li>Repeat steps 4-8 with the "Sales Over Three Months" query ("Sales by Month" and "Sales by Product Category"), "Count Customers by State" query ("Most Active States"), and "All Customers" query ("Customer Locations")</li>
    </ol>
    <p>You should have five visualizations in the dashboard</p>
    <ol start="10">
        <li>Click "Add Textbox" and type "# Customers and Sales"</li>
    </ol>
    <p>Note that text boxes support Markdown.</p>
    <ol start="11">
        <li>Optional: Move the visualizations around by clicking and dragging each one</li>
        <li>Optional: Resize each visualization by dragging the lower-right corner of the visualization</li>
        <li>Optional: Click "Colors" to change the color palette used by visualizations in the dashboard</li>
        <li>Click "Done Editing" in the upper-right corner</li>
        <li>Run every query and refresh all visualizations all at once by clicking "Refresh"</li>
    </ol> 
    </div>
    
    """, statements=None) 

step.render(DA.username) 
step.execute(DA.username)

# COMMAND ----------

step = DA.publisher.add_step(True, instructions="""    

<h2><img class="image-icon-inline" src="https://s3.us-west-2.amazonaws.com/files.training.databricks.com/images/step-icon_small.png" alt="heading-icon" />
Parameterized Queries</h2>
    <div class="instructions-div">
    <p>Before we leave this lesson, let's talk about a customization feature we can apply to our queries to give them more flexibility. Query parameters allow us to make changes to our queries without requiring new code to be written.</p>
    <p>Complete the following:</p>
    <ol>
        <li>Go back to the Query Editor and start a new query</li>
        <li>Paste the query below into the editor and save the query as "Get Product Category"</li>
</ol>
    </div>
    
    """, statements="SELECT DISTINCT product_category FROM sales;") 

step.render(DA.username)
step.execute(DA.username)

# COMMAND ----------

step = DA.publisher.add_step(True, instructions="""    

<h3>Writing a Query with Parameters</h3>
    <div class="instructions-div">
    <p>Now that we have a query that pulls all product categories from the <span class="monofont">Sales</span> table, let's use this query as a parameter in a second query.</p>
    <p>Complete the following:</p>
    <ol start="3">
        <li>Start a new query, and paste the code below in the editor</li>
    </ol>
    <p>Note that the query has empty single quotes.</p>
    <ol start="4">
        <li>Place your cursor in-between the single quotes, and click the icon in the lower-left corner of the query editor window that looks like two curly braces</li>
        <li>Input "category" for the "Keyword" field</li>
        <li>Drop down "Type" and choose "Query Based Dropdown List"</li>
        <li>For "Query" choose the query we created above: "Get Product Category"</li>
        <li>Click "Add Parameter"</li>
        <li>Save the query as "Total Sales by Product Category"</li>
    </ol>
    <p>Note two things: First, we now have a set of double curly braces that contain the word "category". This is where are query parameter was inserted. Finally, note the dropdown list we how have just above the query results window.</p>
    <ol start="10">
        <li>Open the dropdown list and choose a Category from the list</li>
        <li>Click "Apply Changes"</li>
    </ol>
    <p>The query is rerun with the chosen product category replacing the location of the query parameter in the query. Thus, we see the Total Sales of the Category we chose.</p>
    </div>
    
    """, statements="""SELECT sum(total_price) AS Total_Sales FROM sales
    WHERE product_category = '';""") 

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
