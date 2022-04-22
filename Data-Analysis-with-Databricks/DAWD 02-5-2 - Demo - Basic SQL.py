# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md 
# MAGIC # Demo: Basic SQL

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup

# COMMAND ----------

step = DA.publisher.add_step(False, instructions="""    

    <h2><img class="image-icon-inline" src="https://s3.us-west-2.amazonaws.com/files.training.databricks.com/images/step-icon_small.png" alt="heading-icon" />
    Lesson Objective</h2>
    <div class="instructions-div">
    <p>At the end of this lesson, you will be able to:</p>
    <ul>
    <li>Describe how to write basic SQL queries to subset tables using Databricks SQL Queries</li>
    </ul></div>
    
    """, statements=None) 

step.render(DA.username)
step.execute(DA.username) 

# COMMAND ----------

step = DA.publisher.add_step(True, instructions="""    

<h2><img class="image-icon-inline" src="https://s3.us-west-2.amazonaws.com/files.training.databricks.com/images/step-icon_small.png" alt="heading-icon" />
Retrieving Data</h2>
    <h3>SELECT</h3>
    <div class="instructions-div">
    <ol>
        <li>Run the code below.</li>
    </ol>
    <p>This simple command retrieves data from a table. The "*" represents "Select All," so the command is selecting all data from the table</p>
    <p>However, note that only 1,000 rows were retrieved. Databricks SQL defaults to only retrieving 1,000 rows from a table. If you wish to retrieve more, deselect the checkbox "LIMIT 1000".</p>
    </div>
    
    """, statements="SELECT * FROM customers;") 

step.render(DA.username)
step.execute(DA.username)

# COMMAND ----------

step = DA.publisher.add_step(True, instructions="""    

   <h3>SELECT ... AS</h3>
    <div class="instructions-div">
    <p>By adding the <span class="monofont">AS</span> keyword, we can change the name of the column in the results.</p>
    <ol start="2">
        <li>Run the code below.</li>
    </ol>
    <p>Note that the column <span class="monofont">customer_name</span> has been renamed <span class="monofont">Customer</span></p>
    </div>
    
    """, statements="SELECT customer_name AS Customer FROM customers;") 

step.render(DA.username)
step.execute(DA.username)

# COMMAND ----------

step = DA.publisher.add_step(True, instructions="""    

   <h3>DISTINCT</h3>
    <div class="instructions-div">
    <p>If we add the <span class="monofont">DISTINCT</span> keyword, we can ensure that we do not repeat data in the table.</p>
    <ol start="3">
        <li>Run the code below.</li>
    </ol>
    <p>There are more than 1,000 records that have a state in the <span class="monofont">state</span> field. But, we only see 51 results because there are only 51 distinct state names.</p>
    </div>
    
    """, statements="SELECT DISTINCT state FROM customers;") 

step.render(DA.username)
step.execute(DA.username)

# COMMAND ----------

step = DA.publisher.add_step(True, instructions="""    

   <h3>WHERE</h3>
    <div class="instructions-div">
    <p>The <span class="monofont">WHERE</span> keyword allows us to filter the data.</p>
    <ol start="4">
        <li>Run the code below.</li>
    </ol>
    <p>We are selecting from the <span class="monofont">customers</span> table, but we are limiting the results to those customers who have a <span class="monofont">loyalty_segment</span> of 3.</p>
    </div>
    
    """, statements="SELECT * FROM customers WHERE loyalty_segment = 3;") 

step.render(DA.username)
step.execute(DA.username)

# COMMAND ----------

step = DA.publisher.add_step(True, instructions="""    

   <h3>GROUP BY</h3>
    <div class="instructions-div">
    <p>We can run a simple <span class="monofont">COUNT</span> aggregation by adding <span class="monofont">count()</span> and <span class="monofont">GROUP BY</span> to our query.</p>
    <ol start="5">
        <li>Run the code below.</li>
    </ol>
    <p><span class="monofont">GROUP BY</span> requires an aggregating function. We will discuss more aggregations later on.</p>
    </div>
    
    """, statements="SELECT loyalty_segment, count(loyalty_segment) FROM customers GROUP BY loyalty_segment;") 

step.render(DA.username)
step.execute(DA.username)

# COMMAND ----------

step = DA.publisher.add_step(True, instructions="""    

   <h3>ORDER BY</h3>
    <div class="instructions-div">
    <p>By adding <span class="monofont">ORDER BY</span> to the query we just ran, we can place the results in a specific order.</p>
    <ol start="6">
        <li>Run the code below.</li>
    </ol>
    <p><span class="monofont">ORDER BY</span> defaults to ordering in ascending order. We can change the order to descending by adding <span class="monofont">DESC</span> after the <span class="monofont">ORDER BY</span> clause.</p>
    </div>
    
    """, statements="SELECT loyalty_segment, count(loyalty_segment) FROM customers GROUP BY loyalty_segment ORDER BY loyalty_segment;") 

step.render(DA.username)
step.execute(DA.username)

# COMMAND ----------

step = DA.publisher.add_step(True, instructions="""    

<h2><img class="image-icon-inline" src="https://s3.us-west-2.amazonaws.com/files.training.databricks.com/images/step-icon_small.png" alt="heading-icon" />
Column Expressions</h2>
    <h3>Mathematical Expressions of Two Columns</h3>
    <div class="instructions-div">
    <p>In our queries, we can run calculations on the data in our tables. This can range from simple mathematical calculations to more complex computations involving built-in functions.</p>
    <ol start="7">
    <li>Run the code below.</li>
    </ol>
    <p>This code displays three columns from the <span class="monofont">silver_promo_prices</span> table. If the calculation in the <span class="monofont">discounted_price</span> column was done correctly, it should equal the <span class="monofont">sales_price</span> minus the <span class="monofont">promo_disc</span>.</p>
    </div>
    
    """, statements="SELECT sales_price, promo_disc, discounted_price FROM promo_prices;") 

step.render(DA.username)
step.execute(DA.username)

# COMMAND ----------

step = DA.publisher.add_step(True, instructions="""    

    <div class="instructions-div">
    <p>We can check the proper calculation of the discounted price by performing a calculation.</p>
    <ol start="8">
    <li>Run the code below.</li>
    </ol>
    <p>The results show that the Calculated Discount, the one we generated using Column Expressions, matches the Discounted Price.</p>
    </div>
    
    """, statements="SELECT sales_price - sales_price * promo_disc AS Calculated_Discount, discounted_price AS Discounted_Price FROM promo_prices;") 

step.render(DA.username)
step.execute(DA.username)

# COMMAND ----------

step = DA.publisher.add_step(True, instructions="""    

    <h3>Built-In Functions -- String Column Manipulation</h3>
    <div class="instructions-div">
    <p>There are many, many <a href="https://docs.databricks.com/sql/language-manual/sql-ref-functions-builtin.html">Built-In Functions</a>. We are going to talk about just a handful, so you can get a feel for how they work.</p>
    <ol start="9">
    <li>Run the code below.</li>
    </ol>
    <p>Take a look at the <span class="monofont">city</span> column. The city names are mostly in all capital letters, and some are mixed case. We want to get them all into mixed case format.</p>
    </div>
    
    """, statements="SELECT * FROM customers;") 

step.render(DA.username)
step.execute(DA.username)

# COMMAND ----------

step = DA.publisher.add_step(True, instructions="""    

    <div class="instructions-div">
    <p>We are going to use a built-in function called <span class="monofont">lower()</span>. This function takes a string expression and returns the same expression with all characters changed to lowercase. Let's have a look.</p>
    <ol start="10">
    <li>Run the code below.</li>
    </ol>
    <p>Although the letters are now all lowercase, they are not the way the need to be. We want to have the first letter of each word capitalized.</p>
    </div>
    
    """, statements="SELECT lower(city) AS City FROM customers;") 

step.render(DA.username)
step.execute(DA.username)

# COMMAND ----------

step = DA.publisher.add_step(True, instructions="""    

    <div class="instructions-div">
    <p>Let's add a second built-in function, <span class="monofont">initcap()</span>. This function also takes a string expression and returns the same expression with the first character in each word changed to uppercase. We are going to nest the two functions so that the string expression returned by <span class="monofont">lower()</span> is used as the input to <span class="monofont">initcap()</span>.</p>
    <ol start="11">
    <li>Run the code below.</li>
    </ol>
    <p>Now, the city names are correctly capitalized.</p>
    </div>
    
    """, statements="SELECT initcap(lower(city)) AS City FROM customers;") 

step.render(DA.username)
step.execute(DA.username)

# COMMAND ----------

step = DA.publisher.add_step(True, instructions="""    

    <h3>Date Functions</h3>
    <div class="instructions-div">
    <p>In the <span class="monofont">promo_prices</span> table, there is a column called <span class="monofont">promo_began</span>.</p>
    <ol start="12">
    <li>Run the code below.</li>
    </ol>
    <p>We want to use a function to make the date more human-readable.</p>
    </div>
    
    """, statements="SELECT * FROM promo_prices;") 

step.render(DA.username)
step.execute(DA.username)

# COMMAND ----------

step = DA.publisher.add_step(True, instructions="""    

    <div class="instructions-div">
    <p>Let's use <span class="monofont">from_unixtime()</span>.</p>
    <ol start="13">
    <li>Run the code below.</li>
    </ol>
    <p>The date looks better, but let's adjust the formatting. Formatting options for many of the date and time functions are available <a href="https://docs.databricks.com/spark/latest/spark-sql/language-manual/sql-ref-datetime-pattern.html">here</a>.</p>
    </div>
    
    """, statements="SELECT from_unixtime(promo_began, 'd MMM, y') AS Beginning_Date FROM promo_prices;") 

step.render(DA.username)
step.execute(DA.username)

# COMMAND ----------

step = DA.publisher.add_step(True, instructions="""    

    <h3>Date Calculations</h3>
    <div class="instructions-div">
    <p>Let's determine how long a specific promotion has been going by running a date calculation on the <span class="monofont">promo_began</span> column.</p>
    <ol start="14">
    <li>Run the code below.</li>
    </ol>
    <p>In this code, we are using the function <span class="monofont">current_date()</span> to get today's date. We are then nesting <span class="monofont">from_unixtime()</span> inside <span class="monofont">to_date</span> in order to convert <span class="monofont">promo_began</span> to a date object. We can then run the calculation.</p>
    </div>
    
    """, statements="SELECT current_date() - to_date(from_unixtime(promo_began)) FROM promo_prices;") 

step.render(DA.username)
step.execute(DA.username)

# COMMAND ----------

step = DA.publisher.add_step(True, instructions="""    

    <h3>CASE ... WHEN</h3>
    <div class="instructions-div">
    <p>Often, it is important for us to use conditional logic in our queries. <span class="monofont">CASE ... WHEN</span> provides us this ability.</p>
    <ol start="15">
    <li>Run the code below.</li>
    </ol>
    <p>This statement allows us to change numeric values that represent loyalty segments into human-readable strings. It is certainly true that this association would more-likely occur using a join on two tables, but we can still see the logic behind <span class="monofont">CASE ... WHEN</span></p>
    </div>
    
    """, statements="""SELECT customer_name, loyalty_segment,
    CASE 
        WHEN loyalty_segment = 0 THEN 'Rare'
        WHEN loyalty_segment = 1 THEN 'Occasional'
        WHEN loyalty_segment = 2 THEN 'Frequent'
        WHEN loyalty_segment = 3 THEN 'Daily'
    END AS Loyalty 
FROM customers;""") 

step.render(DA.username)
step.execute(DA.username)

# COMMAND ----------

step = DA.publisher.add_step(True, instructions="""    

<h2><img class="image-icon-inline" src="https://s3.us-west-2.amazonaws.com/files.training.databricks.com/images/step-icon_small.png" alt="heading-icon" />
Updating Data</h2>
    <div class="instructions-div">
    <p>So far, all of the changes we have made to our data have only been in the results set. We haven't made any actual changes to our tables. In this section, we are going to run some command that will make changes to the data in the tables.</p>
    <ol start="16">
        <li>Run the code below.</li>
    </ol>
    <p>Recall that earlier we looked at capitalization in the city names in our <span class="monofont">customers</span> table. Note that those changes were not implemented in the table itself.</p>
    </div>
    
    """, statements="SELECT city FROM customers;") 

step.render(DA.username)
step.execute(DA.username)

# COMMAND ----------

step = DA.publisher.add_step(True, instructions="""    

<h3>UPDATE</h3>
    <div class="instructions-div">
    <p>Let's make those changes.</p>
    <ol start="17">
        <li>Run the code below.</li>
    </ol>
    <p>The <span class="monofont">UPDATE</span> does exactly what it sounds like: It updates the table based on the criteria specified.</p>
    </div>
    
    """, statements=["UPDATE customers SET city = initcap(lower(city));", "SELECT city FROM customers;"]) 

step.render(DA.username)
step.execute(DA.username)

# COMMAND ----------

step = DA.publisher.add_step(True, instructions="""    

<h3>INSERT INTO</h3>
    <div class="instructions-div">
    <p>In addition to updating data, we can insert new data into the table.</p>
    <ol start="18">
        <li>Run the code below.</li>
    </ol>
    <p>We can see that there are four loyalty segments in our table.</p>
    </div>
    
    """, statements="SELECT * FROM loyalty_segments;") 

step.render(DA.username)
step.execute(DA.username)

# COMMAND ----------

step = DA.publisher.add_step(True, instructions="""    

    <div class="instructions-div">
    <p><span class="monofont">INSERT INTO</span> is a command for inserting data into a table.</p>
    <ol start="19">
        <li>Run the code below.</li>
    </ol>
    <p>We run the <span class="monofont">INSERT INTO</span> command, and <span class="monofont">SELECT</span> from the table and see the newly inserted data.</p>
    </div>
    
    """, statements=["""INSERT INTO loyalty_segments 
    (loyalty_segment_id, loyalty_segment_description, unit_threshold, valid_from, valid_to)
    VALUES
    (4, 'level_4', 100, current_date(), Null);""", "SELECT * FROM loyalty_segments;"]) 

step.render(DA.username)
step.execute(DA.username)


# COMMAND ----------

step = DA.publisher.add_step(True, instructions="""    

    <h3>INSERT TABLE</h3>
    <div class="instructions-div">
    <p><span class="monofont">INSERT TABLE</span> is a command for inserting entire tables into other tables. There are two tables <span class="monofont">suppliers</span> and <span class="monofont">source_suppliers</span> that currently have the exact same data. Let's run a <span class="monofont">SELECT</span> and take a look at the data.</p>
    <ol start="20">
        <li>Run the code below.</li>
    </ol>
    </div>
    
    """, statements="SELECT * FROM suppliers;") 

step.render(DA.username)
step.execute(DA.username)


# COMMAND ----------

step = DA.publisher.add_step(True, instructions="""    

    <div class="instructions-div">
    <p>Note the number of rows. Now, let's insert the <span class="monofont">source_suppliers</span> table.</p>
    <ol start="21">
        <li>Run the code below.</li>
    </ol>
    <p>After selecting from the table again, we note that the number of rows has doubled. This is because <span class="monofont">INSERT TABLE</span> inserts all data in the source table, whether or not there are duplicates.</p>
    </div>
    
    """, statements=["INSERT INTO suppliers TABLE source_suppliers;", "SELECT * FROM suppliers;"]) 

step.render(DA.username)
step.execute(DA.username)

# COMMAND ----------

step = DA.publisher.add_step(True, instructions="""    

    <h3>INSERT OVERWRITE</h3>
    <div class="instructions-div">
    <p>If we want to completely replace the contents of a table, we can use <span class="monofont">INSERT OVERWRITE</span>.</p>
    <ol start="22">
        <li>Run the code below.</li>
    </ol>
    <p>After running <span class="monofont">INSERT OVERWRITE</span> and then retrieving a <span class="monofont">count(*)</span> from the table, we see that we are back to the original count of rows in the table. <span class="monofont">INSERT OVERWRITE</span> has replaced all the rows.</p>
    </div>
    
    """, statements=["INSERT OVERWRITE suppliers TABLE source_suppliers;", "SELECT * FROM suppliers;"]) 

step.render(DA.username)
step.execute(DA.username)

# COMMAND ----------

step = DA.publisher.add_step(True, instructions="""    

<h2><img class="image-icon-inline" src="https://s3.us-west-2.amazonaws.com/files.training.databricks.com/images/step-icon_small.png" alt="heading-icon" />
Subqueries</h2>
    <div class="instructions-div">
    <p>Let's create two new tables.</p>
    <ol start="23">
    <li>Run the code below.</li>
    </ol>
    <p>These two command use subqueries to <span class="monofont">SELECT</span> from the <span class="monofont">customers</span> table using specific criteria. The results are then fed into <span class="monofont">CREATE OR REPLACE TABLE</span> and <span class="monofont">CREATE OR REPLACE TABLE</span> statements. Incidentally, this type of statement is often called a <span class="monofont">CTAS</span> statement for <span class="monofont">CREATE OR REPLACE TABLE ... AS</span>.</p>
    </div>
    
    """, statements=["""CREATE OR REPLACE TABLE high_loyalty_customers AS
    SELECT * FROM customers WHERE loyalty_segment = 3;""", """CREATE OR REPLACE TABLE low_loyalty_customers AS
    SELECT * FROM customers WHERE loyalty_segment = 1;
    """]) 

step.render(DA.username)
step.execute(DA.username)

# COMMAND ----------

step = DA.publisher.add_step(True, instructions="""    

<h2><img class="image-icon-inline" src="https://s3.us-west-2.amazonaws.com/files.training.databricks.com/images/step-icon_small.png" alt="heading-icon" />
Joins</h2>
    <div class="instructions-div">
    <p>We are now going to run a couple of <span class="monofont">JOIN</span> queries. The first is the most common <span class="monofont">JOIN</span>, an <span class="monofont">INNER JOIN</span>. Since <span class="monofont">INNER JOIN</span> is the default, we can just write <span class="monofont">JOIN</span>.</p>
    <ol start="24">
    <li>Run the code below.</li>
    </ol>
    <p>In this statement, we are joining the <span class="monofont">customers</span> table and the <span class="monofont">loyalty_segments</span> tables. When the <span class="monofont">loyalty_segment</span> from the <span class="monofont">customers</span> table matches the <span class="monofont">loyalty_segment_id</span> from the <span class="monofont">loyalty_segments</span> table, the rows are combined. We are then able to view the <span class="monofont">customer_name</span>, <span class="monofont">loyalty_segment_description</span>, and <span class="monofont">unit_threshold</span> from both tables.</p>
    </div>
    
    """, statements="""SELECT
        customer_name,
        loyalty_segment_description,
        unit_threshold
    FROM
        customers
    INNER JOIN loyalty_segments
        ON customers.loyalty_segment = loyalty_segments.loyalty_segment_id;""") 

step.render(DA.username)
step.execute(DA.username)

# COMMAND ----------

step = DA.publisher.add_step(True, instructions="""    

<h3>CROSS JOIN</h3>
    <div class="instructions-div">
    <p>Even though the <span class="monofont">CROSS JOIN</span> isn't used very often, I wanted to demonstrate it.</p>
    <ol start="25">
    <li>Run the code below.</li>
    </ol>
    <p>First of all, note the use of <span class="monofont">UNION ALL</span>. All this does is combine the results of all three queries, so we can view them all in one results set. The <span class="monofont">Customers</span> row shows the count of rows in the <span class="monofont">customers</span> table. Likewise, the <span class="monofont">Sales</span> row shows the count of the <span class="monofont">sales</span> table. <span class="monofont">Crossed</span> shows the number of rows after performing the <span class="monofont">CROSS JOIN</span>.</p>
    </div>
    
    """, statements="""SELECT
  "Sales", count(*)
FROM
  sales
UNION ALL
SELECT
  "Customers", count(*)
FROM
  customers
UNION ALL
SELECT
  "Crossed", count(*)
FROM
  customers
  CROSS JOIN sales;""") 

step.render(DA.username)
step.execute(DA.username)

# COMMAND ----------

step = DA.publisher.add_step(True, instructions="""    

<h2><img class="image-icon-inline" src="https://s3.us-west-2.amazonaws.com/files.training.databricks.com/images/step-icon_small.png" alt="heading-icon" />
Aggregations</h2>
    <div class="instructions-div">
    <p>Now, let's move into aggregations. There are many aggregating functions you can use in your queries. Here are just a handful.</p>
    <ol start="26">
        <li>Run the code below.</li>
    </ol>
    <p>Again, we are viewing the results of a handful of queries using a <span class="monofont">UNION ALL</span>.</p>
    </div>
    
    """, statements="""SELECT 
        "Sum" Function_Name, sum(units_purchased) AS Value
    FROM customers 
    WHERE state = 'CA'
UNION ALL
SELECT 
        "Min", min(discounted_price) AS Lowest_Discounted_Price 
    FROM promo_prices
UNION ALL
SELECT 
        "Max", max(discounted_price) AS Highest_Discounted_Price 
    FROM promo_prices
UNION ALL
SELECT 
        "Avg", avg(total_price) AS Mean_Total_Price 
    FROM sales
UNION ALL
SELECT 
        "Standard Deviation", std(total_price) AS SD_Total_Price 
    FROM sales
UNION ALL
SELECT 
        "Variance", variance(total_price) AS Variance_Total_Price
        FROM sales;
    """) 

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
