# SQL to PySpark – Phase 3: Final ETL Pipeline

## Objective

In this phase, the goal is to build a complete "ETL (Extract, Transform, Load) pipeline" using PySpark.
This includes reading data, cleaning it, performing transformations, and generating meaningful business insights.

## Problem Summary

We were given datasets such as "customers and orders".

The task was to:

* Perform data cleaning
* Apply transformations and aggregations
* Generate business insights
* Build a complete ETL pipeline

## Approach
1. Created datasets and loaded them into PySpark DataFrames
2. Performed data cleaning:
   * Removed null values
   * Filtered invalid records
3. Applied transformations:
   * Joins between tables
   * Aggregations using groupBy
   * Filtering conditions
   * Window functions
4. Generated multiple business outputs
5. Displayed and saved final results

# ETL Pipeline

Every data engineering workflow follows three main steps:

- Extract → Reading or collecting data from sources
- Transform → Cleaning, filtering, joining, and aggregating data
- Load → Saving or displaying the final processed data

This phase applies all three steps using PySpark.

let me take a sample query 1 to understand ETL pipeline:

## Extract

* Created DataFrames using sample data
* Simulates reading from CSV/Database

## Transform

### PySpark Queries

### 1. Daily Sales

orders_clean.groupBy("order_date") \
    .agg(sum("amount").alias("total_daily_sales"))
    
###  SQL Queries

### 1. Daily Sales

SELECT order_date, SUM(amount) AS total_daily_sales
FROM orders
GROUP BY order_date;

##  Load

* Displayed outputs using '.show()'

## Output / Results

The following outputs were generated:

* Daily Sales
* City-wise Revenue
* Repeat Customers
* Top Customers per City
* Final Reporting Table

Screenshots of outputs are available in the **outputs/** folder.

## Data Engineering Considerations

* Handled null values to avoid incorrect results
* Ensured joins are correct to prevent duplication
* Used aggregation carefully for accurate insights
* Applied window functions for advanced analysis

## Challenges Faced

* Understanding join conditions
* Handling missing/invalid data
* Implementing window functions

## Key Learnings

* Built a complete **ETL pipeline using PySpark**
* Learned:
* 'filter()' for cleaning
* `join()' for combining data
* 'groupBy()' and `agg()' for aggregation
* 'count()' and `sum()' functions
* Window functions like `rank()'
* Understood how SQL queries are converted into PySpark

##Files in this Folder

* `etl_pipeline.py' -> ETL pipeline implementation
* 'pyspark.py' -> pyspark codes
* `sql_queries.sql'  -> SQL queries
* 'README.md' -> Documentation
*  'outputs/' -> Output screenshots

## Conclusion

This phase helped in transitioning from writing individual queries to building a "complete data engineering pipeline" , improving practical understanding of ETL processes in PySpark.

