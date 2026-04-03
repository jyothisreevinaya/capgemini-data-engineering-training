# Phase 4 – Business Pipeline & Analytics (PySpark)

## Objective

The objective of this phase is to build an end-to-end data pipeline using PySpark.
This includes extracting data, performing transformations, and generating business insights such as sales trends, customer behavior, and segmentation.

## Problem Summary

We were given datasets containing customer and order information.
The tasks involved:
* Cleaning the data (handling nulls, duplicates, invalid values)
* Performing aggregations like daily sales and city-wise revenue
* Identifying key insights such as top customers and repeat customers
* Segmenting customers based on spending
* Creating a final reporting table
* 
## Approach

1. Loaded data into PySpark DataFrames
2. Performed data cleaning:
   * Removed null values
   * Filtered invalid data (negative amounts, incorrect age)
3. Joined customer and order datasets using `customer_id`
4. Applied transformations:
   * Aggregations using 'groupBy()' and 'agg()'
   * Filtering using 'filter()'
   * Sorting and limiting results
5. Created customer segmentation logic (Gold, Silver, Bronze)
6. Generated final reporting dataset
7. Saved output to CSV

## Key Transformations Used

* 'join()' -> Combine customers and orders
* 'groupBy()' -> Aggregate data
* 'agg()' -> Calculate sum and count
* 'filter()' -> Remove invalid data
* 'orderBy()' -> Sort results
* 'withColumn()'-> Create new columns (segmentation)
* 
## Output / Results

The following outputs were generated:

* Daily Sales (date-wise total sales)
* City-wise Revenue
* Top 5 Customers
* Repeat Customers
* Customer Segmentation (Gold, Silver, Bronze)
* Final Reporting Table

Output files are saved in the 'outputs' folder.

## Data Engineering Considerations

* Removed null keys before joins to avoid incorrect results
* Ensured valid data before aggregation
* Used proper join conditions to avoid duplication
* Structured pipeline into Extract → Transform → Load

## Challenges Faced

* Understanding join operations between datasets
* Applying correct aggregation logic
* Implementing segmentation conditions
* Handling file output paths in PySpark

## Learnings

* How to build an ETL pipeline in PySpark
* Importance of data cleaning before processing
* How business insights are generated using aggregations
* Differences between SQL and PySpark transformations

## Pipeline Overview

1. **Extract** → Load data into DataFrames
2. **Transform** → Clean, join, aggregate, segment
3. **Load** → Save final output to CSV

## Files in this Folder

* `solution.py' → PySpark implementation
* 'phase4_problem_statement.pdf' → Problem statement
* 'outputs' → Output files
