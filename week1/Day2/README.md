##  Objective

To combine multiple datasets and generate insights using joins and aggregations.

##  Approach

1. Loaded datasets into tables/DataFrames
2. Performed joins between tables using common keys
3. Applied aggregations to summarize data
4. Filtered and structured the results

##  Key Transformations Used

* 'JOIN' → Combine multiple tables
* 'GROUP BY' → Aggregate data
* 'SUM(), COUNT()' → Calculate metrics
* 'FILTER / WHERE' → Remove unwanted records

##  Output / Results

* Aggregated customer-level data
* Summary metrics from transaction
* By using joins, I was able to connect related tables (like customers, accounts, and transactions) using common keys. 
* Using group by, I was able to organize data into categories (such as customer-wise data) and perform calculations on each group.


## Challenges Faced

* Handling inconsistent data formats
* Managing multiple date formats
* Understanding join relationships between tables
* Avoiding duplicate records after joins

##  Learnings

* Importance of data cleaning before analysis
* Real-world data is often messy and inconsistent
* How joins and aggregations work in practice
* Using PySpark and SQL together for data processing

## Files in this Project

* 'joins.ipynb' → Join operations
* 'groupby_sql.ipynb' → Aggregations and SQL queries

