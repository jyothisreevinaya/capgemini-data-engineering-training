# Phase 3A – Data Quality & Cleaning Challenge (PySpark)

## Objective

In this phase, the goal is to work with "messy data" and apply "data cleaning techniques" using PySpark.
This includes identifying data issues, cleaning the dataset, validating results, and performing aggregation.

## Problem Summary

We were given a dataset containing customer information with intentional data issues such as:

• Null values
• Duplicate records
• Invalid values (e.g., negative age)

The task was to:

• Identify data quality issues
• Clean the dataset
• Validate the cleaning process
• Perform aggregation to generate insights

## Approach

1. Loaded dataset into a PySpark DataFrame

2. Identified data issues:
   o Checked for null values
   o Detected duplicate records
   o Identified invalid values (age ≤ 0)

3. Applied data cleaning steps:
   o Removed rows with null  'customer_id'
   o Filled missing values in 'name' and 'city'
   o Removed duplicate records
   o Filtered invalid age values

4. Validated the cleaned data:
   o Compared row counts before and after cleaning

5. Performed aggregation:
   o Counted number of customers per city

## Key Transformations Used

• 'filter()' -> to remove null and invalid records
• 'fillna()' -> to handle missing values
• 'dropduplicates()' -> to remove duplicate rows
•  'groupBy()'  -> for aggregation
• 'count()' -> to calculate number of customers

## Output / Results

The following outputs were generated:

• Cleaned dataset without null keys, duplicates, and invalid values
• Validation metrics (row count before and after cleaning)
• Aggregated data showing number of customers per city

Screenshots of outputs are available in the outputs/ folder.

## Data Engineering Considerations

• Removed null primary keys to maintain data integrity
• Handled missing values to avoid incorrect results
• Eliminated duplicates to ensure accurate aggregation
• Filtered invalid data (negative age)
• Validated results to ensure correctness

## Challenges Faced

• Identifying all types of data issues in the dataset
• Deciding how to handle missing values
• Ensuring correct order of cleaning steps

## Learnings

• Importance of data cleaning in real-world datasets
• How to handle null values and duplicates in PySpark
• Impact of bad data on analysis results
• Need for validation after cleaning

## Files in this Folder

• 'pyspark_code.py' → PySpark implementation
• 'phase3a_problem_statement.pdf'→ Problem description
• 'outputs/' → Output screenshots

