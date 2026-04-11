## Objective

In this phase, the goal is to clean and transform the given dataset using PySpark.
This includes handling missing values, standardizing data, formatting dates, and removing duplicates to prepare the data for analysis.

## Problem Summary

We were given a CSV dataset containing raw data with inconsistencies such as:

* Blank and missing values
* Inconsistent text formats
* Multiple date formats
* Duplicate records

The task was to:

* Clean the dataset
* Standardize values
* Convert data into proper formats
* Ensure data quality and consistency

## Approach

1. Loaded the dataset into a PySpark DataFrame
2. Replaced invalid text values like "blank" with null
3. Standardized text data (converted Country to uppercase)
4. Handled missing values using fillna()
5. Converted JoinDate into proper date format using multiple formats
6. Removed duplicate records

## 🔹 Key Transformations Used

* 'read.csv()'→ to load the dataset
* 'replace()' → to handle "blank" values
* 'withColumn()' → to transform columns
* 'upper()' → to standardize text
* 'fillna()' → to handle missing values
* 'try_to_date()' → to parse dates
* 'coalesce()' → to handle multiple date formats
* 'ropDuplicates()' → to remove duplicate records

## Output / Results

The following output was generated:

* Cleaned dataset with no invalid "blank" values
* Standardized Country column (uppercase)
* Missing values handled appropriately
* Properly formatted JoinDate column
* Duplicate records removed

## Challenges Faced

* Handling multiple date formats in a single column
* Identifying and replacing inconsistent "blank" values
* Ensuring no data loss while cleaning
* 
## Learnings

* Importance of data cleaning in real-world datasets
* How to handle inconsistent and messy data
* Using PySpark functions effectively for transformations
* Ensuring data quality before analysis

## Files in this Folder

* 'C1 csv.ipynb' → Input dataset ,code implementation, cleaned output data
