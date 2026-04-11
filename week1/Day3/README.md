## Objective

The main goal of this task is to clean and transform the dataset using "CASE WHEN, regex functions, and null handling techniques".

These methods help in:

* Applying conditions to data
* Cleaning messy text values
* Handling missing (null) values properly

## Problem Summary

The dataset contained several common issues such as:

* Missing (null) values
* Values like "blank" which should be treated as null
* Inconsistent and messy text data
* Need to classify data based on different conditions

## Approach

* Loaded the dataset into a DataFrame/table
* Replaced "blank" values with null
* Handled missing values using suitable defaults
* Used CASE WHEN to apply conditions on data
* Used regex functions to clean and format text

## Key Transformations Used

### CASE WHEN

* Used to apply conditions like if-else
* Helped in creating new columns based on conditions
* Also used nested CASE WHEN for multiple conditions

###  Regex Functions

* Used to clean text data
* Removed unwanted characters
* Checked patterns in data
* Functions like 'regexp_replace()' and 'rlike()' were used
  
###  Handling Null Values

* Replaced invalid values (like "blank") with null
* Used 'fillna()' to fill missing values
* Ensured no important columns had missing data

##  Output / Results

* Data was cleaned by removing unwanted text and fixing formats
* Missing values were handled properly
* Using CASE WHEN, data was categorized into meaningful groups
* Regex helped in making text data consistent

##  Data Engineering Considerations

* Proper handling of null values to avoid errors
* Clean and consistent text data
* Correct use of conditions for accurate results

##  Challenges Faced

* Identifying which values should be treated as null
* Writing correct conditions in CASE WHEN
* Understanding regex patterns for cleaning data

## Learnings

* Learned how to use CASE WHEN for applying logic
* Understood how regex helps in cleaning data
* Realized the importance of handling null values
* Gained confidence in working with real-world messy data

## Files in this Project

* "Nested case and when.ipynb" → It consists of Nested Case and when input data file, code implmentation and outputs
* "Null values.ipynb" → It consists of Null values input data file, code implmentation and outputs
* "windowfunctions.ipynb" → It consists of  input data file, code implmentation and outputs


