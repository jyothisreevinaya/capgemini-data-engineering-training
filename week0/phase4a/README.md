
# Phase 4A – Bucketing & Segmentation using PySpark

## Objective

In this phase, the goal is to understand how to convert continuous data into categories using PySpark.
This includes applying different bucketing and segmentation techniques to simplify data analysis.

## Problem Summary

We were given a dataset containing continuous values (such as customer spending).
The task was to:

• Convert continuous values into categories (Gold, Silver, Bronze)
• Apply different segmentation methods
• Compare results from multiple approaches
• Generate insights from segmented data

## Approach

1. Loaded dataset into PySpark DataFrame
2. Checked and cleaned data if required
3. Applied segmentation using multiple methods:
   o Conditional logic
   o SQL CASE statement
   o Bucketizer (MLlib)
   o Quantile-based segmentation
   o Window functions
4. Grouped data based on segments
5. Compared outputs from different methods

## Core Concept

Bucketing (or segmentation) is the process of dividing continuous values into categories.
For example:

• High value → Gold
• Medium value → Silver
• Low value → Bronze

This helps in simplifying analysis and supports better business decisions.

## Methods Used

• **when()** → to apply conditional logic
• **CASE statement** → SQL-based segmentation
• **Bucketizer** → to create buckets using defined splits
• **approxQuantile()** → for quantile-based segmentation
• **Window functions (percent_rank)** → ranking-based segmentation

## Output / Results

The following outputs were generated:

• Customer segments (Gold, Silver, Bronze)
• Count of customers in each segment
• Comparison of segmentation methods

## Data Engineering Considerations

• Ensured correct threshold values for segmentation
• Handled edge cases in data ranges
• Verified segmentation results for accuracy
• Compared fixed vs dynamic segmentation approaches

## Challenges Faced

• Choosing appropriate thresholds for segmentation
• Understanding differences between multiple methods
• Handling uneven data distribution
• Interpreting quantile-based results

## Learnings

• Bucketing helps simplify complex data
• Conditional logic is simple and widely used
• Bucketizer is useful in machine learning pipelines
• Quantile-based segmentation adapts to data distribution
• Window functions help in ranking and advanced segmentation

## Reflection

• Continuous data is converted into categories for easy analysis
• Business segmentation depends on requirements, while technical bucketing depends on implementation
• Fixed thresholds may not work well for all datasets
• Quantile-based segmentation provides balanced grouping
• In real-world projects, a combination of methods is often used

## Files in this Folder

• solution.py → PySpark implementation
• phase4a_problem_statement.pdf → Problem description
• outputs/ → Output screenshots


