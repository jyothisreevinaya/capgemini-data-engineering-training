from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark Session

spark = SparkSession.builder.appName("DataCleaning").getOrCreate()

# Create DataFrame

data = [
    (1, "Ravi", "Hyderabad", 25),
    (2, None, "Chennai", 32),
    (None, "Arun", "Hyderabad", 28),
    (4, "Meena", None, 30),
    (4, "Meena", None, 30),
    (5, "John", "Bangalore", -5)
]

columns = ["customer_id", "name", "city", "age"]

df = spark.createDataFrame(data, columns)

print("Initial Data:")
df.show()

# Data Cleaning


# Remove null customer_id
df_clean = df.filter(col("customer_id").isNotNull())

# Fill missing values
df_clean = df_clean.fillna("Unknown")

# Remove duplicates
df_clean = df_clean.dropDuplicates()

# Remove invalid age
df_clean = df_clean.filter(col("age") > 0)

print("Cleaned Data:")
df_clean.show()

# Validation

print("Before Cleaning:", df.count())
print("After Cleaning:", df_clean.count())

# Aggregation

print("Customers per City:")
df_clean.groupBy("city").count().show()


## Refection Questions

## What happens if cleaning is skipped?

If we skip data cleaning, the dataset will still contain errors like missing values, duplicates, and incorrect data. Because of this, the results we get from analysis will not be accurate and can be misleading. In real-world scenarios, this can cause serious problems since decisions will be based on wrong information.

## Which issue impacted results most?

The most impactful issues were the missing `customer_id` values and the invalid age values. Since `customer_id` is like a unique identifier, having it missing makes the data unreliable. Also, negative age values don’t make sense and can distort the results during analysis.

## How would this affect business decisions?

If the data is not clean, businesses may take wrong decisions. For example, they might misunderstand customer behavior or make poor marketing strategies. This can lead to losses in revenue and affect overall business performance.

## Can you define a cleaning checklist?

A simple cleaning checklist would be:
• Remove rows with missing important values (like IDs)
• Handle missing data by filling or removing it
• Remove duplicate records
• Filter out invalid or incorrect data
• Finally, check and validate the cleaned data

