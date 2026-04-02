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
