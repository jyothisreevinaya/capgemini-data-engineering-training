# 1. EXTRACT
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("Phase4_ETL_Pipeline").getOrCreate()

# Source Data
customers_data = [
    (1, "Ravi", "Hyderabad", 25),
    (2, "Sita", "Chennai", 32),
    (3, "Arun", "Hyderabad", 28),
    (4, "Meena", "Bengaluru", 35),
    (5, "Kiran", "Chennai", 22)
]

orders_data = [
    (101, 1, 2500, "2026-03-01"),
    (102, 2, 1800, "2026-03-02"),
    (103, 1, 3200, "2026-03-03"),
    (104, 3, 1500, "2026-03-04"),
    (105, 5, 2800, "2026-03-05")
]

# Create DataFrames
customers = spark.createDataFrame(customers_data,
    ["customer_id","customer_name","city","age"])

orders = spark.createDataFrame(orders_data,
    ["order_id","customer_id","amount","order_date"])

# 2. TRANSFORM

# Data Cleaning 
customers_clean = customers.dropna(subset=["customer_id"]) \
                           .filter(col("age") >= 0)

orders_clean = orders.dropna(subset=["customer_id","order_id"]) \
                     .filter(col("amount") > 0)

#  Daily Sales 
daily_sales = orders_clean.groupBy("order_date") \
    .agg(sum("amount").alias("total_sales"))

#  City-wise Revenue 
city_revenue = customers_clean.join(orders_clean, "customer_id") \
    .groupBy("city") \
    .agg(sum("amount").alias("total_revenue"))

#  Top Customers
top_customers = customers_clean.join(orders_clean, "customer_id") \
    .groupBy("customer_name") \
    .agg(sum("amount").alias("total_spend")) \
    .orderBy(desc("total_spend")) \
    .limit(5)

#  Repeat Customers (>1 order) 
repeat_customers = orders_clean.groupBy("customer_id") \
    .agg(count("order_id").alias("order_count")) \
    .filter(col("order_count") > 1)

# Customer Segmentation (Separate Step) 
customer_spend = customers_clean.join(orders_clean, "customer_id") \
    .groupBy("customer_name") \
    .agg(sum("amount").alias("total_spend"))

segmentation = customer_spend.withColumn(
    "segment",
    when(col("total_spend") > 10000, "Gold")
    .when((col("total_spend") >= 5000) & (col("total_spend") <= 10000), "Silver")
    .otherwise("Bronze")
)

#  Final Reporting Table 
final_df = customers_clean.join(orders_clean, "customer_id") \
    .groupBy("customer_id","customer_name","city") \
    .agg(
        sum("amount").alias("total_spend"),
        count("order_id").alias("order_count")
    )

# Add segmentation to final table
final_df = final_df.withColumn(
    "segment",
    when(col("total_spend") > 10000, "Gold")
    .when((col("total_spend") >= 5000) & (col("total_spend") <= 10000), "Silver")
    .otherwise("Bronze")
)

# 3. LOAD

# Save Final Output (use safe local path)
final_df.write.mode('overwrite').option("header", True).csv("report_output")

# SHOW RESULTS

print("Daily Sales")
daily_sales.show()

print("City Revenue")
city_revenue.show()

print("Top Customers")
top_customers.show()

print("Repeat Customers")
repeat_customers.show()

print("Customer Segmentation")
segmentation.show()

print("Final Report")
final_df.show()
