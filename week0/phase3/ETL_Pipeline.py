from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, count, rank
from pyspark.sql.window import Window

# STEP 1: EXTRACT

spark = SparkSession.builder.appName("final_etl_pipeline").getOrCreate()

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

customers = spark.createDataFrame(customers_data, 
    ["customer_id","customer_name","city","age"])

orders = spark.createDataFrame(orders_data, 
    ["order_id","customer_id","amount","order_date"])

# STEP 2: TRANSFORM (Cleaning)

# Clean customers
customers_clean = customers.filter("customer_id is not null and age >= 0")

# Clean orders
orders_clean = orders.filter("amount is not null and amount > 0")

# STEP 3: TRANSFORM (Business Logic)

# 1. Daily Sales
daily_sales = orders_clean.groupBy("order_date") \
                         .sum("amount")

# 2. City-wise Revenue
city_revenue = customers_clean.join(orders_clean, "customer_id") \
                              .groupBy("city") \
                              .sum("amount")

# 3. Repeat Customers (>2 orders)
repeat_customers = orders_clean.groupBy("customer_id") \
                               .count() \
                               .filter("count > 2")

# 4. Highest Spending Customer per City
total_spend_df = customers_clean.join(orders_clean, "customer_id") \
                                .groupBy("city", "customer_id") \
                                .agg(sum("amount").alias("total_spend"))

window = Window.partitionBy("city").orderBy(total_spend_df["total_spend"].desc())

top_customers = total_spend_df.withColumn("rank", rank().over(window)) \
                             .filter("rank = 1")

# 5. Final Reporting Table
final_report = customers_clean.join(orders_clean, "customer_id") \
                              .groupBy("customer_id", "customer_name", "city") \
                              .agg(
                                  sum("amount").alias("total_spend"),
                                  count("order_id").alias("order_count")
                              )

# STEP 4: LOAD

print("Daily Sales:")
daily_sales.show()

print("City-wise Revenue:")
city_revenue.show()

print("Repeat Customers:")
repeat_customers.show()

print("Top Customer per City:")
top_customers.show()

print("Final Report:")
final_report.show()
