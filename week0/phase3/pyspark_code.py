from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("practice").getOrCreate()
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
customers = spark.createDataFrame(customers_data, ["customer_id","customer_name","city","age"])
orders = spark.createDataFrame(orders_data, ["order_id","customer_id","amount","order_date"])

# PySpark 1 (Read sales → clean nulls → calculate daily sales)
orders.filter("amount is not null") \
.groupBy("order_date") \
.sum("amount") \
.show()

#PySpark 2(Read customers → clean invalid rows → city-wise revenue)Final Reporting Table
customers_clean = customers.filter("customer_id is not null and age >= 0")
customers_clean.join(orders, "customer_id") \
.groupBy("city") \
.sum("amount") \
.show()

# PySpark 3(Find repeat customers (>2 orders)
orders.groupBy("customer_id") \
.count() \
.filter("count > 2") \
.show()

#PySpark 4 (Highest spending customer in each city)
from pyspark.sql.functions import sum, rank
from pyspark.sql.window import Window
total_df = customers.join(orders, "customer_id") \
.groupBy("city", "customer_id") \
.agg(sum("amount").alias("total_spend"))
window = Window.partitionBy("city").orderBy(total_df["total_spend"].desc())
total_df.withColumn("rank", rank().over(window)) \
.filter("rank = 1") \
.show()

#PySpark 5 (Final Reporting Table)
from pyspark.sql.functions import sum, count
customers.join(orders, "customer_id") \
.groupBy("customer_id", "customer_name", "city") \
.agg(
    sum("amount").alias("total_spend"),
    count("order_id").alias("order_count")
) \
.show()
