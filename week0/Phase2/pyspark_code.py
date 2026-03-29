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

PySpark 1
customers.join(orders, "customer_id") \
    .groupBy("customer_id", "customer_name") \
    .sum("amount") \
    .withColumnRenamed("sum(amount)", "total_amount") \
    .show()
PySpark 2
customers.join(orders, "customer_id") \
    .groupBy("customer_id", "customer_name") \
    .sum("amount") \
    .withColumnRenamed("sum(amount)", "total_amount") \
    .orderBy("total_amount", ascending=False) \
    .limit(3) \
    .show()
PySpark 3
customers.join(orders, "customer_id", "left") \
    .filter(orders.customer_id.isNull()) \
    .select("customer_id", "customer_name") \
    .show()
PySpark 4
customers.join(orders, "customer_id") \
    .groupBy("city") \
    .sum("amount") \
    .withColumnRenamed("sum(amount)", "total_revenue") \
    .show()
PySpark 5
customers.join(orders, "customer_id") \
    .groupBy("customer_id", "customer_name") \
    .avg("amount") \
    .withColumnRenamed("avg(amount)", "avg_amount") \
    .show()
PySpark 6
orders.groupBy("customer_id") \
    .count() \
    .filter("count > 1") \
    .show()
PySpark 7
customers.join(orders, "customer_id") \
    .groupBy("customer_id", "customer_name") \
    .sum("amount") \
    .withColumnRenamed("sum(amount)", "total_amount") \
    .orderBy("total_amount", ascending=False) \
    .show()
