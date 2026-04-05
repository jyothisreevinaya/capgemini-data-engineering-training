# 1. Import Libraries
from pyspark.sql.functions import col, sum, to_date, when, rank, dense_rank
from pyspark.sql.window import Window

# 2. Load Data

customers = spark.read.csv("/Volumes/workspace/default/olist_data/olist_customers_dataset.csv", header=True, inferSchema=True)

orders = spark.read.csv("/Volumes/workspace/default/olist_data/olist_orders_dataset.csv", header=True, inferSchema=True)

order_items = spark.read.csv("/Volumes/workspace/default/olist_data/olist_order_items_dataset.csv", header=True, inferSchema=True)

products = spark.read.csv("/Volumes/workspace/default/olist_data/olist_products_dataset.csv", header=True, inferSchema=True)


# 3. Data Validation

customers.dropDuplicates()
orders.dropDuplicates()
order_items.dropDuplicates()

# 4. Join Tables (Create df)

df = order_items.join(orders, "order_id") \
                .join(customers, "customer_id")

# 5. Fix Data Types

df = df.withColumn("price", col("price").cast("double"))

# TASK 1: Top 3 Customers per City

spend = df.groupBy("customer_id", "customer_city") \
          .agg(sum("price").alias("total_spend"))

window1 = Window.partitionBy("customer_city").orderBy(col("total_spend").desc())

top_customers = spend.withColumn("rank", rank().over(window1)) \
                     .filter(col("rank") <= 3)

print("=== Top Customers per City ===")
top_customers.show()

# TASK 2: Running Total of Sales

daily = df.withColumn("date", to_date("order_purchase_timestamp")) \
          .groupBy("date") \
          .agg(sum("price").alias("daily_sales"))

window2 = Window.orderBy("date")

running_total = daily.withColumn("running_total", sum("daily_sales").over(window2))

print("=== Running Total ===")
running_total.show()

# TASK 3: Top Products

prod_sales = df.groupBy("product_id") \
               .agg(sum("price").alias("total_sales"))

window3 = Window.orderBy(col("total_sales").desc())

top_products = prod_sales.withColumn("rank", dense_rank().over(window3))

print("=== Top Products ===")
top_products.show()

# TASK 4: Customer Lifetime Value

clv = df.groupBy("customer_id") \
        .agg(sum("price").alias("total_spend"))

print("=== Customer Lifetime Value ===")
clv.show()

# TASK 5: Customer Segmentation

seg = clv.withColumn("segment",
    when(col("total_spend") > 10000, "Gold")
    .when(col("total_spend") >= 5000, "Silver")
    .otherwise("Bronze")
)

print("=== Customer Segmentation ===")
seg.groupBy("segment").count().show()

# TASK 6: Final Reporting Table

orders_count = df.groupBy("customer_id") \
                 .count() \
                 .withColumnRenamed("count", "total_orders")

final = seg.join(customers, "customer_id") \
           .join(orders_count, "customer_id") \
           .select("customer_id", "customer_city", "total_spend", "segment", "total_orders")

print("=== Final Report ===")
final.show()
