from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, when

# Create Spark Session
spark = SparkSession.builder.appName("phase4a").getOrCreate()

# Data
customers = spark.createDataFrame([
 (1, "Ravi", "Hyderabad", 25),
 (2, "Sita", "Chennai", 32),
 (3, "Arun", "Hyderabad", 28),
 (4, "Meena", "Bengaluru", 35),
 (5, "Kiran", "Chennai", 22)
], ["customer_id", "name", "city", "age"])

orders = spark.createDataFrame([
 (101, 1, 2500, "2026-03-01"),
 (102, 2, 1800, "2026-03-02"),
 (103, 1, 3200, "2026-03-03"),
 (104, 3, 1500, "2026-03-04"),
 (105, 5, 2800, "2026-03-05")
], ["order_id", "customer_id", "amount", "date"])

# Join + total spend
df = customers.join(orders, "customer_id")

df_total = df.groupBy("customer_id", "name") \
             .agg(sum("amount").alias("total_spend"))

# 1. Conditional Segmentation
df_seg = df_total.withColumn(
 "segment",
 when(df_total.total_spend > 5000, "Gold")
 .when((df_total.total_spend >= 2000) & (df_total.total_spend <= 5000), "Silver")
 .otherwise("Bronze")
)

# 2. Group by segment
print("Segment Count:")
df_seg.groupBy("segment").count().show()

# 3. Quantile Segmentation
quantiles = df_total.approxQuantile("total_spend", [0.33, 0.66], 0)

q1 = quantiles[0]
q2 = quantiles[1]

df_quantile = df_total.withColumn(
 "segment_quantile",
 when(df_total.total_spend <= q1, "Bronze")
 .when((df_total.total_spend > q1) & (df_total.total_spend <= q2), "Silver")
 .otherwise("Gold")
)

# 4. Compare both methods
print("Comparison:")
df_compare = df_seg.join(df_quantile, ["customer_id", "name", "total_spend"])
df_compare.select("name", "total_spend", "segment", "segment_quantile").show()

# Show final outputs
df_seg.show()
df_quantile.show()
