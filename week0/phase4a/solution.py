from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, count, when, percent_rank
from pyspark.sql.window import Window
from pyspark.ml.feature import Bucketizer

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

customers = spark.createDataFrame(customers_data, ["customer_id", "name", "city", "age"])
orders = spark.createDataFrame(orders_data, ["order_id", "customer_id", "amount", "date"])

# STEP 2: TRANSFORM (JOIN)

df = customers.join(orders, "customer_id")

# STEP 3: AGGREGATION (TOTAL SPEND PER CUSTOMER)

df_total = df.groupBy("customer_id", "name", "city") \
             .agg(sum("amount").alias("total_spend"))

# STEP 4: SEGMENTATION USING CONDITIONAL LOGIC

df_seg = df_total.withColumn(
    "segment",
    when(df_total.total_spend > 5000, "Gold")
    .when((df_total.total_spend >= 2000) & (df_total.total_spend <= 5000), "Silver")
    .otherwise("Bronze")
)

# STEP 5: GROUP BY SEGMENT

df_seg.groupBy("segment").count().show()

# STEP 6: SQL CASE STATEMENT

df_seg.createOrReplaceTempView("customers_seg")

spark.sql("""
SELECT *,
CASE 
    WHEN total_spend > 5000 THEN 'Gold'
    WHEN total_spend BETWEEN 2000 AND 5000 THEN 'Silver'
    ELSE 'Bronze'
END AS segment_sql
FROM customers_seg
""").show()

# STEP 7: BUCKETIZER

splits = [-float("inf"), 2000, 5000, float("inf")]

bucketizer = Bucketizer(
    splits=splits,
    inputCol="total_spend",
    outputCol="bucket"
)

df_bucket = bucketizer.transform(df_seg)
df_bucket.show()

# STEP 8: QUANTILE-BASED SEGMENTATION

quantiles = df_total.approxQuantile("total_spend", [0.33, 0.66], 0)

q1 = quantiles[0]
q2 = quantiles[1]

df_quantile = df_total.withColumn(
    "segment_quantile",
    when(df_total.total_spend <= q1, "Bronze")
    .when((df_total.total_spend > q1) & (df_total.total_spend <= q2), "Silver")
    .otherwise("Gold")
)

df_quantile.show()

# STEP 9: WINDOW FUNCTION (PERCENT RANK)

window = Window.orderBy("total_spend")

df_rank = df_total.withColumn("rank_pct", percent_rank().over(window))

# STEP 10: SEGMENT USING RANK

df_rank = df_rank.withColumn(
    "segment_rank",
    when(df_rank.rank_pct > 0.66, "Gold")
    .when(df_rank.rank_pct > 0.33, "Silver")
    .otherwise("Bronze")
)

df_rank.show()

# STEP 11: FINAL OUTPUT

df_seg.show()
