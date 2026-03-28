from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Spark Playground').getOrCreate()
customers = spark.createDataFrame([
 (1, "Ravi", "Hyderabad", 25),
 (2, "Sita", "Chennai", 32),
 (3, "Arun", "Hyderabad", 28)
], ["customer_id", "customer_name", "city", "age"])
customers.show()
customers.filter(customers.city == "Chennai").show()
customers.filter(customers.age > 25).show()
customers.select("customer_name", "city").show()
customers.groupBy("city").count().show()
