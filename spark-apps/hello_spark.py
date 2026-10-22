from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("EDAI-Test").getOrCreate()

data = [
    ("orders", 120),
    ("customers", 50),
    ("products", 80),
]

df = spark.createDataFrame(data, ["table_name", "record_count"])
df.show()

spark.stop()
