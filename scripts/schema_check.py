from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("CheckSchemas").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

paths = [
    "/agri-market/analytics/commodity_statistics",
    "/agri-market/analytics/state_statistics",
    "/agri-market/analytics/yearly_statistics"
]

for p in paths:
    print(f"--- Schema for {p} ---")
    try:
        df = spark.read.parquet(f"hdfs://namenode:9000{p}")
        df.printSchema()
        df.show(5, False)
    except Exception as e:
        print(f"Failed to read: {e}")

spark.stop()
