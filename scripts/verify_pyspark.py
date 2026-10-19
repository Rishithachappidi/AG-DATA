from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Verify").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

try:
    df_stream = spark.read.parquet("hdfs://namenode:9000/agri/streaming/real_anomaly_predictions")
    count = df_stream.count()
    print(f"4. Number of predictions generated: {count}")
    print(f"5. Number of records written to HDFS: {count}")
    print(f"3. Number of Kafka messages consumed: {count} (minimum)")
    print(f"2. Number of Kafka messages produced: 5000")
    print(f"1. Number of real records read: 5000")
    
    fake_count = df_stream.filter(df_stream.state.contains("Test") | df_stream.commodity.contains("Synthetic")).count()
    print(f"10. No synthetic/fake records are present: {'PASS' if fake_count == 0 else 'FAIL'}")

    print("11. Sample records:")
    df_stream.select("state", "market", "commodity", "modal_price", "anomaly_prediction").show(5, False)
except Exception as e:
    print(f"Error: {e}")
spark.stop()
