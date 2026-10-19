from pyspark.sql import SparkSession
from kafka import KafkaProducer
import json
import time

def main():
    spark = SparkSession.builder \
        .appName("RealDataReplay") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print("Reading canonical data from HDFS...")
    df = spark.read.parquet("hdfs://namenode:9000/agri/processed/canonical")
    
    # Filter 2025+ and take 5000 records
    pdf = df.filter("year >= 2025").orderBy("arrival_date").limit(5000).toPandas()

    print(f"Fetched {len(pdf)} records. Sending to Kafka topic: agri-market-real-replay")

    # Since we run inside docker_default, kafka is at host.docker.internal:29092
    producer = KafkaProducer(
        bootstrap_servers=['host.docker.internal:29092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    count = 0
    for _, row in pdf.iterrows():
        msg = {
            "state": str(row['state']),
            "district": str(row['district']),
            "market": str(row['market']),
            "commodity": str(row['commodity']),
            "variety": str(row['variety']),
            "arrival_date": str(row['arrival_date']),
            "min_price": float(row['min_price']),
            "max_price": float(row['max_price']),
            "modal_price": float(row['modal_price'])
        }
        producer.send('agri-market-real-replay', msg)
        count += 1
        if count % 100 == 0:
            print(f"Sent {count} records...")
            producer.flush()
        
        # small delay to simulate streaming
        time.sleep(0.01)

    producer.flush()
    producer.close()
    print("Replay complete!")
    spark.stop()

if __name__ == "__main__":
    main()
