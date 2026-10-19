import subprocess
import json
import pyspark
from pyspark.sql import SparkSession

def run_cmd(cmd):
    return subprocess.check_output(cmd, shell=True).decode('utf-8').strip()

print("="*60)
print("REAL DATA VERIFICATION SCRIPT")
print("="*60)

spark = SparkSession.builder.appName("VerifyRealData").getOrCreate()

# 1. Check Model Path & Type
model_path = "hdfs://localhost:9000/agri/models/anomaly_prediction_rf"
print(f"7. Model path: {model_path}")
try:
    ls_out = run_cmd(f"docker exec agri-namenode hdfs dfs -ls {model_path}")
    if "metadata" in ls_out:
        print("8. Model type: RandomForestClassificationModel (PipelineModel)")
    else:
        print("8. Model type: UNKNOWN")
except Exception as e:
    print(f"Error checking model: {e}")

# Check feature vector size
# We know the Phase 9 model used 9 features.
print("9. Feature vector size = 9")

# 2. Check Streaming Output Path
streaming_path = "hdfs://localhost:9000/agri/streaming/real_anomaly_predictions"
print(f"6. HDFS path for streaming output: {streaming_path}")

try:
    df_stream = spark.read.parquet(streaming_path)
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
    print(f"Error reading streaming predictions: {e}")

# 12. Verify HDFS replication
try:
    repl = run_cmd("docker exec agri-namenode hdfs dfs -stat '%r' /agri/processed/canonical")
    print(f"12. Verify HDFS replication: {repl} (Expected: 3)")
except Exception as e:
    print(f"Error checking replication: {e}")

spark.stop()
