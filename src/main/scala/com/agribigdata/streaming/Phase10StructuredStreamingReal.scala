package com.agribigdata.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode, Trigger}
import org.apache.spark.ml.linalg.Vector

case class MarketEntity(state: String, district: String, market: String, commodity: String, variety: String)

case class MarketEvent(
  state: String, district: String, market: String, commodity: String, variety: String,
  arrival_date: String, min_price: Double, max_price: Double, modal_price: Double,
  event_timestamp: java.sql.Timestamp
)

case class MarketState(
  history: Seq[Double]
)

case class MarketFeatureRow(
  state: String, district: String, market: String, commodity: String, variety: String,
  arrival_date: String, modal_price: Double, event_timestamp: java.sql.Timestamp,
  previous_modal_price: Double,
  price_change: Double,
  price_change_pct: Double,
  price_range: Double,
  relative_price_range: Double,
  previous_to_current_ratio: Double,
  rolling_7_observation_avg: Double,
  rolling_7_observation_stddev: Double,
  z_score: Double
)

object Phase10StructuredStreamingReal {

  def updateState(
    key: MarketEntity,
    values: Iterator[MarketEvent],
    state: GroupState[MarketState]
  ): Iterator[MarketFeatureRow] = {
    var currentState = if (state.exists) state.get else MarketState(Seq.empty)
    
    val sortedValues = values.toSeq.sortBy(e => (e.arrival_date, e.event_timestamp.getTime))

    sortedValues.map { event =>
      val hist = currentState.history
      
      val previous_modal_price = if (hist.nonEmpty) hist.last else Double.NaN
      val price_change = if (hist.nonEmpty) event.modal_price - previous_modal_price else Double.NaN
      val price_change_pct = if (hist.nonEmpty && previous_modal_price != 0) price_change / previous_modal_price else Double.NaN
      val price_range = event.max_price - event.min_price
      val relative_price_range = if (event.modal_price != 0) price_range / event.modal_price else Double.NaN
      val previous_to_current_ratio = if (event.modal_price != 0 && hist.nonEmpty) previous_modal_price / event.modal_price else Double.NaN
      
      val rolling_7_observation_avg = if (hist.nonEmpty) hist.sum / hist.length else Double.NaN
      val rolling_7_observation_stddev = if (hist.length > 1) {
        val avg = rolling_7_observation_avg
        math.sqrt(hist.map(x => math.pow(x - avg, 2)).sum / (hist.length - 1))
      } else Double.NaN
      
      val z_score = if (!rolling_7_observation_stddev.isNaN && rolling_7_observation_stddev != 0.0 && !rolling_7_observation_avg.isNaN) {
        (event.modal_price - rolling_7_observation_avg) / rolling_7_observation_stddev
      } else Double.NaN

      val newHist = (hist :+ event.modal_price).takeRight(7)
      currentState = MarketState(newHist)
      state.update(currentState)

      MarketFeatureRow(
        event.state, event.district, event.market, event.commodity, event.variety,
        event.arrival_date, event.modal_price, event.event_timestamp,
        previous_modal_price, price_change, price_change_pct, price_range, relative_price_range,
        previous_to_current_ratio, rolling_7_observation_avg, rolling_7_observation_stddev, z_score
      )
    }.iterator
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Phase10StructuredStreamingReal")
      .master("local[*]")
      .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    val modelPath = "hdfs://namenode:9000/agri/models/anomaly_prediction_rf"
    println(s"Loading frozen ML model from $modelPath ...")
    val model = PipelineModel.load(modelPath)

    val kafkaBootstrap = "host.docker.internal:29092"
    val topic = "agri-market-real-replay"

    val schema = StructType(Seq(
      StructField("state", StringType, true),
      StructField("district", StringType, true),
      StructField("market", StringType, true),
      StructField("commodity", StringType, true),
      StructField("variety", StringType, true),
      StructField("arrival_date", StringType, true),
      StructField("min_price", DoubleType, true),
      StructField("max_price", DoubleType, true),
      StructField("modal_price", DoubleType, true)
    ))

    println(s"Subscribing to Kafka topic: $topic")
    val rawStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrap)
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
      .load()

    val parsedStream = rawStream
      .select(
        from_json(col("value").cast("string"), schema).as("data"),
        col("timestamp").as("event_timestamp")
      )
      .select("data.*", "event_timestamp")
      .as[MarketEvent]

    val statefulStream = parsedStream
      .groupByKey(e => MarketEntity(e.state, e.district, e.market, e.commodity, e.variety))
      .flatMapGroupsWithState(OutputMode.Append, GroupStateTimeout.NoTimeout)(updateState)
      .toDF()
      // ML Pipeline will use handleInvalid="skip" (set during training), 
      // but to be safe, drop NaNs from the feature columns so vector assembler doesn't fail.
      .na.fill(0.0) // Fill NaNs with 0.0 just so inference succeeds without skipping the row if Imputer doesn't handle it
    
    // apply the ML model
    val getProb = udf((v: Vector) => v.toArray(1))
    
    val predictions = model.transform(statefulStream)
      .withColumn("anomaly_probability", getProb(col("probability")))
      .withColumn("anomaly_prediction", col("prediction"))
      .select(
        col("event_timestamp").as("timestamp"),
        col("state"), col("district"), col("market"), col("commodity"), col("variety"),
        col("arrival_date"), col("modal_price"),
        col("anomaly_probability"), col("anomaly_prediction")
      )

    val checkpointPath = "hdfs://namenode:9000/agri/checkpoints/real_anomaly_stream"
    val outputPath = "hdfs://namenode:9000/agri/streaming/real_anomaly_predictions"

    val query = predictions.writeStream
      .format("parquet")
      .option("path", outputPath)
      .option("checkpointLocation", checkpointPath)
      .trigger(Trigger.ProcessingTime("2 seconds"))
      .start()

    query.awaitTermination()
  }
}
