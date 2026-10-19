package com.agribigdata.ml

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.{Imputer, VectorAssembler}
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator

object Phase14HyperTuning {
  case class MetricRow(
    trees: Int, depth: Int, weight: Double, threshold: Double,
    acc: Double, balAcc: Double, prec: Double, rec: Double, f1: Double,
    fpr: Double, tp: Double, fp: Double, fn: Double, tn: Double,
    modelIndex: Int
  )

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Phase14HyperTuning")
      .master("local[*]")
      .config("spark.driver.memory", "6g")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    // HDFS read block is commented out for host execution
    // val inputPath = "hdfs://namenode:9000/agri/features/engineered"
    // val fullData = spark.read.parquet(inputPath)
    // val entityCols = Seq("state", "district", "market", "commodity", "variety")
    // val allEntities = fullData.select(entityCols.map(col): _*).distinct()
    // val sampledEntities = allEntities.sample(withReplacement = false, fraction = 0.01, seed = 42L)
    // val data = fullData.join(sampledEntities, entityCols, "inner")
    // val wEntity = Window.partitionBy(entityCols.map(col): _*).orderBy("arrival_date")
    // val dataWithNextZ = data.withColumn("next_z_score", lead(col("z_score"), 1).over(wEntity))
    // val targetDefinition = dataWithNextZ
    //   .withColumn("target_anomaly", when(abs(col("next_z_score")) > 2.0, 1.0).otherwise(0.0))
    //   .filter(col("next_z_score").isNotNull)
      
    // BREAK LINEAGE TO PREVENT OOM
    // targetDefinition.write.mode("overwrite").parquet("hdfs://namenode:9000/tmp/agri_sample_1pct")
    
    val cachedData = spark.read.parquet("file:///Users/kaush/Downloads/Agricultural_BigData_Project/agri_sample_1pct").cache()

    val trainData = cachedData.filter(year(col("arrival_date")) < 2024)
    val valData = cachedData.filter(year(col("arrival_date")) === 2024)
    val testData = cachedData.filter(year(col("arrival_date")) >= 2025)

    val featureCols = Array(
      "previous_modal_price", "price_change", "price_change_pct", 
      "price_range", "relative_price_range", "previous_to_current_ratio", 
      "rolling_7_observation_avg", "rolling_7_observation_stddev", "z_score"
    )
    val imputedCols = featureCols.map(_ + "_imputed")
    val imputer = new Imputer().setInputCols(featureCols).setOutputCols(imputedCols).setStrategy("median")
    val imputerModel = imputer.fit(trainData)
    
    val assembler = new VectorAssembler().setInputCols(imputedCols).setOutputCol("features").setHandleInvalid("skip")
    
    val prepTrain = assembler.transform(imputerModel.transform(trainData)).cache()
    val prepVal = assembler.transform(imputerModel.transform(valData)).cache()

    val extractProb = udf((v: org.apache.spark.ml.linalg.Vector) => v(1))

    val treesArr = Array(50, 100, 200)
    val depthArr = Array(6, 8, 10, 12)
    val weightArr = Array(1.0, 1.5, 2.0, 3.0, 5.0)
    val threshArr = Array(0.50, 0.55, 0.60, 0.65, 0.70, 0.75, 0.80, 0.85, 0.90, 0.95)

    var bestMetric: MetricRow = null
    var bestModelIndex = -1
    var allMetrics = scala.collection.mutable.ArrayBuffer[MetricRow]()

    var currentModelIndex = 0

    println("=== PHASE 1 & 2: MODEL TRAINING & THRESHOLD SWEEP (VALIDATION SET) ===")
    for (t <- treesArr; d <- depthArr; w <- weightArr) {
      println(s"Training Model $currentModelIndex -> Trees: $t, Depth: $d, Weight: $w")
      val tdWeighted = prepTrain.withColumn("classWeight", when(col("target_anomaly") === 1.0, w).otherwise(1.0))
      
      val rf = new RandomForestClassifier()
        .setLabelCol("target_anomaly")
        .setFeaturesCol("features")
        .setNumTrees(t)
        .setMaxDepth(d)
        .setSeed(42L)
        .setWeightCol("classWeight")
        
      val model = rf.fit(tdWeighted)
      
      val predsRaw = model.transform(prepVal).withColumn("prob_1", extractProb(col("probability"))).cache()
      
      for (th <- threshArr) {
        val tPreds = predsRaw.withColumn("pred", when(col("prob_1") >= th, 1.0).otherwise(0.0))
        val tp = tPreds.filter("target_anomaly = 1.0 AND pred = 1.0").count().toDouble
        val tn = tPreds.filter("target_anomaly = 0.0 AND pred = 0.0").count().toDouble
        val fp = tPreds.filter("target_anomaly = 0.0 AND pred = 1.0").count().toDouble
        val fn = tPreds.filter("target_anomaly = 1.0 AND pred = 0.0").count().toDouble

        val acc = (tp + tn) / (tp + tn + fp + fn)
        val prec = if (tp + fp == 0) 0.0 else tp / (tp + fp)
        val rec = if (tp + fn == 0) 0.0 else tp / (tp + fn)
        val f1 = if (prec + rec == 0) 0.0 else 2 * (prec * rec) / (prec + rec)
        val fpr = if (tn + fp == 0) 0.0 else fp / (tn + fp)
        val balAcc = (rec + (if (tn + fp == 0) 0.0 else tn / (tn + fp))) / 2.0

        val row = MetricRow(t, d, w, th, acc, balAcc, prec, rec, f1, fpr, tp, fp, fn, tn, currentModelIndex)
        allMetrics += row
      }
      predsRaw.unpersist()
      System.gc()
      currentModelIndex += 1
    }

    println("=== SELECTION RULE (Validation Acc subject to Rec>=0.25, Prec>=0.20) ===")
    val validCands = allMetrics.filter(r => r.rec >= 0.25 && r.prec >= 0.20)
    if (validCands.isEmpty) {
      println("NO CANDIDATES MET THE MINIMUM CONSTRAINTS!")
      bestMetric = allMetrics.maxBy(r => (r.acc, r.f1, r.rec))
    } else {
      bestMetric = validCands.maxBy(r => (r.acc, r.f1, r.rec))
    }

    println(s"SELECTED CANDIDATE -> Trees: ${bestMetric.trees}, Depth: ${bestMetric.depth}, Weight: ${bestMetric.weight}, Threshold: ${bestMetric.threshold}")
    println(f"Validation Performance -> Acc: ${bestMetric.acc}%.4f, Prec: ${bestMetric.prec}%.4f, Rec: ${bestMetric.rec}%.4f, F1: ${bestMetric.f1}%.4f")

    println("\n=== PHASE 3: FINAL TEST (2025+ SET) ===")
    
    // Retrain the best model
    val bestTdWeighted = trainData.withColumn("classWeight", when(col("target_anomaly") === 1.0, bestMetric.weight).otherwise(1.0))
    val bestRf = new RandomForestClassifier()
      .setLabelCol("target_anomaly")
      .setFeaturesCol("features")
      .setNumTrees(bestMetric.trees)
      .setMaxDepth(bestMetric.depth)
      .setSeed(42L)
      .setWeightCol("classWeight")
    val bestPipeline = new Pipeline().setStages(Array(imputer, assembler, bestRf))
    val finalModel = bestPipeline.fit(bestTdWeighted)

    val testPredsRaw = finalModel.transform(testData).withColumn("prob_1", extractProb(col("probability"))).cache()
    val testPreds = testPredsRaw.withColumn("pred", when(col("prob_1") >= bestMetric.threshold, 1.0).otherwise(0.0))
    
    val tp = testPreds.filter("target_anomaly = 1.0 AND pred = 1.0").count().toDouble
    val tn = testPreds.filter("target_anomaly = 0.0 AND pred = 0.0").count().toDouble
    val fp = testPreds.filter("target_anomaly = 0.0 AND pred = 1.0").count().toDouble
    val fn = testPreds.filter("target_anomaly = 1.0 AND pred = 0.0").count().toDouble

    val acc = (tp + tn) / (tp + tn + fp + fn)
    val prec = if (tp + fp == 0) 0.0 else tp / (tp + fp)
    val rec = if (tp + fn == 0) 0.0 else tp / (tp + fn)
    val f1 = if (prec + rec == 0) 0.0 else 2 * (prec * rec) / (prec + rec)
    val fpr = if (tn + fp == 0) 0.0 else fp / (tn + fp)
    val balAcc = (rec + (if (tn + fp == 0) 0.0 else tn / (tn + fp))) / 2.0

    val prAuc = new BinaryClassificationEvaluator().setLabelCol("target_anomaly").setRawPredictionCol("rawPrediction").setMetricName("areaUnderPR").evaluate(testPredsRaw)
    val rocAuc = new BinaryClassificationEvaluator().setLabelCol("target_anomaly").setRawPredictionCol("rawPrediction").setMetricName("areaUnderROC").evaluate(testPredsRaw)

    println(f"Accuracy:           ${acc * 100}%.2f%%")
    println(f"Balanced Accuracy:  ${balAcc * 100}%.2f%%")
    println(f"Precision:          ${prec * 100}%.2f%%")
    println(f"Recall:             ${rec * 100}%.2f%%")
    println(f"F1:                 ${f1 * 100}%.2f%%")
    println(f"PR-AUC:             ${prAuc * 100}%.2f%%")
    println(f"ROC-AUC:            ${rocAuc * 100}%.2f%%")
    println(f"FPR:                ${fpr * 100}%.2f%%")
    println("Confusion Matrix:")
    println(f"TN = ${tn.toLong}")
    println(f"FP = ${fp.toLong}")
    println(f"FN = ${fn.toLong}")
    println(f"TP = ${tp.toLong}")

    spark.stop()
  }
}
