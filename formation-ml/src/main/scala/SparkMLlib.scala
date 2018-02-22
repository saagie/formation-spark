import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification._
import org.apache.spark.ml.evaluation.{MulticlassClassificationEvaluator, RegressionEvaluator}
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorAssembler, VectorIndexer}
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{Encoders, SaveMode, SparkSession}

case class Request(uuid: String, origin: String, ip: String, browser: String, timestamp: Long, latitude: Double, longitude: Double, price: Double)

object SparkMLlib extends App {

  val spark = SparkSession.builder()
    .appName("Spark ML")
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._

  val opera = ".*Opera.*".r
  val safari = ".*Safari.*".r
  val chrome = ".*Chrome.*".r
  val firefox = ".*Firefox.*".r

  val toNav = udf((ua: String) => {
    ua match {
      case opera() => "Opera"
      case chrome() => "Chrome"
      case firefox() => "Firefox"
      case _ => ua
    }
  })

  /*  val clean = spark
      .read
      .option("header", true)
      .csv("dataset.csv")
      .select($"uuid", $"url", $"ip", toNav($"user_agent") as "browser", $"timestamp", $"latitude", $"longitude", $"price")

    clean.
      write
      .option("header", true)
      .mode(SaveMode.Overwrite)
      .csv("dataset_clean.csv")

    clean.show()*/

  val schema = Encoders.product[Request].schema

  val ds = spark
    .read
    .option("header", true)
    .schema(schema)
    .csv("dataset_clean.csv")
    .na.drop()

  val encoder = new StringIndexer()
    .setInputCol("browser")
    .setOutputCol("browserFeature")

  val vectorAssembler = new VectorAssembler()
    .setInputCols(Array("latitude", "price"))
    .setOutputCol("features")

  val dsFeatures = vectorAssembler.transform(encoder.fit(ds).transform(ds))

  val labelIndexer = new StringIndexer()
    .setInputCol("browser")
    .setOutputCol("label")
    .fit(dsFeatures)

  val featureIndexer = new VectorIndexer()
    .setInputCol("features")
    .setOutputCol("indexed")
    .setMaxCategories(3)

  val Array(train, test) = ds.randomSplit(Array(0.5, 0.5))

  val rfc = new RandomForestClassifier()
    .setLabelCol("label")
    .setFeaturesCol("indexed")

  val lgc = new LogisticRegression()
    .setLabelCol("label")
    .setFeaturesCol("indexed")

  val labelConverter = new IndexToString()
    .setInputCol("prediction")
    .setOutputCol("predictionLabel")
    .setLabels(labelIndexer.labels)

  val pipeline = new Pipeline()
    .setStages(Array(encoder, vectorAssembler, labelIndexer, featureIndexer, lgc, labelConverter))

  //Single model
  //  val pipelineModel = pipeline.fit(train)
  //  pipelineModel.write.overwrite().save("pipeline.model")
  //  val classification = pipelineModel.transform(test)
  //  classification.show()

  val evaluator = new MulticlassClassificationEvaluator()
    .setLabelCol("label")
    .setPredictionCol("prediction")
    .setMetricName("accuracy")

  val paramGrid = new ParamGridBuilder()
    .build()

  val trainValidationSplit = new TrainValidationSplit()
    .setEstimator(pipeline)
    .setEvaluator(evaluator)
    .setEstimatorParamMaps(paramGrid)
    .setTrainRatio(0.7)

  val model = trainValidationSplit.fit(train)
  model.write.overwrite().save("./model")
  val predictions = model.transform(test)
  val accuracy = evaluator.evaluate(predictions)
  println(s"Test error: ${1.0 - accuracy}")
  println(s"Accuracy: $accuracy")
  predictions.show()

  //Regression evaluator
  //  val regEval = new RegressionEvaluator()
  //
  //  regEval
  //    .setLabelCol("label")
  //    .setPredictionCol("prediction")
  //    .setMetricName("rmse")
}
