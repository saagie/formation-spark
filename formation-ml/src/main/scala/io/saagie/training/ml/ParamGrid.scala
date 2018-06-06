package io.saagie.training.ml

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorAssembler, VectorIndexer}
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{Encoders, SaveMode, SparkSession}

object ParamGrid extends App {
  val spark = SparkSession.builder()
    .appName("Spark ML")
    .master("local[*]")
    .config("spark.ui.showConsoleProgress", "false")
    .config("spark.executor.extraJavaOptions", "-Dlog4j.configuration=log4j.xml")
    .getOrCreate()

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

  //Loading the dataset.
  val dirty = spark
    .read
    .option("header", true)
    .csv("dataset.csv")

  //Transforming our user agent column.
  val clean = dirty
    .select($"uuid", $"url", $"ip", toNav($"user_agent") as "browser", $"timestamp", $"latitude", $"longitude", $"price")

  //Writing a clean dataset.
  clean.
    write
    .option("header", true)
    .mode(SaveMode.Overwrite)
    .csv("dataset_clean.csv")

  //Definition of a schema for our CSV file
  val schema = Encoders.product[Request].schema

  //Read of the cleaned file and drop of empty columns
  val ds = spark
    .read
    .option("header", true)
    .schema(schema)
    .csv("dataset_clean.csv")
    .na
    .drop()

  //Encoder to transform browser
  val encoder = new StringIndexer()
    .setInputCol("browser")
    .setOutputCol("browserFeature")

  //Encoded browser string
  val dfEncoded = encoder
    .fit(ds)
    .transform(ds)

  //Vector assembler to prepare features
  val vectorAssembler = new VectorAssembler()
    .setInputCols(Array("timestamp", "longitude", "latitude", "price"))
    .setOutputCol("features")

  //Applying assembler to our features
  val dsFeatures = vectorAssembler.transform(dfEncoded)

  //Indexer of our browser into a label column
  val labelIndexer = new StringIndexer()
    .setInputCol("browser")
    .setOutputCol("label")
    .fit(dsFeatures)

  //Indexing all our features
  val featureIndexer = new VectorIndexer()
    .setInputCol("features")
    .setOutputCol("indexed")
    .setMaxCategories(3)

  //Split of our dataset
  val Array(train, test) = ds.randomSplit(Array(0.7, 0.3))

  //Creation of our classifier
  val rfc = new RandomForestClassifier()
    .setLabelCol("label")
    .setFeaturesCol("indexed")

  //Converting labels back into String
  val labelConverter = new IndexToString()
    .setInputCol("prediction")
    .setOutputCol("predictionLabel")
    .setLabels(labelIndexer.labels)

  val pipelineRFC = new Pipeline()
    .setStages(Array(encoder, vectorAssembler, labelIndexer, featureIndexer, rfc, labelConverter))

  //Training our model on data
  val modelRFC = pipelineRFC.fit(train)

  //Definition of our evaluator
  val evaluator = new MulticlassClassificationEvaluator()
    .setLabelCol("label")
    .setPredictionCol("prediction")
    .setMetricName("accuracy")

  //Applying model to our test dataset
  val predictions = modelRFC.transform(test)

  //Computing accuracy
  val accuracy = evaluator.evaluate(predictions)

  println(s"Test error: ${1.0 - accuracy}")
  println(s"Accuracy: $accuracy\n")

  //Defining a param grid for chosen algorithm
  val paramGrid = new ParamGridBuilder()
    .addGrid(rfc.getParam("maxDepth"), Array(5, 10, 15, 20, 25))
    .build()

  //Creating a train validation split transformer on our train dataset
  val trainValidationSplit = new TrainValidationSplit()
    .setEstimator(pipelineRFC)
    .setEvaluator(evaluator)
    .setEstimatorParamMaps(paramGrid)
    .setTrainRatio(0.7)

  //Applying our train validation split on the pipeline
  val model = trainValidationSplit.fit(train)

  //Computing predictions
  val predictionsGrid = model.transform(test)

  //Evaluation of accuracy of our multiple classifier
  val accuracyGrid = evaluator.evaluate(predictionsGrid)
  println(s"Test error: ${1.0 - accuracyGrid}")
  println(s"Accuracy: $accuracyGrid")
}
