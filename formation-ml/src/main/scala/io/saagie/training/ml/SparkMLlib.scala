package io.saagie.training.ml

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification._
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorAssembler, VectorIndexer}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{Encoders, SaveMode, SparkSession}

case class Request(uuid: String, origin: String, ip: String, browser: String, timestamp: Long, latitude: Double, longitude: Double, price: Double)

object SparkMLlib extends App {

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

  dirty.show()

  //Transforming our user agent column.
  val clean = dirty
    .select($"uuid", $"url", $"ip", toNav($"user_agent") as "browser", $"timestamp", $"latitude", $"longitude", $"price")

  //Writing a clean dataset.
  clean.
    write
    .option("header", true)
    .mode(SaveMode.Overwrite)
    .csv("dataset_clean.csv")

  clean.show()

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

  //Our loaded dataset
  ds.show()

  //Encoder to transform browser
  val encoder = new StringIndexer()
    .setInputCol("browser")
    .setOutputCol("browserFeature")

  //Encoded browser string
  val dfEncoded = encoder
    .fit(ds)
    .transform(ds)

  dfEncoded.show()

  //Vector assembler to prepare features
  val vectorAssembler = new VectorAssembler()
    .setInputCols(Array("timestamp", "longitude", "latitude", "price"))
    .setOutputCol("features")

  //Applying assembler to our features
  val dsFeatures = vectorAssembler.transform(dfEncoded)
  dsFeatures.show()

  //Indexer of our browser into a label column
  val labelIndexer = new StringIndexer()
    .setInputCol("browser")
    .setOutputCol("label")
    .fit(dsFeatures)

  labelIndexer
    .transform(dsFeatures)
    .show()

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

  modelRFC.write.overwrite().save("pipelineRFC.model")

  modelRFC.transform(test).show()

  System.exit(0)

  //Creation of a new classifier
  val mlp = new MultilayerPerceptronClassifier()
    .setLabelCol("label")
    .setFeaturesCol("indexed")
    .setLayers(Array(4, 5, 4, 3))
    .setBlockSize(128)
    .setSeed(1234L)
    .setMaxIter(100)

  //Creating a new pipeline with a different classifier
  val pipelineMLP = new Pipeline()
    .setStages(Array(encoder, vectorAssembler, labelIndexer, featureIndexer, mlp, labelConverter))

  val modelMLP = pipelineMLP.fit(train)

  modelMLP.transform(ds).show()

  mlp.write.overwrite().save("pipelineRFC.model")
}
