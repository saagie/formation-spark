package io.saagie.training.streaming.sql

import io.saagie.training.streaming.rdd.Request
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{StringType, TimestampType}
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.read

object StreamSQL extends App {
  //Initialize a Spark Session
  val spark = SparkSession.builder()
    .appName("SQL Kafka")
    .config("spark.ui.showConsoleProgress", "false")
    .config("spark.executor.extraJavaOptions", "-Dlog4j.configuration=log4j.xml")
    .getOrCreate()

  //Import sql functions
  import org.apache.spark.sql.functions._
  import spark.implicits._

  //Open a stream to a kafka broker
  val df = spark
    .readStream
    .format("kafka")
    .option("startingOffsets", "earliest")
    .option("kafka.bootstrap.servers", "192.168.61.50:31200")
    .option("subscribe", "spark")
    .load()

  //json4s formats
  implicit val formats = DefaultFormats

  //Regexp used by udf
  val opera = ".*Opera.*".r
  val chrome = ".*Chrome.*".r
  val firefox = ".*Firefox.*".r

  //We create an udf to transform each user agent into a navigator String
  val toNav = udf((ua: String) => {
    if (ua != None.orNull) {
      ua match {
        case opera() => "Opera"
        case chrome() => "Chrome"
        case firefox() => "Firefox"
        case _ => ua
      }
    } else {
      "Aucun"
    }
  })

  //Select the value of the dataframe
  val stringValue = df
    .select($"value" cast StringType)

  //Map into a request object and therefore a Dataset
  val ds = stringValue
    .map(a => read[Request](a.getString(0)))

  /* Apply the udf to our request, group by browsers over a window of 10 seconds,
     based on request's timestamp's and compute average */
  val prices = ds
    .select(toNav($"userAgent") as "browser", $"timestamp" cast TimestampType, $"price")
    .groupBy($"browser", window($"timestamp", "10 seconds"))
    .agg(count($"price"), avg($"price"))

  //Write into a file not working with all Spark versions
  //  prices
  //    .writeStream
  //    .format("parquet")
  //    .option("path", "/user/formation-spark/streaming/sql")
  //    .outputMode(OutputMode.Append())
  //    .start()

  //Write our aggregation into the console on each new value
  prices
    .writeStream
    .format("console")
    .outputMode(OutputMode.Complete())
    .option("truncate", "false")
    .start()
    .awaitTermination()
}
