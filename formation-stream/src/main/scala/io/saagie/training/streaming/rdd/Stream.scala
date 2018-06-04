package io.saagie.training.streaming.rdd

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.read

//Request class matching data in Kafka
case class Request(origin: String, ip: String, userAgent: Option[String], timestamp: Long, latitude: Double, longitude: Double, price: Double)

object Stream extends App {
  //Sets the Hadoop user name
  System.setProperty("HADOOP_USER_NAME", "hdfs")

  //Initializing the spark configuration
  val sparkConf = new SparkConf()
    .setAppName("Spark Formation")
    .set("spark.ui.showConsoleProgress", "false")
    .set("spark.executor.extraJavaOptions", "-Dlog4j.configuration=log4j.xml")

  //Kafka configuration
  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "192.168.61.50:31200",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "spark-formation-group",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  //Context initialisation
  val streamingContext = new StreamingContext(sparkConf, Seconds(1))

  //Creation of the stream
  val stream = KafkaUtils.createDirectStream[String, String](
    streamingContext,
    PreferConsistent,
    Subscribe[String, String](Array("spark"), kafkaParams)
  )

  //Regex initialization
  val opera = ".*Opera.*".r
  val chrome = ".*Chrome.*".r
  val firefox = ".*Firefox.*".r

  //Function that will transform user agent into a simple navigator string
  def nav(ua: String): String = {
    ua match {
      case opera() => "Opera"
      case chrome() => "Chrome"
      case firefox() => "Firefox"
      case _ => ua
    }
  }

  //Format used by json4s
  implicit val formats = DefaultFormats

  //Initialization of the stream
  stream
    //We transform the stream to get only the value
    .transform(_.map(_.value()))
    //If the rdd is not empty, we parse the JSON
    .transform(rdd => if (!rdd.isEmpty()) {
    rdd.map(read[Request])
  } else {
    streamingContext.sparkContext.emptyRDD[Request]
  })
    //Creation of a 5 minutes window
    .window(Minutes(5))
    //Map the navigator name with a price
    .map(r => (nav(r.userAgent.getOrElse("Aucun")), r.price))
    //For each rdd write the file
    .foreachRDD { rdd => {
    val to = rdd
      .groupBy(_._1) //Group by navigators
      .reduceByKey(_ ++ _) //Sum all the prices
      .mapValues(v => (v.size, v.aggregate(0.0)((a, b) => a + b._2, _ + _) / v.size)) //Aggregation to get the mean
      .map(tuple => (tuple._1, tuple._2._1, tuple._2._2)) //Change tuple format from (String, (Int, Double)) to (String, Int, Double)
      .sortBy(-_._2) //Sort by price

    //Print of each rdd /!\ Really bad on big volume of data
    println(to.collect().toSeq)

    //Write data into a file
    to.saveAsTextFile(s"/user/formation-spark/streaming/requests-${System.currentTimeMillis()}")
  }
  }

  //Start of streaming context
  streamingContext.start()

  //Waiting the termination of the stream (Hangs for exception or execution stop)
  streamingContext.awaitTermination()
}
