import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.read

case class Request(origin: String, ip: String, userAgent: Option[String], timestamp: Long, latitude: Double, longitude: Double, price: Double)

object Stream extends App {
  System.setProperty("HADOOP_USER_NAME", "hdfs")

  val sparkConf = new SparkConf()
    .setAppName("Spark Formation")

  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "192.168.61.50:31200",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "spark-formation-group",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  val streamingContext = new StreamingContext(sparkConf, Seconds(1))
  streamingContext.sparkContext.setLogLevel("ERROR")

  val stream = KafkaUtils.createDirectStream[String, String](
    streamingContext,
    PreferConsistent,
    Subscribe[String, String](Array("spark"), kafkaParams)
  )

  val opera = ".*Opera.*".r
  val chrome = ".*Chrome.*".r
  val firefox = ".*Firefox.*".r

  def nav(ua: String): String = {
    ua match {
      case opera() => "Opera"
      case chrome() => "Chrome"
      case firefox() => "Firefox"
      case _ => ua
    }
  }

  implicit val formats = DefaultFormats
  stream
    .transform(_.map(_.value()))
    .transform(rdd => if (!rdd.isEmpty()) {
      rdd.map(read[Request])
    } else {
      streamingContext.sparkContext.emptyRDD[Request]
    })
    .window(Minutes(5))
    .map(r => (nav(r.userAgent.getOrElse("Aucun")), r.price))
    .foreachRDD { rdd => {
      val to = rdd
        .groupBy(_._1)
        .reduceByKey(_ ++ _)
        .mapValues(v => (v.size, v.aggregate(0.0)((a, b) => a + b._2, _ + _) / v.size))
        .map(tuple => (tuple._1, tuple._2._1, tuple._2._2))
        .sortBy(-_._2)
      println(to.collect().toSeq)
      to.saveAsTextFile(s"hdfs://cluster/user/hdfs/spark/requests-${System.currentTimeMillis()}")
    }
    }

  streamingContext.start()
  streamingContext.awaitTermination()
}
