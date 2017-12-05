import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{StringType, TimestampType}
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.read

object StreamSQL extends App {
  val spark = SparkSession.builder()
    .appName("SQL Kafka")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._

  val df = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "192.168.61.50:31200")
    .option("subscribe", "spark")
    .load()

  implicit val formats = DefaultFormats

  import org.apache.spark.sql.functions._

  val opera = ".*Opera.*".r
  val safari = ".*Safari.*".r
  val chrome = ".*Chrome.*".r
  val firefox = ".*Firefox.*".r

  val toNav = udf((ua: String) => {
    if (ua != null) {
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

  val values = df
    .select($"value" cast StringType)
    .map(a => read[Request](a.getString(0)))
    .select(toNav($"userAgent") as "browser", $"timestamp" cast TimestampType, $"price")
    .groupBy($"browser", window($"timestamp", "5 minutes"))
    .agg(count($"price"), avg($"price"))

  values
    .writeStream
    .format("parquet")
    .option("path", "hdfs://cluster/user/hdfs/spark/aggregation.parquet")
    .outputMode(OutputMode.Complete())
    .start()

  values
    .writeStream
    .format("console")
    .outputMode(OutputMode.Complete())
    .option("truncate", "false")
    .start()
    .awaitTermination()
}
