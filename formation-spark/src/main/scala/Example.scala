import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StringType

object Example extends App {
  val spark = SparkSession.builder().appName("Example").master("local[*]").getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._

  import org.apache.spark.sql.functions._

  val df = spark.read.text("/home/ekoffi/Downloads/SampleTextFile_1000kb.txt")

  val df2 = df
    .flatMap(a => a.mkString.split(' '))
    .filter(v => v.nonEmpty)

  val df3 = df2
    .groupBy($"value")
    .count()
    .sort(desc("count"))

  df3
    .write
    .mode("overwrite")
    .csv("/home/ekoffi/Downloads/result.csv")

  df3.createTempView("toto")

  val total = df2.count()

  val replaceSed = udf((s: String) => s.replace("sed", "awk"))

  df3
    .withColumn("value2", $"value")
    .select(replaceSed($"value2"), $"count", $"count" * 100.0 / total as "densite" cast StringType)
    .show()
}
