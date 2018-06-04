package io.saagie.training.sql

import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

//A line in a text file
case class Line(value: String)

//A word
case class Word(value: String)

//Description of a word and its number of occurrences
case class WordCount(value: String, count: Long)

//Word with it's occurrences and density
case class Density(value: String, occurences: Long, density: Double)

//Total count of words
case class Total(value: Long)

object SQLExample extends App {

  //Creation of Spark Session
  val spark = SparkSession.builder()
    .appName("SQL Example")
    .config("spark.ui.showConsoleProgress", false)
    .config("spark.executor.extraJavaOptions", "-Dlog4j.configuration=log4j.xml")
    .getOrCreate()

  //Limiting log output if log4j file is not used
  spark.sparkContext.setLogLevel("ERROR")

  //Import functions such as desc() or lit("")
  import org.apache.spark.sql.functions._

  //Import implicit methods for dataframe conversion
  import spark.implicits._

  //We read the text file
  val df = spark
    .read
    .text("/user/formation-spark/sample.txt")

  //Transformation of every lines into a word and filtering on empty words
  val words = df
    .flatMap(a => a.getString(0).split(' '))
    .filter(v => v.nonEmpty)

  //Grouping of every words, count of each word occurrence and sort of words by occurence
  val occurrences = words
    .groupBy($"value")
    .count()
    .sort(desc("count"))

  occurrences.show()

  //Writing file in csv format
  occurrences
    .write
    .format("csv")
    .mode(SaveMode.Overwrite)
    .save("/user/formation-spark/result.csv")

  //Counting the number of words
  val total: Dataset[Total] = broadcast(Seq(Total(words.count())).toDS())

  //UDF to replace sed by awk in every words
  val replaceSed = udf((s: String) => s.replace("sed", "awk"))

  //Applying UDF to dataframe and computing the density and showing the top 20
  occurrences
    .withColumn("value2", $"value")
    .select(replaceSed($"value2") as "word", $"count", $"count" * 100.0 / total.head().value as "density" cast StringType)
    .orderBy(desc("density"))
    .show()


  //Creating word count with datasets
  val wordCount: Dataset[WordCount] = df
    .as[Line]
    .flatMap(_.value.split(" "))
    .as[Word]
    .filter(_.value.nonEmpty)
    .groupBy($"value")
    .count()
    .as[WordCount]

  //Showing the top 20 results
  wordCount.show()

  //Compute density
  val densityDataframe: Dataset[Density] = wordCount
    .select(replaceSed($"value") as "value", $"count" as "occurences", $"count" * 100.0 / total.head().value as "density")
    .as[Density]
    .orderBy(desc("density"))

  densityDataframe.show()

  val tot = total.head().value

  //Compute density with map /!\ does not work on clusters as tot value can't be broadcasted
  val densityDataset: Dataset[Density] = wordCount
    .map(wordCount => {
      Density(wordCount.value.replace("sed", "awk"), wordCount.count, wordCount.count * 100.0 / tot)
    })
    .orderBy(desc("density"))

  densityDataset.show()
}
