package io.saagie.training.rdd

import org.apache.spark.{SparkConf, SparkContext}

object RDDExample extends App {

  //Initialisation of the SparkContext
  val conf = new SparkConf()
    .setAppName("SQL Example")
    .set("spark.ui.showConsoleProgress", "false")
    .set("spark.executor.extraJavaOptions", "-Dlog4j.configuration=log4j.xml")
    .set("spark.hadoop.validateOutputSpecs", "false") //Option to overwrite directories
  val sc = new SparkContext(conf)

  //Reading of the text file
  val rdd = sc.textFile("/user/formation-spark/sample.txt")

  val words = rdd
    .flatMap(s => s.split(' '))
    .filter(s => s != " " && s != "")

  //Mapping every words as a key and assigning value 1 and then counting the occurrences and sorting by occurrence
  val occurrences = words
    .map(s => (s, 1))
    .reduceByKey(_ + _)
    .sortBy(e => e._2, false)

  //Writing files in csv format
  occurrences
    .map(v => s"${v._1},${v._2}")
    .saveAsTextFile("/user/formation-spark/result.csv")

  //Replacing sed with awk
  val replaced = occurrences
    .map(t => (t._1.replace("sed", "awk"), t._2))

  //Counting element with an aggregation instead of a reduce
  val total = replaced
    .aggregate(0)((t1, t2) => t1 + t2._2, _ + _)

  //Retrieving the top 10 occurences
  println(replaced
    .map(tuple => (tuple._1, tuple._2, tuple._2 * 100.0 / total))
    .sortBy(_._3, false)
    .take(10)
    .toSeq)

  val cleanWord = rdd
    .flatMap(s => {
      s.split(' ')
        .map(_.replace(".", "").toLowerCase)
    })

  //Retrieve the top 10 words that have the most characters in the file.
  println(cleanWord
    .map(t => (t, t.length))
    .reduceByKey(_ + _)
    .sortBy(e => e._2, false)
    .take(10)
    .toSeq)
}
