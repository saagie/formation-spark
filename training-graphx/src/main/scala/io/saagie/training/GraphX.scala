package io.saagie.training

import org.apache.spark.graphx.{GraphLoader, PartitionStrategy}
import org.apache.spark.{SparkConf, SparkContext}

object GraphX extends App {
  //Create spark configuration
  val conf = new SparkConf()
    .setAppName("Spark Formation")
    .setMaster("local[*]")
    .set("spark.ui.showConsoleProgress", "false")
    .set("spark.executor.extraJavaOptions", "-Dlog4j.configuration=log4j.xml")

  //Create spark context
  val sc = new SparkContext(conf)

  //Loading graph from edges
  val graph = GraphLoader.edgeListFile(sc, "followers.txt")
    .partitionBy(PartitionStrategy.RandomVertexCut)

  // Run PageRank
  val ranks = graph.pageRank(0.0001).vertices

  // Join the ranks with the user names
  val users = sc.textFile("users.txt").map { line =>
    val fields = line.split(",")
    (fields(0).toLong, fields(1))
  }

  // Mapping ranks to users
  val ranksByUsername = users.join(ranks).map {
    case (_, (username, rank)) => (username, rank)
  }

  // Print the result
  println(ranksByUsername.sortBy(_._2, false).collect().mkString("\n"))

  println()

  //Run triangle count algorithm
  val triCounts = graph.triangleCount().vertices

  //Mapping triangle counts to users
  val triCountByUsername = users.join(triCounts).map { case (id, (username, tc)) =>
    (username, tc)
  }
  // Print the result
  println(triCountByUsername.sortBy(_._2, false).collect().mkString("\n"))
}
