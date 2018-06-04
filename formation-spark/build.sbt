name := "formation-spark"

version := "0.1"

scalaVersion := "2.11.12"

resolvers += "spark-packages" at "https://dl.bintray.com/spark-packages/maven/"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.1.2" % "provided"

assemblyMergeStrategy in assembly := {
  case PathList("org", "apache", "spark", "unused", "UnusedStubClass.class") => MergeStrategy.first
  case PathList("javax", "inject", xs @ _*) => MergeStrategy.last
  case PathList("org", "aopalliance", xs @ _*) => MergeStrategy.last
  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
  case PathList("overview.html", xs @ _*) => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

//mainClass in assembly := Some("Example")
