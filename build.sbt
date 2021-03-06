name := "SparkKafkaCassandraApp"

version := "1.0"

organization := "com.piosobc"

scalaVersion := "2.11.8"

assemblyMergeStrategy in assembly := {
 case PathList("META-INF", xs @ _*) => MergeStrategy.discard
 case x => MergeStrategy.first
}

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.4" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.4.4"
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.4.4"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.4"
libraryDependencies += "org.twitter4j" % "twitter4j-core" % "4.0.4"
libraryDependencies += "org.twitter4j" % "twitter4j-stream" % "4.0.4"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.3"
libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "2.3.1"
libraryDependencies += "org.apache.logging.log4j" % "log4j-scala" % "11.0"
libraryDependencies += "org.apache.bahir" %% "spark-streaming-twitter" % "2.3.0"
libraryDependencies += "org.scalatest" % "scalatest_2.11" % "3.0.5" % "test"
libraryDependencies += "com.holdenkarau" %% "spark-testing-base" % "2.1.0_0.8.0" % Test

resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"
libraryDependencies += "MrPowers" % "spark-fast-tests" % "0.20.0-s_2.11"