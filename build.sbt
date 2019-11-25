name := "SparkKafkaCassandraApp"

version := "1.0"

organization := "com.piosobc"

scalaVersion := "2.11.8"

assemblyMergeStrategy in assembly := {
 case PathList("META-INF", xs @ _*) => MergeStrategy.discard
 case x => MergeStrategy.first
}

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.3" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.3.3"
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-8" % "2.0.0"
libraryDependencies += "org.twitter4j" % "twitter4j-core" % "4.0.4"
libraryDependencies += "org.twitter4j" % "twitter4j-stream" % "4.0.4"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.3"
libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "2.3.1"
libraryDependencies += "org.apache.logging.log4j" % "log4j-scala" % "11.0"
libraryDependencies += "org.apache.bahir" %% "spark-streaming-twitter" % "2.3.0"
//libraryDependencies += "org.apache.avro"  %  "avro"  %  "1.7.7"
libraryDependencies += "org.scalatest" % "scalatest_2.11" % "3.0.5" % "test"
libraryDependencies += "com.holdenkarau" %% "spark-testing-base" % "2.1.0_0.8.0" % Test
//libraryDependencies += "io.spray" %%  "spray-json" % "1.3.4"