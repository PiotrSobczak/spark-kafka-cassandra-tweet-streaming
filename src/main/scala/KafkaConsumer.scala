// Kafka setup instructions for Windows: https://dzone.com/articles/running-apache-kafka-on-windows-os

package com.piosobc.sparkstreamingtweets

/** Spark imports */
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructType}

/** Kafka imports */
import org.apache.spark.sql.kafka010._

/** Cassandra imports */
import com.datastax.spark.connector.SomeColumns
import com.datastax.spark.connector._

/** Custom imports */
import Utilities._
import Config._

/** Working example of listening for log data from Kafka's testLogs topic on port 9092. */
object KafkaConsumer {
  var spark : SparkSession = null

  def main(args: Array[String]) {    
    // Set up the Cassandra host address
    val argsMap = Utilities.parseArgs(args)

    val kafkaConfig = readJson(argsMap("--config") + "/" + KAFKA_CONFIG_FILE)
    val cassandraConfig = readJson(argsMap("--config") + "/" + CASSANDRA_CONFIG_FILE)
    val sparkConfig = readJson(argsMap("--config") + "/" + SPARK_CONFIG_FILE)

    spark = setupSpark(sparkConfig)
    val sparkVal = spark
    import sparkVal.sqlContext.implicits._

    // TODO Implement Cassandra Setup
//    setupCassandra(cassandraConfig)

    // Create DataSet representing the stream of input lines from kafka
    val lines = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaConfig("bootstrap.servers"))
      .option("subscribe", kafkaConfig("topic"))
      .option("failOnDataLoss", "false")
      .load()
      .selectExpr("CAST(value AS STRING)")
      .as[String]

    setupLogging()

    val tuples = defineFlow(lines, spark)
    val writeQuery = tuples.writeStream.format("console").option("truncate", "false").start()
    writeQuery.awaitTermination()
    // TODO Implement Cassandra Sink
  }

  def defineFlow(lines: Dataset[String], spark: SparkSession) = {
    import spark.sqlContext.implicits._

    val schema = new StructType()
      .add("id", StringType)
      .add("msg", StringType)
      .add("time", StringType)
      .add("lang", StringType)
      .add("user", StringType)

    val tuples = lines.select(from_json($"value", schema).as("person")).select("person.*").
      selectExpr("CAST(id AS STRING)", "CAST(msg AS STRING)", "CAST(time AS STRING)", "CAST(lang AS STRING)", "CAST(user AS STRING)").as[(String, String, String, String, String)]
    tuples
  }
}
