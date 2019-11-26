// Kafka setup instructions for Windows: https://dzone.com/articles/running-apache-kafka-on-windows-os

package com.piosobc.sparkstreamingtweets

import com.datastax.spark.connector.SomeColumns
import com.datastax.spark.connector._

import org.apache.spark.SparkContext

import scala.collection.JavaConverters._

/** Spark imports */
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream

/** Kafka imports */
import org.apache.spark.streaming.kafka010._

/** Custom imports */
import Utilities._
import Config._

/** Working example of listening for log data from Kafka's testLogs topic on port 9092. */
object KafkaConsumer {
  // TODO IMPLEMENT DATASET API

  def main(args: Array[String]) {    
    // Set up the Cassandra host address
    val usage = """Usage: --config path-to-config-dir""";
    assert(args.length == 2, "Invalid usage! " + usage)
    val argsMap = Utilities.parseArgs(args)

    val kafkaConfig = readJson(argsMap("--config") + "/" + KAFKA_CONFIG_FILE)
    val cassandraConfig = readJson(argsMap("--config") + "/" + CASSANDRA_CONFIG_FILE)
    val sparkConfig = readJson(argsMap("--config") + "/" + SPARK_CONFIG_FILE)

    val sparkConf = setupSpark(sparkConfig)
    val kafkaProps = setupKafka(kafkaConfig)
    setupCassandra(cassandraConfig)

    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(sparkConfig("batchInterval").toLong))

    setupLogging()

    val lines = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](
        Set(kafkaProps.getProperty("topic")), kafkaProps.asScala.toMap[String, String]
      )
    ).map(x => x.value())

    // Transforming jsons to tuples
    val objectTuples = defineFlow(lines)

    // Storing to Cassandra
    objectTuples.foreachRDD((rdd, time) => {
      rdd.cache()
      println("Writing " + rdd.count() + " rows to Cassandra")
      rdd.saveToCassandra(cassandraConfig("keyspace"), cassandraConfig("table"), SomeColumns("msg", "user", "lang", "time", "id"))
    })
    
    ssc.checkpoint(sparkConfig("checkpointPath"))
    ssc.start()
    ssc.awaitTermination()
  }

  def defineFlow(lines: DStream[String]) : DStream[(String, String, String, String, Double)] = {
    // Converting jsons to Map objects
    val objects = lines.map(line => scala.util.parsing.json.JSON.parseFull(line).get.asInstanceOf[Map[String, Any]])

    // Converting Map objects to tuples
    val objectTuples = objects.map(obj => (obj("msg").asInstanceOf[String], obj("user").asInstanceOf[String],obj("lang").asInstanceOf[String], obj("time").asInstanceOf[String],obj("id").asInstanceOf[Double]))
    objectTuples
  }
}
