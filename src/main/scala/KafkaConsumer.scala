// Kafka setup instructions for Windows: https://dzone.com/articles/running-apache-kafka-on-windows-os

package com.piosobc.sparkstreamingtweets

import scala.collection.JavaConverters._

/** Spark imports */
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, InputDStream}

/** Cassandra imports */
import com.datastax.spark.connector._
import com.datastax.driver.core.Cluster
import org.apache.spark.sql.cassandra._

/** Kafka imports */
import org.apache.spark.streaming.kafka010._

/** Custom imports */
import Utilities._

/** Working example of listening for log data from Kafka's testLogs topic on port 9092. */
object KafkaConsumer {
  val BATCH_INTERVAL_IN_SEC : Int = 1;
  val CHECKPOINT_PATH : String = System.getProperty("user.home") + "/checkpoint";
  val KEYSPACE : String = "sparkkafkacassandraapp";
  val TABLE_NAME : String = "tweets";

  // TODO IMPLEMENT DATASET API

  def main(args: Array[String]) {    
    // Set up the Cassandra host address
    val conf = new SparkConf()
    conf.set("spark.cassandra.connection.host", "127.0.0.1")
    conf.setMaster("local[*]")
    conf.setAppName("CassandraSink")

    val usage = """Usage: --kafkaConfig path-to-kafka.config""";
    assert(args.length == 2, "Invalid usage! " + usage)
    val argsMap = Utilities.parseArgs(args)
    val kafkaConfig = readJson(argsMap("--kafkaConfig"))
    val kafkaProps = setupKafkaProperties(kafkaConfig)

    // Configuring Cassandra
//    val cassandra_host = conf.get("spark.cassandra.connection.host");
//    val cluster = Cluster.builder().addContactPoint(cassandra_host).build()
//    val session = cluster.connect()
//    session.execute(s"CREATE KEYSPACE IF NOT EXISTS $KEYSPACE WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")
//    session.execute(s"CREATE TABLE IF NOT EXISTS $KEYSPACE.$TABLE_NAME (msg text, user text, lang text, time text, id bigint, PRIMARY KEY(id)) ")
//    session.execute(s"TRUNCATE $KEYSPACE.$TABLE_NAME")
//    session.close()

    val ssc = new StreamingContext(conf, Seconds(BATCH_INTERVAL_IN_SEC))

    setupLogging()

    val lines = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](
        Set(kafkaProps.getProperty("topic")), kafkaProps.asScala.toMap[String, String]
      )
    ).map(x => x.value())
    lines.print()

    // Transforming jsons to tuples
    val objectTuples = transformData(lines)

    objectTuples.print()

    // Storing to Cassandra
    objectTuples.foreachRDD((rdd, time) => {
      rdd.cache()
      println("Writing " + rdd.count() + " rows to Cassandra")
//      rdd.saveToCassandra(KEYSPACE, TABLE_NAME, SomeColumns("msg", "user", "lang", "time", "id"))
    })
    
    ssc.checkpoint(CHECKPOINT_PATH)
    ssc.start()
    ssc.awaitTermination()
  }

  def transformData(lines: DStream[String]) : DStream[(String, String, String, String, Double)] = {
    // Converting jsons to Map objects
    val objects = lines.map(line => scala.util.parsing.json.JSON.parseFull(line).get.asInstanceOf[Map[String, Any]])

    // Converting Map objects to tuples
    val objectTuples = objects.map(obj => (obj("msg").asInstanceOf[String], obj("user").asInstanceOf[String],obj("lang").asInstanceOf[String], obj("time").asInstanceOf[String],obj("id").asInstanceOf[Double]))
    objectTuples
  }
}
