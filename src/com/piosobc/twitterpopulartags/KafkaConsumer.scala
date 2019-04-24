// Kafka setup instructions for Windows: https://dzone.com/articles/running-apache-kafka-on-windows-os

package com.piosobc.twitterpopulartags

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.storage.StorageLevel

import java.util.regex.Pattern
import java.util.regex.Matcher

import Utilities._

/** Cassandra imports */
import com.datastax.spark.connector._
import com.datastax.driver.core.Cluster
import org.apache.spark.sql.cassandra._

/** Kaffka imports */
import org.apache.spark.streaming.kafka._
import kafka.serializer.StringDecoder

/** Working example of listening for log data from Kafka's testLogs topic on port 9092. */
object KafkaConsumer {
  
  val TOPIC : String = "tweets";
  
  def main(args: Array[String]) {    
    // Set up the Cassandra host address
    val conf = new SparkConf()
    conf.set("spark.cassandra.connection.host", "127.0.0.1")
    conf.setMaster("local[*]")
    conf.setAppName("CassandraExample")
    
    // get the values we need out of the config file
    val cassandra_host = conf.get("spark.cassandra.connection.host"); //cassandra host

    val cluster = Cluster.builder().addContactPoint(cassandra_host).build()
    val session = cluster.connect()
    session.execute("CREATE KEYSPACE IF NOT EXISTS ic_example5 WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")
    session.execute("CREATE TABLE IF NOT EXISTS ic_example5.word_count (msg text, user text, lang text, time text, id bigint, PRIMARY KEY(id)) ")
    session.execute("TRUNCATE ic_example5.word_count")
    session.close()
    
    val ssc = new StreamingContext(conf, Seconds(2))
    
    setupLogging()
    
    // Construct a regular expression (regex) to extract fields from raw Apache log lines
    val pattern = apacheLogPattern()

    // hostname:port for Kafka brokers, not Zookeeper
    val kafkaParams = Map("metadata.broker.list" -> "localhost:9092")
    // List of topics you want to listen for from Kafka
    val topics = List(TOPIC).toSet
    // Create our Kafka stream, which will contain (topic,message) pairs. We tack a 
    // map(_._2) at the end in order to only get the messages, which contain individual
    // lines of data.
    val lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics).map(_._2)
    val objects = lines.map(line => scala.util.parsing.json.JSON.parseFull(line).get.asInstanceOf[Map[String, Any]])
    val objectTuples = objects.map(obj => (obj("msg"), obj("user"), obj("lang"), obj("time"), obj("id")))
    
     //Now store it in Cassandra
    objectTuples.foreachRDD((rdd, time) => {
      rdd.cache()
      println("Writing " + rdd.count() + " rows to Cassandra")
      rdd.saveToCassandra("ic_example5", "word_count", SomeColumns("msg", "user", "lang", "time", "id"))
    })
    
    // Kick it off
    ssc.checkpoint("/home/piosobc/checkpoint")
    ssc.start()
    ssc.awaitTermination()
  }
}
