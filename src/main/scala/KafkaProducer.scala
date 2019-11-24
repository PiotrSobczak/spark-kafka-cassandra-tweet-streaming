package com.piosobc.sparkstreamingtweets

/** Spark imports */
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._

/** Kafka imports */
import org.apache.kafka.clients.producer._

import org.apache.kafka.common.serialization._;

/** Other imports */
import java.util.Properties
import twitter4j.Status

/** Custom imports */
import Utilities._

/** Listens to a stream of Tweets and keeps track of the most popular
 *  hashtags over a 5 minute window.
 */

/** KafkaProducer Wrapper */
class KafkaSink(createProducerFunc: () => KafkaProducer[String, String]) extends Serializable {

  lazy val producer = createProducerFunc()

  def send(topic: String, value: String): Unit = producer.send(new ProducerRecord(topic, value))
}

/** KafkaProducer Factory */
object KafkaSink {
  def apply(config: Properties): KafkaSink = {
    val createProducerFunc = () => {
      val producer = new KafkaProducer[String, String](config)

      /** Closing producer on JVM close, Without it, all messages buffered internally by Kafka producer would be lost. */
      sys.addShutdownHook {
        producer.close()
      }

      producer
    }
    new KafkaSink(createProducerFunc)
  }
}


object KafkaProducer {
  
  val TOPIC : String = "tweets";
  val BATCH_INTERVAL_IN_SEC : Int = 1;
  val CHECKPOINT_PATH : String = System.getProperty("user.home") + "/checkpoint";
  
  def toJson(status: Status ) : String = {
    val mapObj = Map[String, Any](
      "msg" -> status.getText,
      "user" -> status.getUser.getName,
      "lang" -> status.getLang,
      "time" -> status.getCreatedAt.toString(),
      "id" -> status.getId
    )
    return scala.util.parsing.json.JSONObject(mapObj).toString()
  }

  // TODO AVRO

  /** Our main function where the action happens */
  def main(args: Array[String]) {
     val  props = new Properties()
     props.put("bootstrap.servers", "localhost:9092")   
     props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
     props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      
    // Configure Twitter credentials using twitter.txt
    setupTwitter()
    
    // Set up a Spark streaming context named "KafkaProducer" that runs locally using
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("KafkaProducer")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(BATCH_INTERVAL_IN_SEC))

    // Setting log level
    setupLogging()
    
    // Create a DStream from Twitter
    val tweetsRaw = TwitterUtils.createStream(ssc, None)

    // Convert Twitter Statuses to jsons
    val tweetObjects = tweetsRaw.map(toJson)
    
    // Publishing Twitter jsons on Kafka topic
    // Sink solution based on: https://allegro.tech/2015/08/spark-kafka-integration.html
    val kafkaSink = sc.broadcast(KafkaSink(props))
    tweetObjects.foreachRDD { rdd =>
      rdd.foreach { message =>
        kafkaSink.value.send(TOPIC, message)
      }
    }
    ssc.checkpoint(CHECKPOINT_PATH)
    ssc.start()
    ssc.awaitTermination()
  }  
}
