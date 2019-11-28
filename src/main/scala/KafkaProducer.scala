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
import Config._

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
      "id" -> status.getId.toString()
    )
    return scala.util.parsing.json.JSONObject(mapObj).toString()
  }

  // TODO AVRO

  /** Our main function where the action happens */
  def main(args: Array[String]) {
    // Parse args
    val argsMap = Utilities.parseArgs(args)

    // Load twitter and kafka config jsons
    val twitterConfig = readJson(argsMap("--config") + "/" + TWITTER_CONFIG_FILE)
    val kafkaConfig = readJson(argsMap("--config") + "/" + KAFKA_CONFIG_FILE)

    // Configure Twitter credentials using twitter.txt
    setupTwitter(twitterConfig)
    
    // Set up a Spark streaming context named "KafkaProducer" that runs locally using
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("KafkaProducer")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(BATCH_INTERVAL_IN_SEC))

    val kafkaProps = setupKafka(kafkaConfig)

    // Sink solution based on: https://allegro.tech/2015/08/spark-kafka-integration.html
    val kafkaSink = sc.broadcast(KafkaSink(kafkaProps))

    // Setting log level
    setupLogging()
    
    // Create a DStream from Twitter
    val tweetsRaw = TwitterUtils.createStream(ssc, None)

    // Convert Twitter Statuses to jsons
    val tweetObjects = tweetsRaw.map(toJson)
    
    // Publishing Twitter jsons on Kafka topic
    tweetObjects.foreachRDD { rdd =>
      rdd.foreach { message =>
        kafkaSink.value.send(kafkaProps.getProperty("topic"), message)
      }
    }
    ssc.checkpoint(CHECKPOINT_PATH)
    ssc.start()
    ssc.awaitTermination()
  }  
}
