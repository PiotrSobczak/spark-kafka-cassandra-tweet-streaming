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
  
  /** Our main function where the action happens */
  def main(args: Array[String]) {
     val  props = new Properties()
     props.put("bootstrap.servers", "localhost:9092")   
     props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
     props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      
    // Configure Twitter credentials using twitter.txt
    setupTwitter()
    
    // Set up a Spark streaming context named "PopularHashtags" that runs locally using
    val ssc = new StreamingContext("local[*]", "KafkaProducer", Seconds(BATCH_INTERVAL_IN_SEC))
    
    // Getting rid of log spam */
    setupLogging()
    
    // Create a DStream from Twitter
    val tweetsRaw = TwitterUtils.createStream(ssc, None)
    
    //Convert Twitter Statuses to jsons
    val tweetObjects = tweetsRaw.map(toJson)
    
    // Publishing Twitter jsons on Kafka topic
    // Note: Creating and disposing KafkaProducer for each partition results in an overhead. This could be optimized using Sinks.
    tweetObjects.foreachRDD { rdd =>
      rdd.foreachPartition { partitionOfRecords =>
        val producer = new KafkaProducer[String, String](props)
        partitionOfRecords.foreach { message =>
          producer.send(new ProducerRecord(TOPIC, "key", message))
        }
        producer.close()
      }
    }
    ssc.checkpoint(CHECKPOINT_PATH)
    ssc.start()
    ssc.awaitTermination()
  }  
}
