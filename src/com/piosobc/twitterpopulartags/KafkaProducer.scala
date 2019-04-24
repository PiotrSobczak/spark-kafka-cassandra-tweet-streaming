package com.piosobc.twitterpopulartags

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._
import Utilities._
import java.util.Properties
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization._;
import twitter4j.Status

/** Listens to a stream of Tweets and keeps track of the most popular
 *  hashtags over a 5 minute window.
 */
object KafkaProducer {
  
  val TOPIC : String = "tweets";
  case class TweetObj(message: String, user: String)
  
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
  // all CPU cores and one-second batches of data
  val ssc = new StreamingContext("local[*]", "PopularHashtags", Seconds(1))
  
  // Get rid of log spam (should be called after the context is set up)
  setupLogging()
  
  // Create a DStream from Twitter using our streaming context
  val tweets = TwitterUtils.createStream(ssc, None)
  
  val tweetObjects = tweets.map(toJson)

  tweetObjects.foreachRDD(rdd => {
    rdd.foreachPartition { message =>
      message.foreach( msg => {
        val producer = new KafkaProducer[String, String](props)
        val pr = new ProducerRecord(TOPIC, "key", msg);
        producer.send(pr)
        producer.close() 
      })
    }
  })
    ssc.checkpoint("/home/piosobc/checkpoint")
    ssc.start()
    ssc.awaitTermination()
  }  
}
