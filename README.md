# Project description
This is a sample application which reads tweets from Twitter stream, processes them and saves to Cassandra.
The application consist of two spark jobs. The first one reads input Twitter stream using spark streaming, 
extracts message, username, datetime, language and tweet_id from tweet and sends it to the second spark job using Kafka.
The second spark job reads the messages from Kafka topic, parses them and saves to Cassanda database.
Note: This is just a sample application which is configured to run on localhost. 

# Dependencies
- Scala(2.11)
- Spark(2.3.3)
- Kafka(1.1)
- Cassandra(3.11.4)  

# Setting up twitter access
Go to https://developer.twitter.com and apply for a Twitter Developer Account. Follow these 
[instructions](https://developer.twitter.com/en/docs/basics/authentication/guides/access-tokens.html) to 
generate access tokens. Copy twitter.config.dist to twitter.config and paste consumerKey, consumerSecret, accessToken,
accessTokenSecret to missing fields. 

# Building jar
This application uses sbt for managing dependencies. In order to build the application jar, simply run:
```
sbt assembly
```

# Running the application
```
cd target/scala-2.11/
spark-submit --class com.piosobc.sparkstreamingtweets.KafkaProducer SparkKafkaCassandraApp-assembly-1.0.jar
spark-submit --class com.piosobc.sparkstreamingtweets.KafkaConsumer SparkKafkaCassandraApp-assembly-1.0.jar

```
