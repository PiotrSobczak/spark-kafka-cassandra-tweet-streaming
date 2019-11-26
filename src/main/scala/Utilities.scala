package com.piosobc.sparkstreamingtweets

import scala.collection.mutable
import scala.util.parsing.json.JSON
import java.util.Properties

import org.apache.spark.SparkConf

/** Cassandra imports */
import com.datastax.driver.core.Cluster

object Utilities {
    def parseArgs(args: Array[String]) : mutable.Map[String, String] = {
      var argsMap:  mutable.Map[String, String] = mutable.Map[String, String]()

      args.sliding(2, 2).toList.collect {
        case Array("--config", twitterConfig: String) => argsMap.put("--config", twitterConfig)
      }
      argsMap
    }

    /** Makes sure only ERROR messages get logged to avoid log spam. */
    def setupLogging() = {
      import org.apache.log4j.{Level, Logger}
      val rootLogger = Logger.getRootLogger()
      rootLogger.setLevel(Level.ERROR)
    }

    def readJson(configPath: String) : Map[String, String] = {
      val jsonString = scala.io.Source.fromFile(configPath).mkString
      val mapObject = JSON.parseFull(jsonString).get.asInstanceOf[Map[String, String]]
      mapObject
    }

    /** Configures Twitter service credentials */
    def setupTwitter(twitterConfig: Map[String, String]) = {
      for ((key, value) <- twitterConfig) System.setProperty("twitter4j.oauth." + key, value)
    }

    /** Configures kafka properties */
    def setupKafka(twitterConfig: Map[String, String]): Properties = {
      val kafkaProps = new Properties()
      for ((key, value) <- twitterConfig) kafkaProps.put(key, value)
      kafkaProps
    }

    def setupCassandra(cassandraConfig: Map[String, String]) = {
      val cluster = Cluster.builder().addContactPoint(cassandraConfig("spark.cassandra.connection.host")).build()
      val session = cluster.connect()
      session.execute("CREATE KEYSPACE IF NOT EXISTS %s WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };" format (cassandraConfig("keyspace")))
      session.execute("CREATE TABLE IF NOT EXISTS %s.%s (msg text, user text, lang text, time text, id bigint, PRIMARY KEY(id)) " format (cassandraConfig("keyspace"), cassandraConfig("table")))
      session.execute("TRUNCATE  %s.%s" format (cassandraConfig("keyspace"), cassandraConfig("table")))
      session.close()
    }

    def setupSpark(sparkConfig: Map[String, String]): SparkConf = {
      val sparkConf = new SparkConf().set(
        "spark.cassandra.connection.host",
        sparkConfig("spark.cassandra.connection.host")
      )
      sparkConf.setMaster(sparkConfig("master"))
      sparkConf.setAppName(sparkConfig("appName"))
      sparkConf
    }
}