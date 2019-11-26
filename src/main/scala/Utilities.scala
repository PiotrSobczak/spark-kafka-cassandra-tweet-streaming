package com.piosobc.sparkstreamingtweets

import org.apache.log4j.Level
import java.util.regex.Pattern
import java.util.regex.Matcher

import scala.collection.mutable
import scala.util.parsing.json.JSON
import scala.io.Source
import java.util.Properties

object Utilities {
    /** Makes sure only ERROR messages get logged to avoid log spam. */
  def setupLogging() = {
    import org.apache.log4j.{Level, Logger}   
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)   
  }

  def readJson(jsonPath: String) : Map[String, String] = {
    val jsonString = scala.io.Source.fromFile(jsonPath).mkString
    val mapObject = JSON.parseFull(jsonString).get.asInstanceOf[Map[String, String]]
    mapObject
  }

  /** Configures Twitter service credentials */
  def setupTwitter(twitterConfig: Map[String, String]) = {
    for ((key, value) <- twitterConfig) System.setProperty("twitter4j.oauth." + key, value)
  }

  /** Configures kafka properties */
  def setupKafkaProperties(twitterConfig: Map[String, String]): Properties = {
    val kafkaProps = new Properties()
    for ((key, value) <- twitterConfig) kafkaProps.put(key, value)
    kafkaProps
  }

  def parseArgs(args: Array[String]) : mutable.Map[String, String] = {
    var argsMap:  mutable.Map[String, String] = mutable.Map[String, String]()

    args.sliding(2, 2).toList.collect {
      case Array("--twitterConfig", twitterConfig: String) => argsMap.put("--twitterConfig", twitterConfig)
      case Array("--kafkaConfig", kafkaConfig: String) => argsMap.put("--kafkaConfig", kafkaConfig)
    }
    argsMap
  }
}