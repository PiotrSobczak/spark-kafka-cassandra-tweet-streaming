//package com.piosobc.sparkstreamingtweets

import com.holdenkarau.spark.testing.StreamingSuiteBase
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}
import org.scalatest._
import org.scalatest.Matchers._
import com.piosobc.sparkstreamingtweets.KafkaConsumer
import com.piosobc.sparkstreamingtweets.Utilities.setupLogging
import org.apache.spark.sql.execution.streaming.MemoryStream
import com.github.mrpowers.spark.fast.tests.DataFrameComparer

class KafkaConsumerTests extends WordSpec with StreamingSuiteBase with DataFrameComparer {

  "KafkaConsumerTests" should {
    "map json to tuples" in  {

      case class TestCase(json: String, tuple: (String, String, String, String, String))

      val json1 = "{\"id\" : \"1198365408786604035\", \"msg\" : \"Je crois que je suis amoureuse\", \"time\" : \"Sat Nov 23 23:19:12 CET 2019\", \"lang\" : \"fr\", \"user\" : \"coiffeuse vie🥀🌹\"}"
      val json2 = "{\"id\" : \"1198365408790753291\", \"msg\" : \"https://t.co/RXJOu9ofcd https://t.co/fdiwFiqFLK\", \"time\" : \"Sat Nov 23 23:19:12 CET 2019\", \"lang\" : \"und\", \"user\" : \"Guaglia\"}"
      val json3 = "{\"id\" : \"1198365408765652992\", \"msg\" : \"Con que ahora Belinda usa a los comandantes e influencias del alto mando del Presidente con el que amablemente colaboró en campaña para amedrentar a todo aquel periodista que hable de su relación con el moreno luchón de Lupillo. O sea no entiendo le hemos conocido peores cosas . https://t.co/txRQ6rW1qQ\", \"time\" : \"Sat Nov 23 23:19:12 CET 2019\", \"lang\" : \"es\", \"user\" : \"Economía Moral ®\"}"
      val json4 = "{\"id\" : \"1198365412955709442\", \"msg\" : \"Putin Benzema il a encore marquer c’est trop wlh\", \"time\" : \"Sat Nov 23 23:19:13 CET 2019\", \"lang\" : \"fr\", \"user\" : \"LaMentalee 🏴‍☠️\"}"
      val json5 = "{\"id\" : \"1198365408773980160\", \"msg\" : \"RT @lita_9_6: ตอนชั้นโดนแหก มีคนบอกว่าอิแด๊ดก็ไม่เห็นจะอะไร อวยเว่อ เยไม่มันส์บ้างอะไรบ้าง อันนี้ไม่เคืองนะคนที่เคืองคืออิแด๊ด555555\", \"time\" : \"Sat Nov 23 23:19:12 CET 2019\", \"lang\" : \"th\", \"user\" : \"NattyThaiELF. ต้องสอบผ่านB1\"}"
      val json6 = "{\"id\" : \"1198365408778215426\", \"msg\" : \"VIVI PRA VER O FLAMENGO SER CAMPEÃO DA LIBERTADORESSSSSS\", \"time\" : \"Sat Nov 23 23:19:12 CET 2019\", \"lang\" : \"pt\", \"user\" : \"ᴀɴɴʏ\"}"
      val json7 = "{\"id\" : \"1198365408757202946\", \"msg\" : \"@ketaminepura el modo!!!!!\", \"time\" : \"Sat Nov 23 23:19:12 CET 2019\", \"lang\" : \"es\", \"user\" : \"💀🧚‍♀️💀🧚‍♀️💀🧚‍♀️🧚‍♀️💀🧚‍♀️💀🧚‍♀️🧚‍♀️\"}"
      val json8 = "{\"id\" : \"1198365408761384960\", \"msg\" : \"@todobakuu I’m a Pisces of trash\", \"time\" : \"Sat Nov 23 23:19:12 CET 2019\", \"lang\" : \"en\", \"user\" : \"Lady Gugu\"}"
      val json9 = "{\"id\" : \"1198365408778215424\", \"msg\" : \"RT @DianAngel01: https://t.co/9w4pMPeHy6\", \"time\" : \"Sat Nov 23 23:19:12 CET 2019\", \"lang\" : \"und\", \"user\" : \"SebastianAristizabal\"}"
      val json10 = "{\"id\" : \"1198365408765583360\", \"msg\" : \"RT @RyanKGives: Should I giveaway a FAST $500 to someone?\", \"time\" : \"Sat Nov 23 23:19:12 CET 2019\", \"lang\" : \"en\", \"user\" : \"ICEDriver\"}"

      val tuple1 = ("1198365408786604035", "Je crois que je suis amoureuse","Sat Nov 23 23:19:12 CET 2019","fr","coiffeuse vie🥀🌹")
      val tuple2 = ("1198365408790753291", "https://t.co/RXJOu9ofcd https://t.co/fdiwFiqFLK","Sat Nov 23 23:19:12 CET 2019","und","Guaglia")
      val tuple3 = ("1198365408765652992", "Con que ahora Belinda usa a los comandantes e influencias del alto mando del Presidente con el que amablemente colaboró en campaña para amedrentar a todo aquel periodista que hable de su relación con el moreno luchón de Lupillo. O sea no entiendo le hemos conocido peores cosas . https://t.co/txRQ6rW1qQ","Sat Nov 23 23:19:12 CET 2019","es","Economía Moral ®")
      val tuple4 = ("1198365412955709442", "Putin Benzema il a encore marquer c’est trop wlh","Sat Nov 23 23:19:13 CET 2019","fr","LaMentalee 🏴‍☠️")
      val tuple5 = ("1198365408773980160", "RT @lita_9_6: ตอนชั้นโดนแหก มีคนบอกว่าอิแด๊ดก็ไม่เห็นจะอะไร อวยเว่อ เยไม่มันส์บ้างอะไรบ้าง อันนี้ไม่เคืองนะคนที่เคืองคืออิแด๊ด555555","Sat Nov 23 23:19:12 CET 2019","th","NattyThaiELF. ต้องสอบผ่านB1")
      val tuple6 = ("1198365408778215426", "VIVI PRA VER O FLAMENGO SER CAMPEÃO DA LIBERTADORESSSSSS","Sat Nov 23 23:19:12 CET 2019","pt","ᴀɴɴʏ")
      val tuple7 = ("1198365408757202946", "@ketaminepura el modo!!!!!","Sat Nov 23 23:19:12 CET 2019","es","💀🧚‍♀️💀🧚‍♀️💀🧚‍♀️🧚‍♀️💀🧚‍♀️💀🧚‍♀️🧚‍♀️")
      val tuple8 = ("1198365408761384960", "@todobakuu I’m a Pisces of trash","Sat Nov 23 23:19:12 CET 2019","en","Lady Gugu")
      val tuple9 = ("1198365408778215424", "RT @DianAngel01: https://t.co/9w4pMPeHy6","Sat Nov 23 23:19:12 CET 2019","und","SebastianAristizabal")
      val tuple10 = ("1198365408765583360", "RT @RyanKGives: Should I giveaway a FAST $500 to someone?","Sat Nov 23 23:19:12 CET 2019","en","ICEDriver")

      val spark: SparkSession = SparkSession.builder.getOrCreate()

      setupLogging()

      import spark.implicits._
      implicit val ctx = spark.sqlContext

      def generateDataset[T: Encoder](data: List[T], columnNames: List[String]): Dataset[T] = {
        val memoryStream = MemoryStream[T]
        val memoryWriteQuery = memoryStream.toDF.writeStream.format("memory").queryName("memStream").start
        memoryStream.addData(data: _*)
        memoryWriteQuery.processAllAvailable()
        val dataset = spark.table("memStream").toDF(columnNames: _*).as[T]
        memoryWriteQuery.stop()
        dataset.show(20, false)
        dataset.printSchema()
        dataset
      }

      val inputDS: Dataset[String] = generateDataset(
        List(json1, json2, json3, json4, json5, json6, json7, json8, json9, json10),
        List("value")
      )

      val targetDS: Dataset[(String, String, String, String, String)] = generateDataset(
        List(tuple1, tuple2, tuple3, tuple4, tuple5, tuple6, tuple7, tuple8, tuple9, tuple10),
        List("id", "msg", "time", "lang", "user")
      )

      val outputDS = KafkaConsumer.defineFlow(inputDS, spark)
      outputDS.show(20, false)
      outputDS.printSchema()

      assert(outputDS.schema == targetDS.schema)
      assertSmallDatasetEquality(outputDS, targetDS)
    }
  }
}