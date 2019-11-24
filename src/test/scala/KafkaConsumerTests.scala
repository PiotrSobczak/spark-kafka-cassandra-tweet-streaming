import com.holdenkarau.spark.testing.StreamingSuiteBase
import com.piosobc.sparkstreamingtweets.KafkaConsumer
import org.apache.spark.streaming.dstream.DStream
import org.scalatest._
import org.scalatest.Matchers._


class TestCase(val _json: String, val _tuple: List[String]) {
  var tuple = List(List(_json))
  var json = List(List(_tuple))
}

class KafkaConsumerTests extends WordSpec with StreamingSuiteBase {

  "KafkaConsumer" should {
    "map json to tuples" in  {


//      case class TestCase(json: String, tuple: (String, String, String, String, Double))

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

      val tuple1 = List("Je crois que je suis amoureuse","coiffeuse vie🥀🌹","fr","Sat Nov 23 23:19:12 CET 2019","1198365408786604035")
      val tuple2 = List("https://t.co/RXJOu9ofcd https://t.co/fdiwFiqFLK","Guaglia","und","Sat Nov 23 23:19:12 CET 2019","1.19836540879075328E18")
      val tuple3 = List("Con que ahora Belinda usa a los comandantes e influencias del alto mando del Presidente con el que amablemente colaboró en campaña para amedrentar a todo aquel periodista que hable de su relación con el moreno luchón de Lupillo. O sea no entiendo le hemos conocido peores cosas . https://t.co/txRQ6rW1qQ","Economía Moral ®","es","Sat Nov 23 23:19:12 CET 2019","1.19836540876565299E18")
      val tuple4 = List("Putin Benzema il a encore marquer c’est trop wlh","LaMentalee 🏴‍☠️","fr","Sat Nov 23 23:19:13 CET 2019","1.19836541295570944E18")
      val tuple5 = List("RT @lita_9_6: ตอนชั้นโดนแหก มีคนบอกว่าอิแด๊ดก็ไม่เห็นจะอะไร อวยเว่อ เยไม่มันส์บ้างอะไรบ้าง อันนี้ไม่เคืองนะคนที่เคืองคืออิแด๊ด555555","NattyThaiELF. ต้องสอบผ่านB1","th","Sat Nov 23 23:19:12 CET 2019","1.19836540877398016E18")
      val tuple6 = List("VIVI PRA VER O FLAMENGO SER CAMPEÃO DA LIBERTADORESSSSSS","ᴀɴɴʏ","pt","Sat Nov 23 23:19:12 CET 2019","1.19836540877821542E18")
      val tuple7 = List("@ketaminepura el modo!!!!!","💀🧚‍♀️💀🧚‍♀️💀🧚‍♀️🧚‍♀️💀🧚‍♀️💀🧚‍♀️🧚‍♀️","es","Sat Nov 23 23:19:12 CET 2019","1.19836540875720294E18")
      val tuple8 = List("@todobakuu I’m a Pisces of trash","Lady Gugu","en","Sat Nov 23 23:19:12 CET 2019","1.19836540876138496E18")
      val tuple9 = List("RT @DianAngel01: https://t.co/9w4pMPeHy6","SebastianAristizabal","und","Sat Nov 23 23:19:12 CET 2019","1.19836540877821542E18")
      val tuple10 = List("RT @RyanKGives: Should I giveaway a FAST $500 to someone?","ICEDriver","en","Sat Nov 23 23:19:12 CET 2019","1.19836540876558336E18")

      val testCases : List[TestCase] = List(
        new TestCase(json1, tuple1), new TestCase(json2, tuple2), new TestCase(json3, tuple3),
        new TestCase(json4, tuple4), new TestCase(json5, tuple5), new TestCase(json6, tuple6),
        new TestCase(json7, tuple7), new TestCase(json8, tuple8), new TestCase(json9, tuple9),
        new TestCase(json10, tuple10)
      )

      testOperation(List(List(json1)), KafkaConsumer.extractData _, List(List(tuple1)), ordered=false)
//      testCases.foreach { x => testOperation(x.json, KafkaConsumer.extractData _, x.tuple, ordered=false)}
    }
  }
}