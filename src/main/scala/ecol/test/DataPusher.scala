package ecol.test

import scala.util.Random._
import ecol.cassandra.AstyanaxConnector
import java.util.Date
import java.text.SimpleDateFormat
import ecol.util.Message

class DataPusher {
  val NB_VALUES = 1000
  
  def pushSampleData {
    for (i <- 1 to NB_VALUES) {
      val now = new Date()
      val day = new SimpleDateFormat("yyyyMMdd").format(now)
      val randomHour = nextInt(24)
      val randomMinute = nextInt(60)
      val dayHourMin = day + "-" + "%2d".format(randomHour) + "%2d".format(randomMinute)
      val tempDate = new SimpleDateFormat("yyyyMMdd-HHmm").parse(dayHourMin)
      val temp = nextDouble() * 20
      val tempStr = "%.3f".format(temp)
      val addressId = nextInt(10)
      //println(tempStr +" - "+ addressId)
      AstyanaxConnector.insertWorker ! Message.InsertTemperature(tempDate, "address" + addressId, tempStr)
    }
  }
}