package ecol.cassandra

import com.netflix.astyanax.annotations.Component
import java.util.UUID
import spray.json._

class TemperatureEvent(@Component(ordinal=0) ts: UUID, @Component(ordinal=1) sensorAddress: String) {

  /**
   * Default constructor - hack for Java compatibility with Astyanax lib
   * http://stackoverflow.com/questions/4045863/scala-extra-no-arg-constructor-plus-default-constructor-parameters
   */
  def this() = this(null, null)
  
  def getTs: UUID = ts 
  def getSensorAddress: String = sensorAddress
}

case class TemperatureLog(ts: String, sensorAddress: String, temperature: String) {
  /*def toJson: JsValue = {
    val jsStr = "{ \"timestamp\": \""+ ts +"\", \"sensor_address\": \""+ sensorAddress +"\", \"value\": \""+ temperature +"\" }"
    jsStr.asJson
  }*/
}

object TemperatureLogJsonProtocol extends DefaultJsonProtocol {
  implicit val temperatureLogFormat = jsonFormat3(TemperatureLog)
}