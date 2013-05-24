package ecol.util

import java.util.Date
import java.util.UUID
import ecol.cassandra.model.Sensor

object Message {
  //case object Start
  case class InsertTemperatureLog (ts: Date, sensor: Sensor, tempVal: String)
  case class InsertCompassLog (ts: Date, sensor: Sensor, compassVal: String)
}