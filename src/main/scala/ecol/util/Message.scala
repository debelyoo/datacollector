package ecol.util

import java.util.Date
import java.util.UUID
import ecol.cassandra.model.Sensor

object Message {
  //case object Start
  case class InsertTemperature (ts: Date, sensor: Sensor, tempVal: String)
}