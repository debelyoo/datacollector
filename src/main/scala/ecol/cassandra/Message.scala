package ecol.cassandra

import java.util.Date

object Message {
  //case object Start
  case class InsertTemperature (ts: Date, sensorAddress: String, tempVal: String)
}