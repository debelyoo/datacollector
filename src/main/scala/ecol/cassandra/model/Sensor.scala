package ecol.cassandra.model

import spray.json._

/**
 * The object representing a sensor connected to the Compactrio
 * name: the name of the sensor
 * address: the address of the sensor (in compactrio)
 */
case class Sensor(name: String, address: String)

object SensorJsonProtocol extends DefaultJsonProtocol {
  implicit val sensorFormat = jsonFormat2(Sensor)
}