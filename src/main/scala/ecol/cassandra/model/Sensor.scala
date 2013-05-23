package ecol.cassandra.model

import com.netflix.astyanax.model.ColumnFamily
import java.util.UUID
import com.netflix.astyanax.serializers._
import ecol.cassandra.AstyanaxConnector
import spray.json.DefaultJsonProtocol
import spray.json.RootJsonFormat
import spray.json._
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException

/**
 * The object representing a sensor connected to the Compactrio
 * name: the name of the sensor
 * address: the address of the sensor (in compactrio)
 */
case class Sensor(name: String, address: String)

object SensorJsonProtocol extends DefaultJsonProtocol {
  implicit val sensorFormat = jsonFormat2(Sensor)
}