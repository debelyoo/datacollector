package ecol.cassandra.composite

import com.netflix.astyanax.annotations.Component
import java.util.UUID
import spray.json._

/**
 * Defines the composite type for the columns in temperature CF
 */
class TemperatureEvent(@Component(ordinal=0) ts: UUID, @Component(ordinal=1) sensorAddress: String) {

  /**
   * Default constructor - hack for Java compatibility with Astyanax lib
   * http://stackoverflow.com/questions/4045863/scala-extra-no-arg-constructor-plus-default-constructor-parameters
   */
  def this() = this(null, null)
  
  def getTs: UUID = ts 
  def getSensorAddress: String = sensorAddress
}