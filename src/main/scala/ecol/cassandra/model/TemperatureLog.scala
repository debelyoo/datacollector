package ecol.cassandra.model

import java.util.Date
import ecol.cassandra.{QueryHelper, AstyanaxConnector, UUIDHelper}
import ecol.util.DateFormatHelper._
import scala.collection.mutable.ListBuffer
import ecol.cassandra.composite.ColumnTimestampStringString
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException
import com.netflix.astyanax.serializers._
import scala.collection.JavaConversions._
import com.netflix.astyanax.model.ColumnFamily
import com.netflix.astyanax.model.Equality
import java.util.Calendar
import spray.json._
import ecol.cassandra.model.SensorJsonProtocol._

/**
 * The object representing a temperature record (timestamp, sensor, temperature value)
 */
case class TemperatureLog(ts: String, sensor: Sensor, value: String)

object TemperatureLog extends UUIDHelper with QueryHelper {
  
  val compositeSerializer = new AnnotatedCompositeSerializer[ColumnTimestampStringString](classOf[ColumnTimestampStringString])
  val CF_TEMPERATURE = new ColumnFamily[String, ColumnTimestampStringString](
        "temperature",			// cf name
        StringSerializer.get(),	// key serializer
        compositeSerializer 	// column serializer (composite)
        )
  
  /**
   * Insert a temperature in database
   * @param tempDate The date of the temperature's measure
   * @param sensor The sensor who took the measure
   * @param tempValue The temperature value
   * @return true if success 
   */
  def insertValue(tempDate: Date, sensor: Sensor, tempValue: String): Boolean = {
    val m = AstyanaxConnector.keyspace.prepareMutationBatch()
	val dateStr = rowKeyDayHourFormatter.format(tempDate)
	val uuid = uuidForDate(tempDate)
	//val t3 = uuidTsToDateTs(uuid.timestamp())
	val tempEvent = new ColumnTimestampStringString(uuid, sensor.name, sensor.address) // composite column
    m.withRow(CF_TEMPERATURE, dateStr)
    	.putColumn(tempEvent, tempValue, null)
    	
    try {
    	val result = m.execute()
    	println("data inserted ! ["+ dateStr +"]")
    	true
    } catch {
      case ce: ConnectionException => ce.printStackTrace();false
      case ex: Exception => ex.printStackTrace(); false
    }
  }
  
  /*def getTemperature: List[String] = {
    val temps = ListBuffer[String]()
    val dayStr = dayFormatter.format(new Date())
    val rowKey = "20130517-02"
    val result = AstyanaxConnector.keyspace.prepareQuery(CF_TEMPERATURE)
    .getRowSlice(Seq(rowKey))
    .execute()
    val rows = result.getResult()
    val rowIterator = rows.iterator()
    while (rowIterator.hasNext()) {
      val row = rowIterator.next()
      val columnIterator = row.getColumns().iterator()
      while (columnIterator.hasNext()) {
        val col = columnIterator.next()
        val colName = col.getName()
        val colVal = col.getStringValue()
        val valueDate = new Date(uuidTsToDateTs(colName.getTs.timestamp()))
        val valueDateStr = dateTimeFormatter.format(valueDate)
        println("row: "+rowKey+", col: ["+ valueDateStr + ", "+ colVal +"]")
        temps += colVal
      }
      
    }
    temps.toList
  }*/
  
  /**
   * Get the temperatures by time range and sensor address
   * @param timeRange The range of time
   * @param sensorAddresses The list of addresses of the sensors to get
   * @return The list of temperature log objects
   */
  private def getTemperatureByTimeRangeAndSensor(
      timeRange: Option[(Date, Date)] = Some(new Date(), new Date()), 
      sensorAddresses: Option[Seq[String]] = None): List[TemperatureLog] = {
    val temps = ListBuffer[TemperatureLog]()
    //val rowKey = "20130516-17"
    //val dayStr = dayFormatter.format(new Date())
    val rowKeys = getRowKeysForRange(timeRange)
    //println(rowKeys)
    val rangeMin = rowKeys.head + minSecFormatter.format(timeRange.get._1) // yyyyMMdd-HHmmss
    val rangeMax = rowKeys.last +  minSecFormatter.format(timeRange.get._2)
    //println("min: "+rangeMin+", max: "+rangeMax)
    val uuid1 = uuidForDate(dateTimeFormatter.parse(rangeMin))
    val uuid2 = uuidForDate(dateTimeFormatter.parse(rangeMax))
    val result = AstyanaxConnector.keyspace.prepareQuery(CF_TEMPERATURE)
    .getKeySlice(rowKeys)
    .withColumnRange(
      compositeSerializer.makeEndpoint(uuid1, Equality.EQUAL).toBytes(),
      compositeSerializer.makeEndpoint(uuid2, Equality.LESS_THAN_EQUALS).toBytes(),
      false, 100)
    .execute()
    val rows = result.getResult()
    val rowIterator = rows.iterator()
    while (rowIterator.hasNext()) {
      val row = rowIterator.next()
      val rowKey = row.getKey()
      val columnIterator = row.getColumns().iterator()
      while (columnIterator.hasNext()) {
        val col = columnIterator.next()
        val colName = col.getName()
        val colVal = col.getStringValue()
        val valueDate = new Date(uuidTsToDateTs(colName.getTs.timestamp()))
        val valueDateStr = dateTimeFormatter.format(valueDate)
        //println("row: "+rowKey+", col: ["+ valueDateStr + ", "+ colVal +"]")
        //val sensor = Sensor.getById(colName.getSensorId)
        //assert(sensor.isDefined, {println("Sensor is not defined ! ["+ colName.getSensorId +"]")})
        temps += TemperatureLog(valueDateStr, Sensor(colName.getSensorName, colName.getSensorAddress), colVal)
      }
    }

    sensorAddresses.map(addressList => temps.toList.filter(tl => addressList.contains(tl.sensor.address))).getOrElse(temps.toList)
  }

  /**
   * Request temperature logs
   * @param startTime The start time of the query window
   * @param endTime The end time of the query window
   * @param addresses The addresses to get (dash separated: addr1-addr2)
   * @return A list with the temperature logs (in an option)
   */
  def requestTemperatures(startTime: String, endTime: String, addresses: Option[String]): Option[List[TemperatureLog]] = {
    if (startTime.length() != 15 || endTime.length() != 15)
      None
    else {
      val date1 = dateTimeFormatter.parse(startTime)
      val date2 = dateTimeFormatter.parse(endTime)
      val addr = addresses.map(_.split("-").toSeq)
      val temps = getTemperatureByTimeRangeAndSensor(
        timeRange = Some((date1, date2)),
        sensorAddresses = addr
      )
      Some(temps)
    }
  }
}



/**
 * Convert The TemperatureLog class in JSON object
 */
/* JSON protocol for case class only (without companion object)
 * 
object TemperatureLogJsonProtocol extends DefaultJsonProtocol {
  implicit val temperatureLogFormat = jsonFormat3(TemperatureLog)
}
*/

object TemperatureLogJsonProtocol extends DefaultJsonProtocol {
  implicit object TemperatureLogJsonFormat extends RootJsonFormat[TemperatureLog] {
    def write(tl: TemperatureLog) = JsObject(
      "timestamp" -> JsString(tl.ts),
      "sensor" -> tl.sensor.toJson,
      "value" -> JsString(tl.value)
    )

    def read(value: JsValue) = {
      value.asJsObject.getFields("timestamp", "sensor", "value") match {
        case Seq(JsString(ts), sensor, JsString(value)) =>
          TemperatureLog(ts, sensor.convertTo[Sensor], value)
        case _ => throw new DeserializationException("TemperatureLog expected")
      }
    }
  }
}