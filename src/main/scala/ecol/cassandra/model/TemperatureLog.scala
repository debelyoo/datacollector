package ecol.cassandra.model

import spray.json.DefaultJsonProtocol
import java.util.Date
import ecol.cassandra.AstyanaxConnector
import ecol.cassandra.UUIDHelper
import ecol.util.DateFormatHelper._
import scala.collection.mutable.ListBuffer
import ecol.cassandra.composite.TemperatureEvent
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException
import com.netflix.astyanax.serializers._
import scala.collection.JavaConversions._
import com.netflix.astyanax.model.ColumnFamily
import com.netflix.astyanax.model.Equality
import java.util.Calendar
import spray.json.RootJsonFormat
import spray.json._
import java.util.UUID
import ecol.cassandra.model.SensorJsonProtocol._

/**
 * The object representing a temperature record (timestamp, sensor, temperature value)
 */
case class TemperatureLog(ts: String, sensor: Sensor, temperature: String)

object TemperatureLog extends UUIDHelper {
  
  val temperatureSerializer = new AnnotatedCompositeSerializer[TemperatureEvent](classOf[TemperatureEvent])  
  val CF_TEMPERATURE = new ColumnFamily[String, TemperatureEvent](
        "temperature",			// cf name
        StringSerializer.get(),	// key serializer
        temperatureSerializer 	// column serializer (composite)
        )
  
  /**
   * Insert a temperature in database
   * @param tempDate The date of the temperature's measure
   * @param sensorId The id of the sensor
   * @param tempValue The temperature value
   * @return true if success 
   */
  def insertTemperature(tempDate: Date, sensor: Sensor, tempValue: String): Boolean = {  
    val m = AstyanaxConnector.keyspace.prepareMutationBatch()
	val dateStr = temperatureRowKeyFormatter.format(tempDate)
	val uuid = uuidForDate(tempDate)
	//val t3 = uuidTsToDateTs(uuid.timestamp())
	val tempEvent = new TemperatureEvent(uuid, sensor.name, sensor.address) // composite column
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
  def getTemperatureByTimeRangeAndSensor(
      timeRange: Option[(Date, Date)] = Some(new Date(), new Date()), 
      sensorAddresses: Option[Seq[String]] = None): List[TemperatureLog] = {
    try {
	    val temps = ListBuffer[TemperatureLog]()
	    //val rowKey = "20130516-17"
	    val dayStr = dayFormatter.format(new Date())
	    val rowKeys = getRowKeysForRange(timeRange)
	    //println(rowKeys)
	    val rangeMin = rowKeys.head + minSecFormatter.format(timeRange.get._1) // yyyyMMdd-HHmmss
	    val rangeMax = rowKeys.last +  minSecFormatter.format(timeRange.get._2)
	    println("min: "+rangeMin+", max: "+rangeMax)
	    val uuid1 = uuidForDate(dateTimeFormatter.parse(rangeMin))
	    val uuid2 = uuidForDate(dateTimeFormatter.parse(rangeMax))
	    val result = AstyanaxConnector.keyspace.prepareQuery(CF_TEMPERATURE)
	    .getKeySlice(rowKeys)
	    .withColumnRange(
	      temperatureSerializer.makeEndpoint(uuid1, Equality.EQUAL).toBytes(),
	      temperatureSerializer.makeEndpoint(uuid2, Equality.LESS_THAN_EQUALS).toBytes(),
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
    } catch {
      case ae: AssertionError => List() // TODO - remove
    }
  }
  
  /**
   * Get the row keys to query to match the specified time range (sample key: 20130516-08)
   * @param timeRange The time range to query
   * @return The list of row keys for this time range
   */
  private def getRowKeysForRange(timeRange: Option[(Date, Date)]): Seq[String] = {
    if (timeRange.isEmpty) Seq() else {
      val (date1, date2) = timeRange.get
      val day1 = dayFormatter.format(date1)
      var incDay = day1
      val day2 = dayFormatter.format(date2)
      val minHourDay1 = hourFormatter.format(date1).toInt
      val maxHourDay2 = hourFormatter.format(date2).toInt
      val days = ListBuffer[String]()
      if (day1 == day2) {
        days += day1
      } else {
        val calendar = Calendar.getInstance()
        calendar.setTime(date1)
        days += incDay
        while(incDay != day2) {
          calendar.add(Calendar.DAY_OF_YEAR, 1)
          incDay = dayFormatter.format(calendar.getTime())
          days += incDay
        }
      }
      val rowKeys = days.toList.map(day => 
        for {
          i <- 0 to 23
          if ((day != day1 && day != day2) || 
              (day1 != day2 && ((i >= minHourDay1 && day == day1) || (i <= maxHourDay2 && day == day2))) ||
               (day1 == day2 && i >= minHourDay1 && i <= maxHourDay2))
        } yield {
          //val hourOffset = 8
          day + "-" + "%02d".format(i)
        }
      ).flatten
      rowKeys.toSeq
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
      "temperature" -> JsString(tl.temperature)
    )

    def read(value: JsValue) = {
      value.asJsObject.getFields("timestamp", "sensor", "temperature") match {
        case Seq(JsString(ts), sensor, JsString(temperature)) =>
          TemperatureLog(ts, sensor.convertTo[Sensor], temperature)
          //TemperatureLog(ts, Sensor(sensorJs.get("name").get.convertTo[String],sensorJs.get("address").get.convertTo[String]), temperature)
        case _ => throw new DeserializationException("TemperatureLog expected")
      }
    }
  }
}