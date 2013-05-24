package ecol.cassandra.model

import ecol.cassandra.{QueryHelper, AstyanaxConnector, UUIDHelper}
import com.netflix.astyanax.serializers.{StringSerializer, AnnotatedCompositeSerializer}
import ecol.cassandra.composite.{ColumnTimestampStringString}
import com.netflix.astyanax.model.{Equality, ColumnFamily}
import java.util.Date
import ecol.util.DateFormatHelper._
import scala.collection.JavaConversions._
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException
import scala.collection.mutable.ListBuffer
import spray.json._
import scala.Some
import ecol.cassandra.model.SensorJsonProtocol._

case class CompassLog(ts: String, sensor: Sensor, value: String)

object CompassLog extends UUIDHelper with QueryHelper {

  val compositeSerializer = new AnnotatedCompositeSerializer[ColumnTimestampStringString](classOf[ColumnTimestampStringString])
  val CF_COMPASS = new ColumnFamily[String, ColumnTimestampStringString](
    "compass",			// cf name
    StringSerializer.get(),	// key serializer
    compositeSerializer 	// column serializer (composite)
  )

  /**
   * Insert a compass value in database
   * @param valueDate The date of the compass's measure
   * @param sensor The sensor who took the measure
   * @param compassValue The temperature value
   * @return true if success
   */
  def insertValue(valueDate: Date, sensor: Sensor, compassValue: String): Boolean = {
    val m = AstyanaxConnector.keyspace.prepareMutationBatch()
    val dateStr = rowKeyDayHourFormatter.format(valueDate)
    val uuid = uuidForDate(valueDate)
    //val t3 = uuidTsToDateTs(uuid.timestamp())
    val tempEvent = new ColumnTimestampStringString(uuid, sensor.name, sensor.address) // composite column
    m.withRow(CF_COMPASS, dateStr)
      .putColumn(tempEvent, compassValue, null)

    try {
      val result = m.execute()
      println("data inserted ! ["+ dateStr +"]")
      true
    } catch {
      case ce: ConnectionException => ce.printStackTrace();false
      case ex: Exception => ex.printStackTrace(); false
    }
  }

  /**
   * Get the compass values by time range
   * @param timeRange The range of time
   * @return The list of compass value objects
   */
  private def getCompassValueByTimeRangeAndSensor(timeRange: Option[(Date, Date)] = Some(new Date(), new Date())): List[CompassLog] = {
    val vals = ListBuffer[CompassLog]()
    val rowKeys = getRowKeysForRange(timeRange)
    //println(rowKeys)
    val rangeMin = rowKeys.head + minSecFormatter.format(timeRange.get._1) // yyyyMMdd-HHmmss
    val rangeMax = rowKeys.last +  minSecFormatter.format(timeRange.get._2)
    //println("min: "+rangeMin+", max: "+rangeMax)
    val uuid1 = uuidForDate(dateTimeFormatter.parse(rangeMin))
    val uuid2 = uuidForDate(dateTimeFormatter.parse(rangeMax))
    val result = AstyanaxConnector.keyspace.prepareQuery(CF_COMPASS)
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
        vals += CompassLog(valueDateStr, Sensor(colName.getSensorName, colName.getSensorAddress), colVal)
      }
    }

    vals.toList
  }

  /**
   * Request compass logs
   * @param startTime The start time of the query window
   * @param endTime The end time of the query window
   * @return A list with the compass logs (in an option)
   */
  def requestCompassValues(startTime: String, endTime: String): Option[List[CompassLog]] = {
    if (startTime.length() != 15 || endTime.length() != 15)
      None
    else {
      val date1 = dateTimeFormatter.parse(startTime)
      val date2 = dateTimeFormatter.parse(endTime)
      val temps = getCompassValueByTimeRangeAndSensor(timeRange = Some((date1, date2)))
      Some(temps)
    }
  }
}

object CompassLogJsonProtocol extends DefaultJsonProtocol {
  implicit object CompassLogJsonFormat extends RootJsonFormat[CompassLog] {
    def write(tl: CompassLog) = JsObject(
      "timestamp" -> JsString(tl.ts),
      "sensor" -> tl.sensor.toJson,
      "value" -> JsString(tl.value)
    )

    def read(value: JsValue) = {
      value.asJsObject.getFields("timestamp", "sensor", "value") match {
        case Seq(JsString(ts), sensor, JsString(value)) =>
          CompassLog(ts, sensor.convertTo[Sensor], value)
        case _ => throw new DeserializationException("TemperatureLog expected")
      }
    }
  }
}