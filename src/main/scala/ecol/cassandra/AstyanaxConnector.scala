package ecol.cassandra

import com.netflix.astyanax.AstyanaxContext
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl
import com.netflix.astyanax.connectionpool.NodeDiscoveryType
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor
import com.netflix.astyanax.thrift.ThriftFamilyFactory
import com.netflix.astyanax.Keyspace
import com.netflix.astyanax.model.ColumnFamily
import com.netflix.astyanax.serializers._
import java.util.Date
import java.text.SimpleDateFormat
import java.util.UUID
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException
import scala.collection.mutable.ListBuffer
import com.netflix.astyanax.model.Equality
import scala.collection.JavaConversions._
import ecol.Boot
import akka.actor.Props
import akka.actor.ActorRef
import java.util.Calendar
import ecol.util.DateFormatHelper._

object AstyanaxConnector extends UUIDHelper {
  
  var keyspace: Keyspace = null
  var insertWorker: ActorRef = null
  val temperatureSerializer = new AnnotatedCompositeSerializer[TemperatureEvent](classOf[TemperatureEvent])  
  val CF_TEMPERATURE = new ColumnFamily[String, TemperatureEvent](
        "temperature",			// cf name
        StringSerializer.get(),	// key serializer
        temperatureSerializer 	// column serializer
        )

  /**
   * Initializes the connection to Cassandra database
   */
  def init() {
    val context = new AstyanaxContext.Builder()
    .forCluster("TestCluster")
    .forKeyspace("catssandra")
    .withAstyanaxConfiguration(new AstyanaxConfigurationImpl()      
        .setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE)
    )
    .withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl("MyConnectionPool")
        .setPort(9160)
        .setMaxConnsPerHost(1)
        .setSeeds("127.0.0.1:9160")
    )
    .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
    .buildKeyspace(ThriftFamilyFactory.getInstance())

    context.start()
    keyspace = context.getEntity()
    insertWorker = Boot.system.actorOf(Props[InsertWorker], name = "insertWorker")
  }
  
  /**
   * Insert a temperature in database
   * @param tempDate The date of the temperature's measure
   * @param sensorAddress The address of the sensor
   * @param tempValue The temperature value
   * @return true if success 
   */
  def insertTemperature(tempDate: Date, sensorAddress: String, tempValue: String): Boolean = {  
    val m = keyspace.prepareMutationBatch()
    //val now = new Date()
    //val t1 = tempDate.getTime()
	val dateStr = temperatureRowKeyFormatter.format(tempDate)
	val uuid = uuidForDate(tempDate)
	//val t3 = uuidTsToDateTs(uuid.timestamp())
	//println("t1: "+t1+", t3: "+t3)
	val tempEvent = new TemperatureEvent(uuid, sensorAddress)
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
  
  def getTemperature: List[String] = {
    val temps = ListBuffer[String]()
    val dayStr = new SimpleDateFormat("yyyyMMdd").format(new Date())
    val rowKey = "20130517-02"
    val result = keyspace.prepareQuery(CF_TEMPERATURE)
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
        val valueDateStr = new SimpleDateFormat("yyyyMMdd-HHmmss").format(valueDate)
        println("row: "+rowKey+", col: ["+ valueDateStr + ", "+ colVal +"]")
        temps += colVal
      }
      
    }
    temps.toList
  }
  
  /**
   * Get the temperatures by time range and sensor address
   * @param timeRange The range of time
   * @param sensorAddresses The list of addresses of the sensors to get
   */
  def getTemperatureByTimeRangeAndSensor(
      timeRange: Option[(Date, Date)] = Some(new Date(), new Date()), 
      sensorAddresses: Option[Seq[String]] = None): List[TemperatureLog] = {
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
    val result = keyspace.prepareQuery(CF_TEMPERATURE)
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
        temps += TemperatureLog(valueDateStr, colName.getSensorAddress, colVal)
      }
    }
    
    sensorAddresses.map(addressList => temps.toList.filter(tl => addressList.contains(tl.sensorAddress))).getOrElse(temps.toList)
  }
  
  /**
   * Get the row keys to query to match the specified time range (sample key: 20130516-08)
   * @param timeRange The time range to query
   * @return The list of row keys for this time range
   */
  def getRowKeysForRange(timeRange: Option[(Date, Date)]): Seq[String] = {
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