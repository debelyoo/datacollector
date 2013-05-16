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

object AstyanaxConnector extends UUIDHelper {
  
  var keyspace: Keyspace = null
  val temperatureSerializer = new AnnotatedCompositeSerializer[TemperatureEvent](classOf[TemperatureEvent])
  
  val CF_TEMPERATURE = new ColumnFamily[String, TemperatureEvent](
        "temperature",			// cf name
        StringSerializer.get(),	// key serializer
        temperatureSerializer 	// column serializer
        )

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
  }
  
  def insertTemperature(tempValue: String): Boolean = {  
    val m = keyspace.prepareMutationBatch()
    val now = new Date()
    val t1 = now.getTime()
	val dateStr = new SimpleDateFormat("yyyyMMdd-HH").format(now)
	val uuid = uuidForDate(now)
	val t3 = uuidTsToDateTs(uuid.timestamp())
	//println("t1: "+t1+", t3: "+t3)
	val tempEvent = new TemperatureEvent(uuid, "address1")
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
    val rowKey = "20130516-17"
    val result = keyspace.prepareQuery(CF_TEMPERATURE)
    .getRow(rowKey)
    .execute()
    val cols = result.getResult()
    val it = cols.iterator()
    while (it.hasNext()) {
      val col = it.next()
      val colName = col.getName()
      val colVal = col.getStringValue()
      val valueDate = new Date(uuidTsToDateTs(colName.getTs.timestamp()))
      val valueDateStr = new SimpleDateFormat("yyyyMMdd-HHmmss").format(valueDate)
      println("row: "+rowKey+", col: ["+ valueDateStr + ", "+ colVal +"]")
    }
    temps.toList
  }
  
  def getTemperatureByTimeRange: List[TemperatureLog] = {
    val temps = ListBuffer[TemperatureLog]()
    val rowKey = "20130516-17"
      val uuid1 = uuidForDate(new SimpleDateFormat("yyyyMMdd-HHmmss").parse("20130516-175000"))
      val uuid2 = uuidForDate(new SimpleDateFormat("yyyyMMdd-HHmmss").parse("20130516-175900"))
    val result = keyspace.prepareQuery(CF_TEMPERATURE)
    .getKey(rowKey)
    .withColumnRange(
      temperatureSerializer.makeEndpoint(uuid1, Equality.EQUAL).toBytes(),
      temperatureSerializer.makeEndpoint(uuid2, Equality.LESS_THAN_EQUALS).toBytes(),
      false, 100)
    .execute()
    val cols = result.getResult()
    val it = cols.iterator()
    while (it.hasNext()) {
      val col = it.next()
      val colName = col.getName()
      val colVal = col.getStringValue()
      val valueDate = new Date(uuidTsToDateTs(colName.getTs.timestamp()))
      val valueDateStr = new SimpleDateFormat("yyyyMMdd-HHmmss").format(valueDate)
      println("row: "+rowKey+", col: ["+ valueDateStr + ", "+ colVal +"]")
      temps += TemperatureLog(valueDateStr, colName.getSensorAddress, colVal)
    }
    temps.toList
  }
}