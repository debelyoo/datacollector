package ecol.cassandra

import com.netflix.astyanax.AstyanaxContext
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl
import com.netflix.astyanax.connectionpool.NodeDiscoveryType
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor
import com.netflix.astyanax.thrift.ThriftFamilyFactory
import com.netflix.astyanax.Keyspace
import ecol.Boot
import akka.actor.Props
import akka.actor.ActorRef
import ecol.util.InsertWorker

object AstyanaxConnector {
  
  var keyspace: Keyspace = null
  var insertWorker: ActorRef = null
  

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
  
}