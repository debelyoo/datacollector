package ecol.cassandra

/*import com.twitter.cassie.Cluster
import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.cassie.Keyspace
import com.twitter.cassie.ColumnFamily
import com.twitter.cassie.codecs.Utf8Codec
import com.twitter.cassie.codecs.LexicalUUIDCodec
import java.util.Date
import java.text.SimpleDateFormat
import com.twitter.cassie.clocks.Clock
import com.twitter.cassie.types.LexicalUUID
import com.twitter.cassie.Column
*/

object CassieConnector {
  /*var cluster: Cluster = null
  var keyspace: Keyspace = null

  def init() {
    cluster = new Cluster("localhost", NullStatsReceiver)
	keyspace = cluster.keyspace("catssandra").connect()
  }
  
  def insertTemperature(tempValue: String): Boolean = {
	val cf = keyspace.columnFamily("temperature", Utf8Codec, LexicalUUIDCodec, Utf8Codec) // cf name, row name format, col name format, col value format
	val now = new Date()
	val dateMinStr = new SimpleDateFormat("yyyyMMdd-HHmm").format(now)
	val clock = new Clock {
	  def timestamp = now.getTime()
	}
	val uuid = LexicalUUID(clock)
	val fRes = cf.insert(dateMinStr, Column(uuid, tempValue)) // row name, col name, col value
	// TODO handle future
	true
  }*/
  
  
}