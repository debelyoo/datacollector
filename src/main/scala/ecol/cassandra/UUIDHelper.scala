package ecol.cassandra

import java.util.Date
import java.util.UUID

trait UUIDHelper {

  def uuidForDate(d: Date): UUID = {
	/*
	  http://stackoverflow.com/questions/15179428/it-is-possible-to-convert-uuid-to-date-using-java
	*/
    val NUM_100NS_INTERVALS_SINCE_UUID_EPOCH = 0x01b21dd213814000L
    val NUM_MS_INTERVALS_SINCE_UUID_EPOCH = 12219292800L

    val origTime = d.getTime()
    //val time = origTime * 10000 + NUM_100NS_INTERVALS_SINCE_UUID_EPOCH
    val time = (origTime + NUM_MS_INTERVALS_SINCE_UUID_EPOCH) * 10000
    val timeLow = time &       0xffffffffL
    val timeMid = time &   0xffff00000000L
    val timeHi = time & 0xfff000000000000L
    val upperLong = (timeLow << 32) | (timeMid >> 16) | (1 << 12) | (timeHi >> 48)
    new java.util.UUID(upperLong, 0xC000000000000000L)
  }
  
  def uuidTsToDateTs(ts: Long): Long = {
    val tsMilli = math.round(ts.toDouble / 10000.toDouble)
    tsMilli - 12219292800L
  }
  
  /*def getTimeUUID: UUID = {
    java.util.UUID.fromString(new com.eaio.uuid.UUID().toString())
  }*/
  
}