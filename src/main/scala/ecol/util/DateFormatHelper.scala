package ecol.util

import java.text.SimpleDateFormat
import java.util.Date

object DateFormatHelper {
	val dayFormatter = new SimpleDateFormat("yyyyMMdd")
    val hourFormatter = new SimpleDateFormat("HH")
	val minSecFormatter = new SimpleDateFormat("mmss")
	val dateTimeFormatter = new SimpleDateFormat("yyyyMMdd-HHmmss")
	val rowKeyDayHourFormatter = new SimpleDateFormat("yyyyMMdd-HH")
	
	/**
	 * Convert a labview timestamp to Java Date (in time zone UTC+01:00)
	 * @param labviewTs The timestamp in labview time reference (nb of seconds from 1.1.1904) UTC
	 * @return The corresponding date in Java time reference
	 */
	def labViewTsToJavaDate(labviewTs: Double): Date = {
	  val labviewEpoch = dateTimeFormatter.parse("19040101-000000") // 1.1.1904
	  val newTs = labviewEpoch.getTime() + math.round(labviewTs * 1000) + (3600 * 1000) // add one hour for the time zone (UTC+01:00)
	  new Date(newTs)
	}
}