package ecol.util

import java.text.SimpleDateFormat

object DateFormatHelper {
	val dayFormatter = new SimpleDateFormat("yyyyMMdd")
    val hourFormatter = new SimpleDateFormat("HH")
	val minSecFormatter = new SimpleDateFormat("mmss")
	val dateTimeFormatter = new SimpleDateFormat("yyyyMMdd-HHmmss")
	val temperatureRowKeyFormatter = new SimpleDateFormat("yyyyMMdd-HH")
}