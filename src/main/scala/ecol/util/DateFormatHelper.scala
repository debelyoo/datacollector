package ecol.util

import java.text.SimpleDateFormat

object DateFormatHelper {
	val dayFormatter = new SimpleDateFormat("yyyyMMdd")
    val hourFormatter = new SimpleDateFormat("HH")
	val dateTimeFormatter = new SimpleDateFormat("yyyyMMdd-HHmmss")
	val temperatureRowKeyFormatter = new SimpleDateFormat("yyyyMMdd-HH")
}