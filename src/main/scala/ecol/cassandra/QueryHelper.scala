package ecol.cassandra

import java.util.{Calendar, Date}
import ecol.util.DateFormatHelper._
import scala.collection.mutable.ListBuffer

trait QueryHelper {

  /**
   * Get the row keys to query to match the specified time range (sample key: 20130516-08)
   * @param timeRange The time range to query
   * @return The list of row keys for this time range
   */
  protected def getRowKeysForRange(timeRange: Option[(Date, Date)]): Seq[String] = {
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
