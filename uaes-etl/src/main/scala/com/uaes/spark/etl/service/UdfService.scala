package com.uaes.spark.etl.service

/**
  * Created by Flink on 2017/10/23.
  */
import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.SQLContext


class UdfService extends Serializable {
  final val format = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
  final val dateFomat = new SimpleDateFormat("yyyy-MM-dd")
  final val calendar = Calendar.getInstance()
  val getStringDate = (date: String) => {
    val ps = format.parse(date)
    dateFomat.format(ps)
  }
  val getLongTime = (date: String) => {
    val ps = format.parse(date)
    val longTime = ps.getTime()
    longTime
  }
  val getDay = (date: String) => {

    val ps = format.parse(date)
    calendar.setTime(ps)
    val day = calendar.get(Calendar.DAY_OF_MONTH).toLong
    day
  }
  val getMonth = (date: String) => {
    val ps = format.parse(date)
    calendar.setTime(ps)
    val month = calendar.get(Calendar.MONTH).toLong
    month + 1
  }
  val getDate = (year: Long, month: Long, date: Long) => {
    val buffer = new StringBuffer()
    buffer.append(year + "")
    if (month + "".length == 1) {
      buffer.append("-0" + month)
    } else {
      buffer.append("-" + month)
    }
    buffer.append("-")
    if (date + "".length == 1) {
      buffer.append("0" + date + "")
    } else {
      buffer.append(date + "")
    }
    buffer.toString
  }
  val getYear = (date: String) => {
    val ps = format.parse(date)
    calendar.setTime(ps)
    val year = calendar.get(Calendar.YEAR).toLong
    year
  }
  def register(map: Map[String, Function[String, Any]], sqlSc: SQLContext): Unit = {
    map.keySet.foreach(f => {
      val value = map.get(f).get
      value match {
        case `value` if value.isInstanceOf[Function[String, Long]] =>
          sqlSc.udf.register(f, value.asInstanceOf[Function[String, Long]])
      }
    })
  }

}
