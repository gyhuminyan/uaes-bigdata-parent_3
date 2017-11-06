package com.uaes.spark.analysis.KPIhandle

import java.util.Properties

import com.uaes.spark.analysis.config.ConfigManager
import com.uaes.spark.analysis.utils.{DBUtils, SparkUtil}
import org.apache.spark.sql.SQLContext
import org.slf4j.LoggerFactory

/**
  * Created by hand on 2017/11/3.
  */
object AttrCurrent {
  val logger = LoggerFactory.getLogger(OtherFuel.getClass)
  lazy val url = ConfigManager.getConfig("mysql.url")
  lazy val username = ConfigManager.getConfig("mysql.user")
  lazy val password = ConfigManager.getConfig("mysql.password")
  lazy val table1 = "t_car_used_day"
  lazy val table2 = "t_car_used_km"

  def main(args: Array[String]): Unit = {

    val sc = SparkUtil.getSparkContext("OtherFuel", logger)
    val sqlContext = SQLContext.getOrCreate(sc)
    val prop = new Properties()
    val dDF = sqlContext.read.jdbc(url, table1, prop)
    val kmDF = sqlContext.read.jdbc(url, table2, prop)
    prop.put("user", username)
    prop.put("password", password)
    carFuel(sqlContext)
  }

  //  每天其他耗油
  def carFuel(sqlContext: SQLContext): Unit = {
    val kmTotalDF = sqlContext.sql("select VIN,\"C001\" as data_key,sum(DATA_VALUE) as DATA_VALUE FROM t_car_used_day where DATA_KEY=\"D005\" GROUP BY vin")
    val fuelTotalDF = sqlContext.sql("select VIN,\"C002\" as data_key,sum(DATA_VALUE) as DATA_VALUE FROM t_car_used_day where DATA_KEY=\"D001\" GROUP BY vin")
    val carRunFuelTotalDF = sqlContext.sql("select VIN,\"C003\" as data_key,sum(DATA_VALUE) as DATA_VALUE FROM t_car_used_day where DATA_KEY=\"D003\" GROUP BY vin")
    val waitSpeedFuelTotalDF = sqlContext.sql("select VIN,\"C004\" as data_key,sum(DATA_VALUE) as DATA_VALUE FROM t_car_used_day where DATA_KEY=\"D002\" GROUP BY vin")
    val airConFuelTotalDF = sqlContext.sql("select VIN,\"C005\" as data_key,sum(DATA_VALUE) as DATA_VALUE FROM t_car_used_day where DATA_KEY=\"D004\" GROUP BY vin")
    val everyKmFuelTotal = sqlContext.sql("select VIN,\"C006\" as data_key,sum(DATA_VALUE)/count(DATA_VALUE) as DATA_VALUE FROM t_car_used_km  where DATA_KEY=\"KM001\" GROUP BY vin")

    //保存到数据库
    val kmTotalRDD = kmTotalDF.rdd.map(row => {
      val vin = row.getString(0)
      val date = row.getString(1)
      val totalFuel = row.getString(2).toDouble
      (date, vin, totalFuel)
    })
    val fuelTotalRDD = kmTotalDF.rdd.map(row => {
      val vin = row.getString(0)
      val date = row.getString(1)
      val totalFuel = row.getString(2).toDouble
      (date, vin, totalFuel)
    })
    val carRunFuelTotalRDD = kmTotalDF.rdd.map(row => {
      val vin = row.getString(0)
      val date = row.getString(1)
      val totalFuel = row.getString(2).toDouble
      (date, vin, totalFuel)
    })
    val waitSpeedFuelTotalRDD = kmTotalDF.rdd.map(row => {
      val vin = row.getString(0)
      val date = row.getString(1)
      val totalFuel = row.getString(2).toDouble
      (date, vin, totalFuel)
    })
    val airConFuelTotalRDD = kmTotalDF.rdd.map(row => {
      val vin = row.getString(0)
      val date = row.getString(1)
      val totalFuel = row.getString(2).toDouble
      (date, vin, totalFuel)
    })
    val everyKmFuelRDD = kmTotalDF.rdd.map(row => {
      val vin = row.getString(0)
      val date = row.getString(1)
      val totalFuel = row.getString(2).toDouble
      (date, vin, totalFuel)
    })
    DBUtils.saveAttrCurrentToDB(kmTotalRDD)
    DBUtils.saveAttrCurrentToDB(fuelTotalRDD)
    DBUtils.saveAttrCurrentToDB(carRunFuelTotalRDD)
    DBUtils.saveAttrCurrentToDB(waitSpeedFuelTotalRDD)
    DBUtils.saveAttrCurrentToDB(airConFuelTotalRDD)
    DBUtils.saveAttrCurrentToDB(everyKmFuelRDD)
  }
}
