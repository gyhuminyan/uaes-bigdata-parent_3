package com.uaes.spark.analysis.KPIhandle

import java.util.Properties

import com.uaes.spark.analysis.config.ConfigManager
import com.uaes.spark.analysis.utils.{DBUtils, SparkUtil}
import org.apache.spark.sql.SQLContext
import org.slf4j.LoggerFactory

/**
  * Created by hand on 2017/10/29.
  *
  * 每天和每百公里的空调耗油量
  */
object OtherFuel {
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
    everyDayOtherFuel(sqlContext)
    everyHunKiOtherFuel(sqlContext)
  }

  //  每天其他耗油
  def everyDayOtherFuel(sqlContext: SQLContext): Unit = {
    val fuelDF = sqlContext.sql(
      "select a.vin," +
      "a.MILEAGE," +
      " 'other' as data_key," +
      "(a.data_value - b.data_value  - c.data_value - d.data_value) as data_value " +
      "from " +
      "(select * FROM t_car_used_km where DATA_KEY=\"D001\") a " +
      "JOIN (select * FROM t_car_used_km where DATA_KEY=\"D002\") b on a.vin = b.vin and a.MILEAGE = b.MILEAGE " +
      "JOIN(select * FROM t_car_used_km where DATA_KEY=\"D003\") c  on a.vin = c.vin and a.MILEAGE = c.MILEAGE " +
      "JOIN(select * FROM t_car_used_km where DATA_KEY=\"D004\") d  on a.vin = d.vin and a.MILEAGE = d.MILEAGE"
    )

    //保存到数据库
    val resRDD = fuelDF.rdd.map(row => {
      val vin = row.getString(0)
      val date = row.getString(1)
      val KPI = row.getString(2)
      val totalFuel = row.getString(3).toDouble
      (date, vin, KPI, totalFuel)
    })
    DBUtils.saveDayResultToDB(resRDD)
  }

  //每百公里其他耗油
  def everyHunKiOtherFuel(sqlContext: SQLContext): Unit = {
    val fuelDF = sqlContext.sql("select a.vin," +
      "a.now_date," +
      " 'other' as data_key," +
      "(a.data_value - b.data_value  - c.data_value - d.data_value) as data_value " +
      "from " +
      "(select * FROM t_car_used_day where DATA_KEY=\"D001\") a " +
      "JOIN (select * FROM t_car_used_day where DATA_KEY=\"D002\") b on a.vin = b.vin and a.now_date = b.now_date " +
      "JOIN(select * FROM t_car_used_day where DATA_KEY=\"D003\") c  on a.vin = c.vin and a.now_date = c.now_date " +
      "JOIN(select * FROM t_car_used_day where DATA_KEY=\"D004\") d  on a.vin = d.vin and a.now_date = d.now_date")

    //保存到数据库
    val resRDD = fuelDF.rdd.map(row => {
      val vin = row.getString(0)
      val km = row.getInt(1)
      val KPI = row.getString(2)
      val totalFuel = row.getString(3).toDouble
      (vin, km, KPI, totalFuel)
    })
    DBUtils.saveMileageResultToDB(resRDD)
  }
}
