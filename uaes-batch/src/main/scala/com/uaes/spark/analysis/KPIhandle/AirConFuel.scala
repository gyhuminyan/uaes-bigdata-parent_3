package com.uaes.spark.analysis.KPIhandle

import com.alibaba.fastjson.{JSON, JSONObject}
import com.uaes.spark.analysis.utils._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.slf4j.LoggerFactory

/**
  * Created by hand on 2017/10/25.
  *
  * 每天以及每百公里的空调耗油量
  */
object AirConFuel {
  val logger = LoggerFactory.getLogger(AirConFuel.getClass)

  def main(args: Array[String]): Unit = {
    val sc = SparkUtil.getSparkContext("WaitSpeedFuel", logger)
    val rdd = sc.textFile("H:/UAES/TestData.txt")

    val (startTime ,endTime) = TimeUtils.currentTime(args)

    val filterEverydayRdd = rdd.filter(line => {
      val jobj = JSON.parseObject(line)
      val dateTime = jobj.get("timestamp").toString
      jobj.get("stype").equals("airConditioningState") || //空调状态
        jobj.get("stype").equals("InsFuelInjection") || //瞬时喷油量
        (dateTime >= startTime && dateTime <= endTime)
    })
    val filterEveryHunKilRdd = rdd.filter(line => {
      val jobj = JSON.parseObject(line)
      val daeTime = jobj.get("timestamp").toString
      jobj.get("stype").equals("airConditioningState") || //空调状态
        jobj.get("stype").equals("InsFuelInjection") || //瞬时喷油量
        jobj.get("stype").equals("drivingMileage")  //行驶里程
    })
    everyDayAirConFuel(filterEverydayRdd)
//    everyHunKiAirConFuel(filterEveryHunKilRdd)
  }

  //每天空调耗油
  def everyDayAirConFuel(rDD: RDD[String]): Unit = {
    val jsonRDD = rDD.map(line => {
      val jobj = JSON.parseObject(line)
      val date = jobj.getString("timestamp").substring(0,14)
      val vin = jobj.getString("VIN")
      jobj.put("timestamp", date)
      (vin + "_" + date, jobj)
    }).groupByKey().map(pair => {
      val tmpObj = new JSONObject()
      val strs = pair._1.split("_")
      tmpObj.put("VIN", strs(0))
      tmpObj.put("timestamp", strs(1))
      tmpObj.put("airConditioningState", 0)
      tmpObj.put("InsFuelInjection", 0)
      var airState = 0
      var InsFuelInjection = 0.0
      for (jObj <- pair._2) {
        jObj.getString("stype") match {
          case "airConditioningState" => {
            airState += (if (jObj.getInteger("value") == 0) 0 else 1)
            tmpObj.put("airConditioningState", airState)
          }
          case "InsFuelInjection" => {
            InsFuelInjection += jObj.getString("value").toDouble
            tmpObj.put("InsFuelInjection", InsFuelInjection)
          }
        }
      }
      tmpObj.toJSONString
    })

    val sqlContext = SQLContext.getOrCreate(rDD.sparkContext)
    val df = sqlContext.read.json(jsonRDD)
    df.createTempView("car")

    val resDF = sqlContext.sql("select VIN,subString(timestamp,1,8) as date,sum(InsFuelInjection) as totalFuel " +
      "from car where airConditioningState=0 group by VIN,subString(timestamp,1,8)")
    //保存到数据库
    val resRDD = resDF.rdd.map(row => {
      val vin = row.getString(0)
      val date = row.getString(1).substring(0, 8)
      val KPI = "每天空调耗油"
      val airConFuelCoeff = AirconditioningFuelConsumptionUtil.getRateFromDB()
      val totalFuel = row.getDouble(2) * airConFuelCoeff.toDouble
      (vin,date, KPI, totalFuel)
    })
    DBUtils.saveDayResultToDB(resRDD)
  }

  //每百公里空调耗油
  def everyHunKiAirConFuel(rDD: RDD[String]): Unit = {
    val per100UtilsRdd = Per100Utils.getAfterLastPer100Data(rDD, "3")
    val jsonRDD = rDD.map(line => {
      val jobj = JSON.parseObject(line)
      val vin = jobj.getString("VIN")
      val date = jobj.getString("timestamp").substring(0,14)
      jobj.put("timestamp", date)
      (vin + "_" + date, jobj)
    }).groupByKey().map(pair => {
      val tmpObj = new JSONObject()
      val strs = pair._1.split("_")
      tmpObj.put("VIN", strs(0))
      tmpObj.put("timestamp", strs(1))
      tmpObj.put("drivingMileage", 0)
      tmpObj.put("airConditioningState", 0)
      tmpObj.put("InsFuelInjection", 0)
      var airConditioningState = 0.0
      var InsFuelInjection = 0.0
      var drivingMileage = 0.0
      for (jObj <- pair._2) {
        jObj.getString("stype") match {
          case "drivingMileage" => {
            val value = jObj.getString("value").toDouble / 100
            tmpObj.put("drivingMileage", value.toInt * 100)
          }
          case "InsFuelInjection" => {
            InsFuelInjection += jObj.getString("value").toDouble
            tmpObj.put("InsFuelInjection", InsFuelInjection)
          }
          case "airConditioningState" => {
            airConditioningState += jObj.getString("value").toDouble
            tmpObj.put("airConditioningState", airConditioningState)
          }
        }
      }
      tmpObj.toJSONString
    })

    val sqlContext = SQLContext.getOrCreate(rDD.sparkContext)
    val df = sqlContext.read.json(jsonRDD)
    df.createTempView("car")

    val resDF = sqlContext.sql("select VIN,drivingMileage,sum(InsFuelInjection) as totalFuel " +
      "from car where airConditioningState=0 group by VIN,drivingMileage")
    //保存到数据库
    val resRDD = resDF.rdd.map(row => {
      val vin = row.getString(0)
      val everyHunKil = row.getInt(1)
      val KPI = "每百公里空调耗油"
      val totalFuel = row.getDouble(2)
      (vin, everyHunKil, KPI, totalFuel)
    })
    DBUtils.saveMileageResultToDB(resRDD)
  }
}
