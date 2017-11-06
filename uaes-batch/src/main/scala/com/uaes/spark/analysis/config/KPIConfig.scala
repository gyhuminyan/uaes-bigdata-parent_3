package com.uaes.spark.analysis.config

/**
  * Created by mzhang on 2017/11/2.
  */
object KPIConfig {

  val fuelTotalDay = "D001" //累计耗油量

  val waitSpeedFuelDay = "D002" //怠速耗油率

  val carRunFuelDay = "D003" //正常行驶耗油率

  val AirConFuelDay = "D004" //空调耗油率

  val otherFuelDay = "D005" //其它耗油

  val mileageDay = "D006" // 当日行驶里程

  val fuelTotalKM = "KM001" //累计耗油量

  val waitSpeedFuelKM = "KM002" //怠速耗油率

  val carRunFuelKM = "KM003" //正常行驶耗油率

  val AirConFuelKM = "KM004" //空调耗油率

  val otherFuelKM = "KM005" //其它耗油

  val totalMileage = "C001" //累计行驶里程

  val totalFuel = "C002" //累计用油量

  val totalCarRunFuel = "C003" //行驶耗油

  val totalWaitSpeedFuel = "C004" //怠速耗油

  val totalAirConFuel = "C005" //空调耗油

  val fuelPer100 = "C006" //百公里耗油

  val avgSpeed = "FF001" //	平均速度

  val fuelHeat = "FF002" //	燃油热值

  val econRank = "FF003" //	经济型排名

}
