package com.uaes.spark.etl.config

import com.typesafe.config.ConfigFactory

/**
  * Created by Flink on 2017/10/20.
  */
object ConfigManager {
    lazy val conf = ConfigFactory.load("app.conf")
    def getConfig(item: String): String = {
        conf.getString(item)
    }

//    def main(args: Array[String]) {
//        val app_name = ConfigManager.getConfig("spark.app_name")
//        print(app_name)
//    }

}
