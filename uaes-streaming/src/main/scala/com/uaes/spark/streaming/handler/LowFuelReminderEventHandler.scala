package com.uaes.spark.streaming.handler

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.uaes.spark.streaming.service.HttpService
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.DStream

class LowFuelReminderEventHandler(dStream: DStream[String]) extends BaseHandler(dStream) {

  val url = "http://uaes.test.tunnel.echomod.cn/pub/v1/common/push/pushNoticeTmp"
  def handle(): Unit = {

    val filtedDS = dStream.filter(line => {
      val jObj = JSON.parseObject(line)
      jObj.getString("stype").equals("fuelLevel")
    }).map(line => {
      val jObj = JSON.parseObject(line)
      val vin = jObj.getString("VIN")
      val value = jObj.getString("value").toDouble
      (vin, (value, 1))
    })

    filtedDS.reduceByKeyAndWindow((a, b) => {
      (a._1 + b._1, a._2 + b._2)
    }, (a, b) => {
      (a._1 - b._1, a._2 - b._2)
    }, Seconds(30), Seconds(10))
      .map(pair => {
        val avgFuel = pair._2._1 / pair._2._2
        (pair._1, avgFuel)
      })
      .foreachRDD(rdd => {
        rdd.foreachPartition(partition => {
          val httpService = new HttpService(url)
          partition.foreach(pair => {
            if (pair._2 < 10) {
              //发送http消息
              sendToHttp(pair, httpService)
            }
          })
        })
      })
  }

  def sendToHttp(pari : (String ,Double), httpService: HttpService): Unit = {
    val jObj = new JSONObject()
    jObj.put("msgType", "lowFuel")
    jObj.put("vin", pari._1)

    val arrJSon = new JSONArray()
    val attrJObj = new JSONObject()
    attrJObj.put("type", "currentFuel")
    attrJObj.put("value", pari._2)
    arrJSon.add(attrJObj)
    jObj.put("msgPara", arrJSon)

    httpService.sendHttpRequest(jObj.toString)
  }
}
