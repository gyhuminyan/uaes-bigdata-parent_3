package com.uaes.spark.etl.service

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

/**
  * Created by Flink on 2017/10/24.
  */
class HdfsService extends Serializable {
  final val udfService = new UdfService()
  final var map = Map[String, Function[String, Any]]()
  map += ("getDay" -> udfService.getDay)
  map += ("getMonth" -> udfService.getMonth)
  map += ("getYear" -> udfService.getYear)


  def save(path: String, rdd: RDD[String], tsName: String): Unit = {
    val sqlSc = SQLContext.getOrCreate(rdd.sparkContext)

    val df = sqlSc.read.json(rdd)
    save(path, df, tsName)
  }


  def save(path: String, df: DataFrame, tsName: String): Unit = {
    udfService.register(map, df.sqlContext)
//    df.filter("timestamp is not null")
      df
      .withColumn("year", callUDF("getYear", col(tsName)))
      .withColumn("month", callUDF("getMonth", col(tsName)))
      .withColumn("day", callUDF("getDay", col(tsName)))
      .repartition(col("year"), col("month"), col("day"))
      .write.partitionBy("year", "month", "day")
      .mode(SaveMode.Append)
      .json(path)

  }
}
