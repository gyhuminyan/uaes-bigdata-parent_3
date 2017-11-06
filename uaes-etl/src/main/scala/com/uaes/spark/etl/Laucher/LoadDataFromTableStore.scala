package com.uaes.spark.etl.Laucher

import java.util

import com.alicloud.openservices.tablestore.SyncClient
import com.alicloud.openservices.tablestore.model._
import com.aliyun.openservices.tablestore.hadoop._
import com.uaes.spark.common.utils.CarDataUtils
import com.uaes.spark.etl.config.ConfigManager
import com.uaes.spark.etl.service.HdfsService
import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory



/**
  * Created by Flink on 2017/10/20.
  * 从TableStore中读取数据
  */
object LoadDataFromTableStore {

  lazy val runMode = ConfigManager.getConfig("spark.run_mode")
  lazy val master = ConfigManager.getConfig("spark.master")
  lazy val endPoint = ConfigManager.getConfig("tableStore.endPoint")
  lazy val accessKeyId = ConfigManager.getConfig("tableStore.accessKeyId")
  lazy  val accessKeySecret = ConfigManager.getConfig("tableStore.accessKeySecret")
  lazy val instanceName = ConfigManager.getConfig("tableStore.instanceName")
  lazy val tableName = ConfigManager.getConfig("tableStore.tableName")
  lazy val securityToken = ConfigManager.getConfig("tableStore.securityToken")
  final lazy val PRIMARY_KEY_NAME_1 = "VIN"
  final lazy val PRIMARY_KEY_NAME_2 = "DATE"


  def main(args: Array[String]) {
    val logger = LoggerFactory.getLogger(LoadDataFromTableStore.getClass)
    val conf = new SparkConf().setAppName("LoadDataFromTableStore")
    runMode match {
      case "local" => conf.setMaster("local[*]")
      case "yarn" => conf.setMaster("yarn-client")
      case "spark" => conf.setMaster(master)
      case _ => {
        logger.error("unknown spark run mode,system exit with code -1,please check your config file")
        System.exit(-1)
      }
    }

    val sc = new SparkContext(conf)
    val hadoopConf = new Configuration()
    val cred = new Credential(accessKeyId, accessKeySecret, securityToken)
    try {
      TableStore.setCredential(hadoopConf, cred)
      val ep = new Endpoint(endPoint, instanceName)
      TableStore.setEndpoint(hadoopConf, ep)
      TableStoreInputFormat.addCriteria(hadoopConf, fetchCriteria(args(0), args(1)))
      val strRDD = sc.newAPIHadoopRDD(hadoopConf, classOf[TableStoreInputFormat],
        classOf[PrimaryKeyWritable],
        classOf[RowWritable])
        .map(row => row._2.getRow.getColumn("DATA").toString)
      // 1.原始数据保存到HDFS
      new HdfsService().save("hdfs://emr-header-1.cluster-50949:9000/uaes/etl_date/", strRDD, "TS")
      val etlRDD = strRDD.flatMap(line => CarDataUtils.transform(line))
      //2.经过etl之后保存到HDFS上的数据
      new HdfsService().save("hdfs://emr-header-1.cluster-50949:9000/uaes/etl_date/", strRDD, "timestamp")


//      val lines = strRDD.map(line => line._1.getPrimaryKey.toString +  "  #_#   " + line._2.getRow.toString)
//      lines.collect().foreach(row => println(row))

    }finally {
      if (sc != null){
        sc.stop()
      }
    }

  }

  // 初始化tablestore连接
  private def getOTSClient(): SyncClient = {
    var ep = null.asInstanceOf[Endpoint]
    if (instanceName == null){
        ep = new Endpoint(endPoint)
    }else{
        ep = new Endpoint(endPoint, instanceName)
    }
    val cred = new Credential(accessKeyId, accessKeySecret, securityToken)
    if (cred.securityToken == null) {
      new SyncClient(endPoint, cred.accessKeyId, cred.accessKeySecret, instanceName)
    }else{
      new SyncClient(endPoint, cred.accessKeyId, cred.accessKeySecret, instanceName, cred.securityToken)
    }
  }

  //tablestore 表的元数据管理
  private def fetchTableMeta(): TableMeta = {
    val ots = getOTSClient()
    var resp = null.asInstanceOf[DescribeTableResponse]
    try {
      resp = ots.describeTable(new DescribeTableRequest(tableName))
    }finally {
      ots.shutdown()
    }
    resp.getTableMeta
  }


  //table中进行整行查询
  private def fetchCriteria(startPkValue: String, endPkValue: String): RangeRowQueryCriteria = {
    val meta :TableMeta = fetchTableMeta()
    val res = new RangeRowQueryCriteria(tableName)
    res.setMaxVersions(1)
    val lower = new util.ArrayList[PrimaryKeyColumn]()
    val upper = new util.ArrayList[PrimaryKeyColumn]()

      lower.add(new PrimaryKeyColumn(PRIMARY_KEY_NAME_1, PrimaryKeyValue.INF_MIN))
      upper.add(new PrimaryKeyColumn(PRIMARY_KEY_NAME_1, PrimaryKeyValue.INF_MAX))
      lower.add(new PrimaryKeyColumn(PRIMARY_KEY_NAME_2, PrimaryKeyValue.fromString(startPkValue)))
      upper.add(new PrimaryKeyColumn(PRIMARY_KEY_NAME_2, PrimaryKeyValue.fromString(endPkValue)))
//    }
    res.setInclusiveStartPrimaryKey(new PrimaryKey(lower))
    res.setExclusiveEndPrimaryKey(new PrimaryKey(upper))
    res
  }


  // TableStore中按时间主键进行间隔为一天的范围查询
  private def getRangeByIterator(startPkValue: String, endPkValue: String): RangeIteratorParameter = {
    // TableStore初始化连接
    val client = getOTSClient()
    val timeRangeIterator = new RangeIteratorParameter(tableName)
    //设置开始主键
    val startKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder()
    startKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_1, PrimaryKeyValue.INF_MIN)
    startKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_2, PrimaryKeyValue.fromString(startPkValue))
    timeRangeIterator.setInclusiveStartPrimaryKey(startKeyBuilder.build())

    //设置结束主键
    val endKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder()
    endKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_1, PrimaryKeyValue.INF_MAX)
    endKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_2, PrimaryKeyValue.fromString(endPkValue))
    timeRangeIterator.setExclusiveEndPrimaryKey(endKeyBuilder.build())

    timeRangeIterator.setMaxVersions(1)
    val iterator = client.createRangeIterator(timeRangeIterator)
    println("使用Iterator进行GetRange的结果为:")
    while (iterator.hasNext) {
      val row: Row = iterator.next()
      println(row)
    }
    timeRangeIterator
  }

}
