package com.csylh.recommend.config

import java.util.Properties

import org.apache.spark.sql.SparkSession

/**
  * Description: TODO
  *
  * @Author: 留歌36
  * @Date: 2019-07-17 16:53
  */
trait AppConf {
  val localMasterURL = "local[2]"
  val clusterMasterURL = ""

  // 面向SparkSession编程
  val spark = SparkSession.builder()
//    .master(localMasterURL)
    .enableHiveSupport() //开启访问Hive数据, 要将hive-site.xml等文件放入Spark的conf路径
    .getOrCreate()

  val sc = spark.sparkContext

  val minPartitions = "8"
  //  在生产环境中一定要注意设置spark.sql.shuffle.partitions，默认是200,及需要配置分区的数量
  spark.sqlContext.setConf("spark.sql.shuffle.partitions",minPartitions)

  //jdbc连接
  val jdbcURL = "jdbc:mysql://hadoop001:3306/recommend?useUnicode=true&characterEncoding=UTF-8&useSSL=false"

  val alsTable = "recommend.alsTab"
  val recResultTable = "recommend.similarTab"
  val top5Table = "recommend.top5Result"
  val userTable= "recommend.user"
  val ratingTable= "recommend.rating"

  val mysqlusername = "root"
  val mysqlpassword = "P@ssw0rd"

  val prop = new Properties
  prop.put("driver", "com.mysql.jdbc.Driver")
  prop.put("user", mysqlusername)
  prop.put("password", mysqlpassword)

}
