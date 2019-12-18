package com.csylh.recommend.utils

import com.csylh.recommend.entity.Links
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Description: 数据的预处理
  *com.csylh.recommend.dataclearer.SourceDataETLDemo1
  *
  * @Author: 留歌36
  * @Date: 2019-07-17 10:08
  */
object SourceDataETLDemo1 {
//  System.setProperty("hadoop.home.dir", "C:\\hadoop")
  def main(args: Array[String]): Unit = {
    val localMasterURL = "local[2]"
    val clusterMasterURL = "spark://hadoop001:7077"

    val conf = new SparkConf().setMaster(clusterMasterURL).setAppName("SourceDataETLDemo1")
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)
    val hiveContext = new HiveContext(sc)

    import sqlContext.implicits._
//    file:///root/data/ml/ml-latest/links.csv
    val links = sc.textFile("file:///root/data/ml/ml-latest/links.txt").filter(!_.endsWith(",")).map(_.split(",")).map(x => Links(x(0).trim.toInt, x(1).trim.toInt, x(2).trim.toInt)).toDF()

    links.show(false)
//    println(links.count())


  }

}
