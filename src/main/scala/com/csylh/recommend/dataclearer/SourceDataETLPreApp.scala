package com.csylh.recommend.dataclearer

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.log4j._

/**
  * Description:  留歌版   <===Spark2.X  版本
  *
  * 数据预处理
  * 1.下载数据集
  *   http://files.grouplens.org/datasets/movielens/
  *   movieId| imdbId|tmdbId
  * 2.
  * //    spark.read.option("header","true").csv("data2/links.csv")
  * val csv_data = spark.read.csv("file:///D:/java_workspace/fun_test.csv") //本地文件
  * val csv_data = spark.read.csv("hdfs:///tmp/fun_test.csv") //HDFS文件
  *
  *
  * 58098
  * 57917
  *  181
  *
  * @Author: 留歌36
  * @Date: 2019-07-17 10:53
  */
object SourceDataETLPreApp {
//  Logger.getLogger("org").setLevel(Level.ERROR)
//  System.setProperty("hadoop.home.dir", "C:\\hadoop")
  def main(args: Array[String]): Unit = {
    val localMasterURL = "local[2]"
    val clusterMasterURL = "spark://hadoop001:7077"

    // 面向SparkSession编程
    val spark = SparkSession.builder()
      .appName("SourceDataETLPreApp")
      .master(clusterMasterURL)
      .enableHiveSupport() // 开启访问Hive数据, 要将hive-site.xml等文件放入Spark的conf路径
      .getOrCreate()

    val minPartitions = "8"
    //  在生产环境中一定要注意设置spark.sql.shuffle.partitions，默认是200,及需要配置分区的数量
    spark.sqlContext.setConf("spark.sql.shuffle.partitions",minPartitions)

    val links = spark.read.format("csv")
      .option("header","true")
      .load("data2/links.csv")
      .filter("tmdbId is not null")


    // 把数据写入到HDFS上
    links.coalesce(1) //设置为一个partition, 这样可以把输出文件合并成一个文件
      .write
      .mode(SaveMode.Overwrite)
      .format("parquet")
      .save("'/tmp/links2'")

    // 将数据加载到数据仓库中去
    spark.sql("drop table if exists links")
    spark.sql("create table if not exists links(movieId int,imdbId int,tmdbId int) stored as parquet")
    spark.sql("load data inpath '/tmp/links2' overwrite into table links")

    // 关闭
    spark.stop()
  }

}
