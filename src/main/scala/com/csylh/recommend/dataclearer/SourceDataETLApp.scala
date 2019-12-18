package com.csylh.recommend.dataclearer

import com.csylh.recommend.entity.{Links, Movies, Ratings, Tags}
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Description:  讲师正式版 <==视频位置：一直到数据加工1
  *    hadoop001 下的文件  ===>   Hive数据仓库   ===>
  *
  * @Author: 留歌36
  * @Date: 2019-07-12 13:48
  */
object SourceDataETLApp {
//  System.setProperty("hadoop.home.dir", "C:\\hadoop")
  def main(args: Array[String]): Unit = {
    val localMasterURL = "local[2]"
    val clusterMasterURL = "spark://hadoop001:7077"

    val conf = new SparkConf().setAppName("SourceDataETLApp").setMaster(clusterMasterURL)
    // 得到SparkContext
    val sc = new SparkContext(conf)
    // 得到SqlContext
    val sqlContext = new SQLContext(sc)
    // 得到hiveContext
    val hiveContext = new HiveContext(sc)

    import sqlContext.implicits._

    // 设置RDD的partitions 的数量一般以集群分配给应用的CPU核数的整数倍为宜， 4核8G ，设置为8就可以
    // 问题一：为什么设置为CPU核心数的整数倍？
    // 问题二：数据倾斜，拿到数据大的partitions的处理，会消耗大量的时间，因此做数据预处理的时候，需要考量会不会发生数据倾斜
    val minPartitions = 8

    /**
      * 1
      */
    val links = sc.textFile("file:///root/data/ml/ml-latest/links.txt",minPartitions) //DRIVER
      .filter(!_.endsWith(",")) //EXRCUTER
      .map(_.split(",")) //EXRCUTER
      .map(x => Links(x(0).trim.toInt, x(1).trim.toInt, x(2).trim.toInt)) //EXRCUTER
      .toDF()
    println("===============links===================:",links.count())

    links.show()

    // 把数据写入到HDFS上
    links.write.mode(SaveMode.Overwrite).parquet("/tmp/links")

    // 将数据从HDFS加载到Hive数据仓库中去
    hiveContext.sql("drop table if exists links")
    hiveContext.sql("create table if not exists links(movieId int,imdbId int,tmdbId int) stored as parquet")
    hiveContext.sql("load data inpath '/tmp/links' overwrite into table links")

    /**
      * 2
      */
    val movies = sc.textFile("file:///root/data/ml/ml-latest/movies.txt",minPartitions)
      .filter(!_.endsWith(","))
      .map(_.split(","))
      .map(x => Movies(x(0).trim.toInt, x(1).trim.toString, x(2).trim.toString))
      .toDF()

    println("===============movies===================:",movies.count())
    movies.show()

    // 把数据写入到HDFS上
    movies.write.mode(SaveMode.Overwrite).parquet("/tmp/movies")

    // 将数据从HDFS加载到Hive数据仓库中去
    hiveContext.sql("drop table if exists movies")
    hiveContext.sql("create table if not exists movies(movieId int,title String,genres String) stored as parquet")
    hiveContext.sql("load data inpath '/tmp/movies' overwrite into table movies")

    /**
      * 3
      */
    val ratings = sc.textFile("file:///root/data/ml/ml-latest/ratings.txt",minPartitions)
      .filter(!_.endsWith(","))
      .map(_.split(","))
      .map(x => Ratings(x(0).trim.toInt, x(1).trim.toInt, x(2).trim.toDouble, x(3).trim.toInt))
      .toDF()
    println("===============ratings===================:",ratings.count())

    ratings.show()

    // 把数据写入到HDFS上
    ratings.write.mode(SaveMode.Overwrite).parquet("/tmp/ratings")

    // 将数据从HDFS加载到Hive数据仓库中去
    hiveContext.sql("drop table if exists ratings")
    hiveContext.sql("create table if not exists ratings(userId int,movieId int,rating Double,timestamp int) stored as parquet")
    hiveContext.sql("load data inpath '/tmp/ratings' overwrite into table ratings")

    /**
      * 4
      */
    val tags = sc.textFile("file:///root/data/ml/ml-latest/tags.txt",minPartitions)
      .filter(!_.endsWith(","))
      .map(x => rebuild(x))  // 注意这个坑的解决思路
      .map(_.split(","))
      .map(x => Tags(x(0).trim.toInt, x(1).trim.toInt, x(2).trim.toString, x(3).trim.toInt))
      .toDF()

    println("===============tags===================:",tags.count())
    tags.show()

    // 把数据写入到HDFS上
    tags.write.mode(SaveMode.Overwrite).parquet("/tmp/tags")

    // 将数据从HDFS加载到Hive数据仓库中去
    hiveContext.sql("drop table if exists tags")
    hiveContext.sql("create table if not exists tags(userId int,movieId int,tag String,timestamp int) stored as parquet")
    hiveContext.sql("load data inpath '/tmp/tags' overwrite into table tags")

  }

  /**
    * 该方法是用于处理不符合规范的数据
    * @param input
    * @return
    */
  private def rebuild(input:String): String ={
    val a = input.split(",")

    val head = a.take(2).mkString(",")
    val tail = a.takeRight(1).mkString
    val tag = a.drop(2).dropRight(1).mkString.replaceAll("\"","")
    val output = head + "," + tag + "," + tail
    output
  }

}
