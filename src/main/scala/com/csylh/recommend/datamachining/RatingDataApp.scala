package com.csylh.recommend.datamachining

import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Description:
  *  数据加工:  数据切分成训练集和测试集数据 6:4
  *
  *   为模型准备训练的RDD数据集
  *
  *
  * @Author: 留歌36
  * @Date: 2019-07-17 14:55
  */
object RatingDataApp {
  def main(args: Array[String]): Unit = {
    val localMasterURL = "local[2]"
//    val clusterMasterURL = "spark://hadoop001:7077"

    // 面向SparkSession编程
    val spark = SparkSession.builder().appName("RatingDataApp")
//      .master(localMasterURL)
      .enableHiveSupport().getOrCreate()

    //开启访问Hive数据, 要将hive-site.xml等文件放入Spark的conf路径

    /**
      *     1. RDD[UserRating]需要从原始表中提取userid,movieid,rating数据
      *     2. 并把这些数据切分成训练集和测试集数据 6:4  越靠近最近时间段的数据作为测试数据集
      *     3. 调用cache table tableName即可将一张表缓存到内存中，来极大的提高查询效率
      */
//    https://spark.apache.org/docs/latest/sql-performance-tuning.html
    spark.catalog.cacheTable("ratings")


    // 取第一行.first() 第一列元素.getLong(0)
    val count = spark.sql("select count(1) from ratings").first().getLong(0)
    val percent = 0.6
    val trainingdatacount = (count * percent).toInt  // 训练数据集大小
    val testdatacount = (count * (1 - percent)).toInt  // 测试数据集大小

    // 1.将数据按时间 升序排序,order by limit的时候，需要注意OOM(Out Of Memory)的问题: 全表扫描+limit
    val trainingDataAsc = spark.sql(s"select userId,movieId,rating from ratings order by timestamp asc")
    // 2.数据写入到HDFS上  这一步可能出现OOM
    trainingDataAsc.write.mode(SaveMode.Overwrite).parquet("/tmp/trainingDataAsc")
    // 3.将数据加载到数据仓库中去
    spark.sql("drop table if exists trainingDataAsc")
    spark.sql("create table if not exists trainingDataAsc(userId int,movieId int,rating double) stored as parquet")
    spark.sql("load data inpath '/tmp/trainingDataAsc' overwrite into table trainingDataAsc")

    // 将数据按时间 降序排序
    val trainingDataDesc = spark.sql(s"select userId,movieId,rating from ratings order by timestamp desc")
    trainingDataDesc.write.mode(SaveMode.Overwrite).parquet("/tmp/trainingDataDesc")
    spark.sql("drop table if exists trainingDataDesc")
    spark.sql("create table if not exists trainingDataDesc(userId int,movieId int,rating double) stored as parquet")
    spark.sql("load data inpath '/tmp/trainingDataDesc' overwrite into table trainingDataDesc")


    /**
      * 1.获取60% 升序排列数据进行训练模型
      */
    val trainingData = spark.sql(s"select * from trainingDataAsc limit $trainingdatacount")
    trainingData.write.mode(SaveMode.Overwrite).parquet("/tmp/trainingData")
    spark.sql("drop table if exists trainingData")
    spark.sql("create table if not exists trainingData(userId int,movieId int,rating double) stored as parquet")
    spark.sql("load data inpath '/tmp/trainingData' overwrite into table trainingData")

    /**
      * 2.获取40% 降序排列数据进行测试模型
      */
    val testData = spark.sql(s"select * from trainingDataDesc limit $testdatacount")
    testData.write.mode(SaveMode.Overwrite).parquet("/tmp/testData")
    spark.sql("drop table if exists testData")
    spark.sql("create table if not exists testData(userId int,movieId int,rating double) stored as parquet")
    spark.sql("load data inpath '/tmp/testData' overwrite into table testData")


     val ratingRDD = spark.sql("select * from trainingData").rdd.map(x => Rating(x.getInt(0),x.getInt(1),x.getDouble(2)))
    // 测试构造模型
    // ratingRDD：数据集

    // rank：对应的是隐因子的个数，这个值设置越高越准，但是也会产生更多的计算量。一般将这个值设置为10-20
    //      隐因子：如一部电影，决定它评分的有导演、主演、特效和剧本4个隐因子

    // iterations：对应迭代次数，一般设置个10就够了；
    // lambda：该参数控制正则化过程，其值越高，正则化程度就越深。一般设置为0.01
     val model = ALS.train(ratingRDD, 1, 10)


  }

}














