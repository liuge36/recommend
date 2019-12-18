package com.csylh.recommend.streaming

import com.csylh.recommend.config.AppConf
import kafka.serializer.StringDecoder
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Description: 
  *
  * @Author: 留歌36
  * @Date: 2019/10/18 16:33
  */
object SparkDirectStreamApp extends AppConf{
    def main(args:Array[String]): Unit ={
      val ssc = new StreamingContext(sc, Seconds(5))

      val topics = "movie_topic".split(",").toSet

      val kafkaParams = Map[String, String](
        "metadata.broker.list"->"hadoop001:9093,hadoop001:9094,hadoop001:9095",
        "auto.offset.reset" -> "largest" //smallest :从头开始 largest：最新
      )
      // Direct 模式：SparkStreaming 主动去Kafka中pull拉数据
      val modelPath = "hdfs://hadoop001:8020/tmp/BestModel/0.8575484870109622"

      val stream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)

      def exist(u: Int): Boolean = {
        val trainingdataUserIdList = spark.sql("select distinct(userid) from trainingdata")
          .rdd
          .map(x => x.getInt(0))
          .collect()  // RDD[row] ==> RDD[Int]

        trainingdataUserIdList.contains(u)
      }

      // 为没有登录的用户推荐电影的策略：
      // 1.推荐观看人数较多的电影，采用这种策略
      // 2.推荐最新的电影
      val defaultrecresult = spark.sql("select * from top5DF").rdd.toLocalIterator
      println("#################################################")
      // 创建SparkStreaming接收kafka消息队列数据的2种方式
      // 一种是Direct approache,通过SparkStreaming自己主动去Kafka消息队
      // 列中查询还没有接收进来的数据，并把他们拉pull到sparkstreaming中。

      val model = MatrixFactorizationModel.load(ssc.sparkContext, modelPath)
      val messages = stream.foreachRDD(rdd=> {

              val userIdStreamRdd = rdd.map(_._2.split("|")).map(x=>x(1)).map(_.toInt)

              val validusers = userIdStreamRdd.filter(userId => exist(userId))
              val newusers = userIdStreamRdd.filter(userId => !exist(userId))

              // 采用迭代器的方式来避开对象不能序列化的问题。
              // 通过对RDD中的每个元素实时产生推荐结果，将结果写入到redis，或者其他高速缓存中，来达到一定的实时性。
              // 2个流的处理分成2个sparkstreaming的应用来处理。
              val validusersIter = validusers.toLocalIterator
              val newusersIter = newusers.toLocalIterator

              while (validusersIter.hasNext) {
                val u= validusersIter.next
                println("userId"+u)
                val recresult = model.recommendProducts(u, 5)
                val recmoviesid = recresult.map(_.product)
                println("我为用户" + u + "【实时】推荐了以下5部电影：")
                for (i <- recmoviesid) {
                  val moviename = spark.sql(s"select title from movies where movieId=$i").first().getString(0)
                  println(moviename)
                }
              }

              while (newusersIter.hasNext) {
                println("*新用户你好*以下电影为您推荐below movies are recommended for you :")
                for (i <- defaultrecresult) {
                  println(i.getString(0))
                }
              }


     })
      ssc.start()
      ssc.awaitTermination()
    }
}
