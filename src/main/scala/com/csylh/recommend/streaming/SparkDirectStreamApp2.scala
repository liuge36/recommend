package com.csylh.recommend.streaming

import com.csylh.recommend.config.AppConf
import kafka.serializer.StringDecoder
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Description:  TODO 待调试...
  *
  * @Author: 留歌36
  * @Date: 2019/10/18 16:33
  */
object SparkDirectStreamApp2 extends AppConf{
    def main(args:Array[String]): Unit ={
      val ssc = new StreamingContext(sc, Seconds(3))

      val topics = "movie_topic".split(",").toSet

      val kafkaParams = Map[String, String](
        "metadata.broker.list"->"hadoop001:9093,hadoop001:9094,hadoop001:9095",
        "auto.offset.reset" -> "largest" //smallest :从头开始 largest：最新
      )
      // Direct 模式：SparkStreaming 主动去Kafka中pull拉数据
      val modelPath = "hdfs://hadoop001:8020/tmp/BestModel/0.8575484870109622"
      val stream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)


     val messages = stream.foreachRDD(rdd=> {
       // TODO  代码一: 思考这段代码为啥不行？
//        rdd.foreach(record => {
//          val user = record._2.split("|").apply(1).toInt
//          val model = MatrixFactorizationModel.load(sc, modelPath)
//          val recresult = model.recommendProducts(user, 5)
//          println(recresult)
//        })
       //  代码二:

            val model = MatrixFactorizationModel.load(sc, modelPath)

            rdd.foreachPartition(partitionOfRecords => {

              val userList = partitionOfRecords.map(_._2.split("|")).map(x=>x(1)).map(_.toInt)

              val validusers = userList.filter(userId => exist(userId))
              val newusers = userList.filter(userId => !exist(userId))

              // 采用迭代器的方式来避开对象不能序列化的问题。
              // 通过对RDD中的每个元素实时产生推荐结果，将结果写入到redis，或者其他高速缓存中，来达到一定的实时性。
              // 2个流的处理分成2个sparkstreaming的应用来处理。

              while (validusers.hasNext) {
                val recresult = model.recommendProducts(validusers.next, 5)
                println("以下电影为您推荐below movies are recommended for you :")
                println(recresult)
              }
              while (newusers.hasNext) {
                println("*新用户你好*以下电影为您推荐below movies are recommended for you :")
                for (i <- defaultrecresult) {
                  println(i.getString(0))
                }
              }



//              while (userList.hasNext){
//                  val userId = userList.next()
//                  // 判断用户是否是新用户
//
//                  if (exist(userId)){
//                    val recresult = model.recommendProducts(userId, 5)
//
//                    println("以下电影为您推荐below movies are recommended for you :")
//                    println(recresult)
//                  }else{
//
//                    println(" *新用户你好*以下电影为您推荐below movies are recommended for you :")
//                    recommendPopularMovies
//                  }
//              }

            })

     })

      ssc.start()
      ssc.awaitTermination()
    }


    def exist(u: Int): Boolean = {
      val trainingdataUserIdList = spark.sql("select distinct(userid) from trainingdata")
        .rdd
        .map(x => x.getInt(0)).collect() // RDD[row] ==> RDD[Int]
      // 查看传递进来的userId 是否在训练集中 ，看是否是老用户
      trainingdataUserIdList.contains(u)
    }


    // 为没有登录的用户推荐电影的策略：
    // 1.* 推荐观看人数较多的电影，这里采用这种策略
    // 2.推荐最新的电影
    def recommendPopularMovies()={
          spark.sql("select * from top5DF").show(false)
    }

    // 为没有登录的用户推荐电影的策略：
    // 1.推荐观看人数较多的电影，采用这种策略
    // 2.推荐最新的电影
    val defaultrecresult = spark.sql("select * from top5DF").rdd.toLocalIterator
}
