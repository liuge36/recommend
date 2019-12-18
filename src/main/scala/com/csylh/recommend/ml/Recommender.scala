package com.csylh.recommend.ml

import com.csylh.recommend.config.AppConf
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel

/**
  * Description: 为某一用户产生推荐结果
  *
  * @Author: 留歌36
  * @Date: 2019-07-18 10:04
  */
object Recommender extends AppConf{
  def main(args: Array[String]): Unit = {
    // 从trainingData中取得的userid 一定存在于模型中。
    val users = spark.sql("select distinct(userId) from trainingData order by userId asc")
    // 取随意一个用户
    val index = 139
    val uid = users.take(index).last.getInt(0)

    val modelpath = "/tmp/BestModel/0.8575484870109622"
    val model = MatrixFactorizationModel.load(sc, modelpath)
    val rec = model.recommendProducts(uid, 5)

    val recmoviesid = rec.map(_.product)

    println("我为用户" + uid + "推荐了以下5部电影：")

    /**
      * 1
      */
    for (i <- recmoviesid) {
      val moviename = spark.sql(s"select title from movies where movieId=$i").first().getString(0)
      println(moviename)
    }

//    /**
//      * 2
//      */
//    recmoviesid.foreach(x => {
//      val moviename = spark.sql(s"select title from movies where movieId=$x").first().getString(0)
//      println(moviename)
//    })
//
//    /**
//      * 3
//      */
//    recmoviesid.map(x => {
//      val moviename = spark.sql(s"select title from movies where movieId=$x").first().getString(0)
//      println(moviename)
//    })



  }
}












