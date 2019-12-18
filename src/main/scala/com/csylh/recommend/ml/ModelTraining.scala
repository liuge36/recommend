package com.csylh.recommend.ml

import com.csylh.recommend.config.AppConf
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.rdd.RDD

/**
  * Description: 第四节课
  * 训练多个模型，取其中最好，即取RMSE(均方根误差)值最小的模型
  *
  * @Author: 留歌36
  * @Date: 2019-07-17 16:56
  */
object ModelTraining extends AppConf {
  def main(args: Array[String]): Unit = {

    // 训练集，总数据集的60%
    val trainingData = spark.sql("select * from trainingData")
    // 测试集，总数据集的40%
    val testData = spark.sql("select * from testData")


    //--------------------------
    // 训练集，转为Rating格式
    val ratingRDD = trainingData.rdd.map(x => Rating(x.getInt(0), x.getInt(1), x.getDouble(2)))
    // 用于计算模型的RMSE      Rating(userid, movieid, rating)   ==>转为tuple  (userid, movieid)
    val training2 :RDD[(Int,Int)]  = ratingRDD.map{ case Rating(userid, movieid, rating) => (userid, movieid)}

    // 测试集，转为Rating格式
    val testRDD = testData.rdd.map(x => Rating(x.getInt(0), x.getInt(1), x.getDouble(2)))
    val test2 :RDD[((Int,Int),Double)]= testRDD.map {case Rating(userid, movieid, rating) => ((userid, movieid), rating)}
    //--------------------------


    // 特征向量的个数
    val rank = 1
    // 正则因子
    // val lambda = List(0.001, 0.005, 0.01, 0.015)
    val lambda = List(0.001, 0.005, 0.01)
    // 迭代次数
    val iteration = List(10, 12, 16)
    var bestRMSE = Double.MaxValue

    var bestIteration = 0
    var bestLambda = 0.0

    // persist可以根据情况设置其缓存级别
    ratingRDD.persist() // 持久化放入内存，迭代中使用到的RDD都可以持久化
    training2.persist()

    test2.persist()
    for (l <- lambda; i <- iteration) {
      // 循环收敛这个模型
      //lambda 用于表示过拟合的这样一个参数，值越大，越不容易过拟合，但精确度就低
      val model = ALS.train(ratingRDD, rank, i, l)

      //---------这里是预测-----------------
      val predict = model.predict(training2).map {
        // 根据 (userid, movieid) 预测出相对应的rating
        case Rating(userid, movieid, rating) => ((userid, movieid), rating)
      }
      //-------这里是实际的predictAndFact-------------------

      // 根据(userid, movieid)为key，将提供的rating与预测的rating进行比较
      val predictAndFact = predict.join(test2)

      // 计算RMSE(均方根误差)
      val MSE = predictAndFact.map {
        case ((user, product), (r1, r2)) =>
          val err = r1 - r2
          err * err
      }.mean()  // 求平均

      val RMSE = math.sqrt(MSE) // 求平方根

      // RMSE越小，代表模型越精确
      if (RMSE < bestRMSE) {
        // 将模型存储下来
        model.save(sc, s"/tmp/BestModel/$RMSE")
        bestRMSE = RMSE
        bestIteration = i
        bestLambda = l
      }


      println(s"Best model is located in /tmp/BestModel/$RMSE")
      println(s"Best RMSE is $bestRMSE")
      println(s"Best Iteration is $bestIteration")
      println(s"Best Lambda is $bestLambda")


    }
  }
}

