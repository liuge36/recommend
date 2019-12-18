package com.csylh.recommend.ml

import com.csylh.recommend.config.AppConf
import com.csylh.recommend.entity.Result
import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation._
import org.apache.spark.sql.{SQLContext, SaveMode, SparkSession}

/**
  * Description: TODO
  *
  * @Author: 留歌36
  * @Date: 2019-07-18 10:42
  */
object RecommendForAllUsers extends AppConf{
  def main(args: Array[String]): Unit = {
    val users = spark.sql("select distinct(userId) from trainingData order by userId asc")
    // 取得所有的用户ID
    val allusers = users.rdd.map(_.getInt(0)).toLocalIterator

    // 方法1，可行，但是效率不高，一条条取
    val modelpath = "hdfs://hadoop001:8020/tmp/BestModel/0.8575484870109622"
    val model = MatrixFactorizationModel.load(sc, modelpath)
    while (allusers.hasNext) {
      val rec = model.recommendProducts(allusers.next(), 5)  // 得到Array[Rating]
      writeRecResultToMysql(rec, spark, sc)
      // writeRecResultToSparkSQL(rec)，写入到SPARK-SQL(DataFrame)+hive，同ETL。
      // writeRecResultToHbase(rec, sqlContext, sc)
    }

    // 方法2，不可行，因为一次将矩阵表全部加载到内存，消耗资源太大
    // val recResult = model.recommendProductsForUsers(5)

    def writeRecResultToMysql(uid: Array[Rating], spark: SparkSession, sc: SparkContext) {
      val uidString = uid.map(x => x.user.toString() + ","
        + x.product.toString() + "," + x.rating.toString())

      import spark.implicits._
      val uidDFArray = sc.parallelize(uidString)
      val uidDF = uidDFArray.map(_.split(",")).map(x => Result(x(0).trim().toInt, x(1).trim.toInt, x(2).trim().toDouble)).toDF
      // 写入mysql数据库，数据库配置在 AppConf中
      uidDF.write.mode(SaveMode.Append).jdbc(jdbcURL, alsTable, prop)
    }

//    // 把推荐结果写入到phoenix+hbase,通过DF操作，不推荐。
//    val hbaseConnectionString = "localhost"
//    val userTupleRDD = users.rdd.map { x => Tuple3(x.getInt(0), x.getInt(1), x.getDouble(2)) }
//    // zkUrl需要按照hbase配置的zookeeper的url来设置，本地模式就写localhost
//    userTupleRDD.saveToPhoenix("NGINXLOG_P", Seq("USERID", "MOVIEID", "RATING"), zkUrl = Some(hbaseConnectionString))
//
//    // 把推荐结果写入到phoenix+hbase,通过DF操作，不推荐。
//    def writeRecResultToHbase(uid: Array[Rating], sqlContext: SQLContext, sc: SparkContext) {
//      val uidString = uid.map(x => x.user.toString() + "|"
//        + x.product.toString() + "|" + x.rating.toString())
//      import sqlContext.implicits._
//      val uidDF = sc.parallelize(uidString).map(_.split("|")).map(x => Result(x(0).trim().toInt, x(1).trim.toInt, x(2).trim().toDouble)).toDF
//      // zkUrl需要按照hbase配置的zookeeper的url来设置
//      uidDF.save("org.apache.phoenix.spark", SaveMode.Overwrite, Map("table" -> "phoenix_rec", "zkUrl" -> "localhost:2181"))
//    }
  }

}
