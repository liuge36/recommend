package com.csylh.recommend.utils

import org.apache.spark.sql.SparkSession

/**
  * Description: TODO
  *
  * @Author: 留歌36
  * @Date: 2019-07-12 15:17
  *
  *
  */
object tt {
  private def rebuild(input:String): String ={
    val a = input.split(",")

    val head = a.take(2).mkString(",")
    val tail = a.takeRight(1).mkString
    val tag = a.drop(2).dropRight(1).mkString.replaceAll("\"","")
    val output = head + "," + tag + "," + tail
    println(output)
    output
  }




  def main(args: Array[String]): Unit = {
    /**
      *
      * 测试1
      */
    //    val input = "3064,260,\"action, scifi\",1442943290"
//    println(rebuild(input))
    /**
      * 测试2
      * for 循环
      */
//    val input = "101,102,"
//    val arr = input.split(",")
//    for(i <- 0 until arr.length){
//      println(arr(i))
//    }
    /**
      * 测试3
      */
    val spark = SparkSession.builder()
      .appName("text3")
      .master("local[2]")
      .getOrCreate()
    spark.read.option("header","true").csv("data2/links.csv")

      .show(false)


    spark.stop()



  }
}
