package com.csylh.recommend.streaming

import java.util.Properties

import com.csylh.recommend.config.AppConf
import org.apache.kafka.clients.producer._
import org.apache.spark.sql.Dataset

/**
  * Description: 我们把验证集合（40%总数据）的数据  ，怼到 Kafka 消息队列里
  *
  *  这份代码
  *
  * @Author: 留歌36
  * @Date: 2019/10/18 10:17
  */
object KafkaProducer extends AppConf {
  def main(args: Array[String]) {
    // 如果数据不加 limit限制，会出现OOM错误
    val testDF = spark.sql("select * from testData limit 10000")
    val props = new Properties()
    // 指定kafka的 ip地址:端口号
    props.put("bootstrap.servers", "hadoop001:9093,hadoop001:9094,hadoop001:9095")
    // 配置可以设定发送消息后是否需要Broker端返回确认，有"0"，"1"，"all"
    //    props.put("acks", "all")
    //    props.put("retries", "0")
    //    props.put("batch.size", "16384")
    //    props.put("linger.ms", "1")
    //    props.put("buffer.memory", "33554432")
    // 设定ProducerRecord发送的key值为String类型
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    // 设定ProducerRecord发送的value值为String类型
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val topic = "movie_topic"
    import spark.implicits._
    val testData:Dataset[(String,String)] = testDF.map(x => (topic, x.getInt(0).toString() + "|" + x.getInt(1).toString + "|" + x.getDouble(2).toString()))
    val producer = new KafkaProducer[String, String](props)

    // 如果服务器内存不够，会出现OOM错误
    val messages = testData.toLocalIterator

    while (messages.hasNext) {
      val message = messages.next()
      val record = new ProducerRecord[String, String](topic, message._1, message._2)
      println(record)
      producer.send(record)
      // 延迟10毫秒
      Thread.sleep(1000*5)
    }
    producer.close()


    // TODO... for循环会有序列化的问题
    //for (x <- testData) {
    //  val message = x
    //  val record = new ProducerRecord[String, String]("test", message._1, message._2)
    //  println(record)
    //  producer.send(record)
    //  Thread.sleep(1000)
    //}

    // 不用testData.map或者foreach，因为这两种方法会让你的数据做分布式计算，在计算时，处理数据是无序的。
    // testData.foreach
  }

}
