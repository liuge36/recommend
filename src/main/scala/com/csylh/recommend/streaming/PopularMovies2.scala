package com.csylh.recommend.streaming

import com.csylh.recommend.config.AppConf
import org.apache.spark.sql.{Dataset, SaveMode}

/**
  * Description: 
  *
  * @Author: 留歌36
  * @Date: 2019/10/18 17:42
  */
object PopularMovies2 extends AppConf{
    val movieRatingCount = spark.sql("select count(*) c, movieid from trainingdata group by movieid order by c")
    // 前5部进行推荐
    val Top5Movies = movieRatingCount.limit(5)

    Top5Movies.registerTempTable("top5")

    val top5DF = spark.sql("select a.title from movies a join top5 b on a.movieid=b.movieid")

    // 把数据写入到HDFS上
    top5DF.write.mode(SaveMode.Overwrite).parquet("/tmp/top5DF")

    // 将数据从HDFS加载到Hive数据仓库中去
    spark.sql("drop table if exists top5DF")
    spark.sql("create table if not exists top5DF(title string) stored as parquet")
    spark.sql("load data inpath '/tmp/top5DF' overwrite into table top5DF")

    // 最终表里应该是5部推荐电影的名称
}
