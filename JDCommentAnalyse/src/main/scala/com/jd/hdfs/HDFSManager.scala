package com.jd.hdfs

import java.util

import org.apache.hadoop.mapred.InputSplit
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object HDFSManager {

  def startUploadFileSparkStreaming(): Unit = {

    val conf = new SparkConf().setMaster("local[2]").setAppName("JDWordCount")
    val ssc = new StreamingContext(conf, Seconds(2))

    // local file path :  /Users/wangxiaofa/Desktop/Hadoop/JDCommentAnalyse/src/main/input/
    val lines = ssc.textFileStream("/Users/wangxiaofa/Desktop/Hadoop/JDCommentAnalyse/src/main/input/")


    lines.foreachRDD(rdd => {

      if (!rdd.isEmpty()){

        rdd.repartition(1).saveAsTextFile("/Users/wangxiaofa/Desktop/Hadoop/JDCommentAnalyse/src/main/output/1/1.txt")
      }
      else {

      }
    })
    //hdfs://101.132.70.198:9000/user/root/sparkOutput/

    lines.print()

    ssc.start()
    ssc.awaitTermination()
  }

  def main(args: Array[String]): Unit = {

    startUploadFileSparkStreaming()
  }
}
