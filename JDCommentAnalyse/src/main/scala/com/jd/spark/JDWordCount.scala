package com.jd.spark
import com.jd.db.MongodbManager
import com.mongodb.casbah.commons.MongoDBObject
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object JDWordCount {

  val dbManager = new MongodbManager()
  val collectionName = "monthwordcount"

  def startWordCountSparkStreaming(): Unit = {

    val conf = new SparkConf().setMaster("local[2]").setAppName("JDWordCount")
    val ssc = new StreamingContext(conf, Seconds(2))

    //hdfs://101.132.70.198:9000/user/root/spark/
    // local file path :  /Users/wangxiaofa/Desktop/Hadoop/JDCommentAnalyse/src/main/input/
    val lines = ssc.textFileStream("/Users/wangxiaofa/Desktop/Hadoop/JDCommentAnalyse/src/main/input/")

    val words = lines.flatMap(line => line.split(" "))

    val pairs = words.map(word => (word,1)) //map
    val wordsCount = pairs.reduceByKey(_+_) //reduce

    wordsCount.foreachRDD(rdd => {

      if (!rdd.isEmpty()){

        for (x <- rdd.collect()) dbManager.insertDB(collectionName,x._1.toString,x._2.toString)

      }
    })
    wordsCount.print()

    ssc.start()
    ssc.awaitTermination()
  }



  def main(args: Array[String]): Unit = {

    dbManager.connnectDB("localhost","spark")
    startWordCountSparkStreaming()
//    dbManager.insertDB(collectionName,"name","111")



  }



}
