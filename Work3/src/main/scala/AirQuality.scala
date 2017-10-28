import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf}


class AirQuality(time:String,id:Int,PM2_5:Int,PM10:Int,SO2:Int,CO:Int,NO2:Int,O3:Int) {


  def calculateAirQuality:Unit = {

    if (this.id == 0){
      return
    }

    //空气质量检测
    if (this.PM2_5 >= 150) {

      this.printResult("PM2.5",this.PM2_5)
    }
    if (this.PM10 >= 350) {

      this.printResult("PM10",this.PM10)
    }
    if(this.SO2 >= 800){
      this.printResult("SO2",this.SO2)
    }
    if(this.CO >= 60){
      this.printResult("CO",this.CO)

    }
    if(this.NO2 >= 1200){
      this.printResult("NO2",this.NO2)
    }
    if(this.O3 >= 400) {
      this.printResult("O3", this.O3)
    }
  }

  def printResult(key:String,value:Int):Unit = {

    val result = this.time + " | " + this.id + " | " + key + " | " + value

    println(result)
  }
}
object AirQuality {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[2]").setAppName("AirQuality")
    val ssc = new StreamingContext(conf,Seconds(1))

    //从socket端口读取数据,返回的是ReceiverInputDStream
    val receiverInputStream = ssc.socketTextStream("localhost",9999);

    //对返回的是ReceiverInputDStream操作，进行切分数据,生成DStream
    val words = receiverInputStream.flatMap(line => line.split(","))

    //遍历所有的rdd，并将他们转化成数组
    words.foreachRDD(rdd => {

      val arr = rdd.collect()

      if (arr.length > 7) {

        val airQuality = new AirQuality(
          arr(0),
          arr(1).trim.toInt,
          arr(2).trim.toInt,
          arr(3).trim.toInt,
          arr(4).trim.toInt,
          arr(5).trim.toInt,
          arr(6).trim.toInt,
          arr(7).trim.toInt)

        airQuality.calculateAirQuality
      }


    })

    ssc.start()
    ssc.awaitTermination()

  }

}
