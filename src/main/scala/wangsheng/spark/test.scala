package wangsheng.spark
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import wangsheng.spark.timeformat._


object test {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("GPS").setMaster("local").set("spark.sql.warehouse.dir", "E:\\迅雷下载\\spark-2.0.0-bin-hadoop2.7")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    /***
      ** 2016-06-01 17:49:05
      ** 20170602073442
      ** param time
      * *return
      * *2016-06-02T00:05:05.000Z
      **/
    
    val time1 = "2016-06-01 17:49:05"
    val time2 = "2016-06-01 18:41:05"
    val time = "2012-12-22 09:00:00"
    println(CustomFormatToISO(time))
    println(CustomFormatToISO(time1))
    println(CustomFormatToISO(time2))
    println(calTimeDiff(CustomFormatToISO(time1),CustomFormatToISO(time2) ,3600))
    println(calTimeDiff(CustomFormatToISO(time1),CustomFormatToISO(time2) ,60))
    println(calTimeDiff(CustomFormatToISO(time1),CustomFormatToISO(time2) ,1))
    
  }
}