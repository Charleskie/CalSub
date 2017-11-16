package wangsheng.spark
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}


object cal_bustimes {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("GPS").setMaster("local").set("spark.sql.warehouse.dir", "E:\\迅雷下载\\spark-2.0.0-bin-hadoop2.7")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val data = sc.textFile("F:\\北斗\\公交新线开通\\输出文件\\2017-*").map(s => s.split("\t"))
      .filter(s => s(6) == "B689" || s(6) == "M312" || s(6) == "M454" || s(6) == "M441" || s(6) == "M483")
      .map(s => {
        if( s(10).substring(14,15) == "0" || s(10).substring(14,15) == "1" || s(10).substring(14,15) == "2"){
          s(10) = s(10).substring(11,14) + "00:00"
        }else if( s(10).substring(14,15) == "3" || s(10).substring(14,15) == "4" || s(10).substring(14,15) == "5"){
          s(10) = s(10).substring(11,14) + "30:00"
        }
        (s(0),s(1),s(6),s(3),s(10))})
      .toDF("id","day","line","carId","timeO")
      .groupBy("line","timeO").count().sort("line").repartition(1).write.csv("F:\\北斗\\公交新线开通\\输出文件\\2017busround")
  }
}


