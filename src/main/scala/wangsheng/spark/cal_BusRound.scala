package wangsheng.spark
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import wangsheng.spark.timeformat._

object cal_BusRound {
  def main(args: Array[String]): Unit = {
    //    val spark = SparkSession.builder().appName("GPS").master("local[*]").config("spark.sql.warehouse.dir", "E:/spark/wangsheng/spark-warehouse")
    //      .getOrCreate()
    val conf = new SparkConf().setAppName("GPS").setMaster("local").set("spark.sql.warehouse.dir","E:\\迅雷下载\\spark-2.0.0-bin-hadoop2.7")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
//    val data = sc.textFile("F:\\北斗\\公交新线开通\\输出文件\\2017-07").map(s => s.split("\t"))
//      .filter(s => s(6) == "216" || s(6) == "M441" || s(6) == "44" || s(6) == "67" || s(6) == "M204"  || s(6) == "M204" || s(6) == "60" || s(6) == "M312")
//      .map(s => {
//        val time = isTime(CustomFormatToISO(s(5)))
//        s(0)+","+s(1)+","+s(6)+","+s(3)+","+s(4)+","+ time + ","+s(15)
//      }).map(s => s.split(","))
//      .map(s => s(2)).countByValue().foreach(println)
//2017-07-01 00:00:00,0312M,BS04238D,下行,农科中心公交总站,上梅林公交总站,刘思红,2017-07-01 07:00:00.000,2017-07-01 07:31:00.000,2017-07-01 07:01:33.000,2017-07-01 07:31:00.000    
    val newdata = sc.textFile("F:\\北斗\\五线路调整\\文本文件\\busRound11th.csv").map(s => s.split(","))
          .filter( s => s(1) == "0312M" || s(1) == "0044" || s(1) == "0060" || s(1) == "0201" || s(1) == "0324" || s(1) == "0204M" || s(1) == "0441M" ).map(s => {
      val time = isTime(CustomFormatToISO(s(s.length - 4).substring(1,20)))
      val timeO = CustomFormatToISO(s(s.length - 4).substring(1,20))
      val timeD = CustomFormatToISO(s(s.length - 3).substring(1,20))
      val timediff = calTimeDiff(timeO,timeD,60)
  (s(1),s(s.length - 1),time , timediff)
    }).toDF("line","direction","isTime","timediff").groupBy("line","direction","isTime").count.show
  }
}