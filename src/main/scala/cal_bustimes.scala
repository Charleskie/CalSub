import org.apache.spark
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql
import org.apache.spark.sql.{SQLContext, SparkSession}


object cal_bustimes {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("GPS").setMaster("local").set("spark.sql.warehouse.dir", "E:\\迅雷下载\\spark-2.0.0-bin-hadoop2.7")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val data = sc.textFile("F:\\北斗\\公交新线开通\\输出文件\\2016*").map(s => s.split(","))
      .filter(s => s(2) == "B689" || s(2) == "M312" || s(2) == "M454" || s(2) == "M441" || s(2) == "M483")
      .map(s => {
        if( s(5).substring(14,15) == "0" || s(5).substring(14,15) == "1" || s(5).substring(14,15) == "2"){
          s(5) = s(5).substring(11,14) + "00:00"
        }else if( s(5).substring(14,15) == "3" || s(5).substring(14,15) == "4" || s(5).substring(14,15) == "5"){
          s(5) = s(5).substring(11,14) + "30:00"
        }
        (s(0),s(1),s(2),s(3),s(5),s(6),s(15).toDouble)})
      .toDF("id","day","line","carId","timeO","timeD","sum")
      .groupBy("line","timeO").count().show
  }
}