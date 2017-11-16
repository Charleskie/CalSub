package wangsheng.spark
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}


object cal_busflow{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("GPS").setMaster("local").set("spark.sql.warehouse.dir","E:\\迅雷下载\\spark-2.0.0-bin-hadoop2.7")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    //--7f28871e187a000d6a99ede128e57e64,2016-06-01,1,粤B82661,上行,2016-06-01 13:03:35,2016-06-01 13:24:58,五方广场(5),火车站(14),2016-06-01 12:58:44,2016-06-01 13:24:58,东湖客运站(1),火车站(14),10/14,10/14,26,
    val data = sc.textFile("F:\\北斗\\公交新线开通\\输出文件\\2016*").map(s => s.split(","))
      .filter(s => s(2) == "B689" || s(2) == "M312" || s(2) == "M454" || s(2) == "M441" || s(2) == "M483")
      .map(s => {
        if((s(5).substring(11,13) == "07" || s(5).substring(11,13) == "08") || (s(5).substring(11,13) == "09" && (s(5).substring(14,15) == "0" || s(5).substring(14,15) == "1" || s(5).substring(14,15) == "2"))){
          s(5) = "mor"
        }else if((s(5).substring(11,13) == "17" || s(5).substring(11,13) == "18" || s(5).substring(11,13) == "19") || (s(5).substring(11,13) == "16" && (s(5).substring(14,15) == "3" || s(5).substring(14,15) == "4" || s(5).substring(14,15) == "5"))){
          s(5) = "eve"
        }else{ s(5) = "flat" }
          (s(0),s(1),s(2),s(3),s(5),s(6),s(15).toDouble)})
        .toDF("id","day","line","carId","timeO","timeD","sum")
        .groupBy("line","timeO").avg("sum").show
    
  }
}