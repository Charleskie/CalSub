package wangsheng.spark
import org.apache.spark.{SparkConf, SparkContext}


object cal_BusRound {
  def main(args: Array[String]): Unit = {
    //    val spark = SparkSession.builder().appName("GPS").master("local[*]").config("spark.sql.warehouse.dir", "E:/spark/wangsheng/spark-warehouse")
    //      .getOrCreate()
    val conf = new SparkConf().setAppName("GPS").setMaster("local")
    val sc = new SparkContext(conf)
//    for(i <- 1 to 30) {
//      var path = "2016-06-"
//      if(i < 10){
//        path = path + "0$i"
//      }else {
//        path = path + "$i"
//      }
//      val data = sc.textFile("F:\\北斗\\公交新线开通\\输出文件\\2016-06").map(s => s.split(","))
//        .filter(s => s(2) == "B689" || s(2) == "M312" || s(2) == "M454" || s(2) == "M441" || s(2) == "M483")
//          .map(s => {
//            if((s(5).substring(11,13) == "07" || s(5).substring(11,13) == "08") || (s(5).substring(11,13) == "09" && (s(5).substring(14,15) == "0" || s(5).substring(14,15) == "1" || s(5).substring(14,15) == "2"))){
//              s(5) = "mor"
//            }else if((s(5).substring(11,13) == "17" || s(5).substring(11,13) == "18" || s(5).substring(11,13) == "19") || (s(5).substring(11,13) == "16" && (s(5).substring(14,15) == "3" || s(5).substring(14,15) == "4" || s(5).substring(14,15) == "5"))){
//              s(5) = "eve"
//            }else{ s(5) = "flat" }
//            s(0)+","+s(1)+","+s(2)+","+s(3)+","+s(4)+","+s(5)+","+s(15)
//          }).map(s => s.split(","))
////      .map(s => s(2)+","+s(5)+","+s(6)).map(s => s.split(",")).groupBy(s => s(0)+","+s(1)).map(s => {
////          val lineandtime = s._1
////          val count = s._2.toArray.length
////          val avg = s._2.toArray.map(s => s(2).toDouble)
////              lineandtime+","+count+","+avg
////            }).foreach(println)
//  .map(s => s(2)+","+s(5)).countByValue().foreach(println)
////    }

    val data = sc.textFile("F:\\北斗\\公交新线开通\\输出文件\\2017-07").map(s => s.split("\t"))
      .filter(s => s(6) == "B689" || s(6) == "M312" || s(6) == "M454" || s(6) == "M441" || s(6) == "M483")
      .map(s => {
        if((s(5).substring(11,13) == "07" || s(5).substring(11,13) == "08") || (s(5).substring(11,13) == "09" && (s(5).substring(14,15) == "0" || s(5).substring(14,15) == "1" || s(5).substring(14,15) == "2"))){
          s(5) = "mor"
        }else if((s(5).substring(11,13) == "17" || s(5).substring(11,13) == "18" || s(5).substring(11,13) == "19") || (s(5).substring(11,13) == "16" && (s(5).substring(14,15) == "3" || s(5).substring(14,15) == "4" || s(5).substring(14,15) == "5"))){
          s(5) = "eve"
        }else{ s(5) = "flat" }
        s(0)+","+s(1)+","+s(6)+","+s(3)+","+s(4)+","+","+s(15)
      }).map(s => s.split(","))
      //      .map(s => s(2)+","+s(5)+","+s(6)).map(s => s.split(",")).groupBy(s => s(0)+","+s(1)).map(s => {
      //          val lineandtime = s._1
      //          val count = s._2.toArray.length
      //          val avg = s._2.toArray.map(s => s(2).toDouble)
      //              lineandtime+","+count+","+avg
      //            }).foreach(println)
      .map(s => s(2)+","+s(5)).countByValue().foreach(println)
  }
}