import org.apache.spark
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql
import org.apache.spark.sql.{SQLContext, SparkSession}


object cal_stationFlow {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("GPS").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val inpath = args(0)
    val outpath = args(1)
    //180016121,03080,粤B79755,2,91e25c99dcc8de5b12f00e92538e4843,2016-06-04T07:38:15.000Z,XBUS_00005468,畔山花园,15,114.174196,22.560408,,XBUS_00005365,市慢病防治中心,24,114.134764,22.5754
    val data = sc.textFile(inpath).map(s => s.split(","))
      .filter(s => s(1).substring(0,4) == "B689" || s(1).substring(0,4) == "M312" || s(1).substring(0,4) == "M454" || s(1).substring(0,4) == "M441" || s(1).substring(0,4) == "M483")
      .map(s =>{
        val cardId = s(0)
        val line = s(1).substring(0,4)
        val day = s(5).substring(0,10)
        if((s(5).substring(11,13) == "07" || s(5).substring(11,13) == "08") || (s(5).substring(11,13) == "09" && (s(5).substring(14,15) == "0" || s(5).substring(14,15) == "1" || s(5).substring(14,15) == "2"))){
          s(5) = "mor"
        }else if((s(5).substring(11,13) == "17" || s(5).substring(11,13) == "18" || s(5).substring(11,13) == "19") || (s(5).substring(11,13) == "16" && (s(5).substring(14,15) == "3" || s(5).substring(14,15) == "4" || s(5).substring(14,15) == "5"))){
          s(5) = "eve"
        }else{ s(5) = "flat" }
        val time = s(5)
        val stationO = s(7)
        val stationD = s(s.length - 4)
        (cardId,line,day,time,stationO,stationD)
      }).toDF("cardId","line","day","time","stationO","stationD")
    val Onflow = data.groupBy("line","time","stationO").count().repartition(1).write.csv(outpath + "/Oflow")
    val Dflow = data.groupBy("line","time","stationD").count().repartition(1).write.csv(outpath + "/Dflow")
    val ODflow = data.groupBy("line","time","stationO","stationD").count().repartition(1).write.csv(outpath + "/ODflow")
  }
}