package wangsheng.spark
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import wangsheng.spark.timeformat._

object cal_stationForB689 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("GPS").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val inpath = args(0)
    val outpath = args(1)
    //020006546,2017-06-01T16:14:46.000Z,00290,粤B6878D,1,aa8f4b0458f0af324fc61fdb9757a852,XBUS_00005376,金稻田路口,7,114.134106,22.582618,2017-06-01T16:15:09.000Z,23
    val data = sc.textFile(inpath)
      .map(s => s.split(",")).filter(s => s(7) == "监控中心" || s(7) == "莲花北村*" || s(7) == "彩田村*" || s(7) == "公交大夏" || s(7) == "莲花一村" || s(7) == "笔架山公园" || s(7) == "笔架山公交总站")
      .map(s =>{
        val card = s(0)
        val time = changetime(s(1),30).substring(0,10)
        val line = s(2)
        val derection = s(4)
        val station = s(7)
        (card,time,line,derection,station)
      }).toDF("card","time","line","derection","station").groupBy("derection","station","time").count()
      .groupBy("derection","station").avg("count").repartition(1).write.csv(outpath + "/forB689/addStationFlow/2017")
  }
}

//spark-submit --class "wangsheng.spark.cal_stationForB689" --master yarn /export/home/wangsheng/spark/calsub.jar /user/zhangjun/Mining/BusO/2017-{06,07}-* /user/wangsheng/