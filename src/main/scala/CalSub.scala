import org.apache.spark
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import scala.collection.mutable.ArrayBuffer

object CalSub {
  def main(args: Array[String]): Unit = {
    //    val spark = SparkSession.builder().appName("GPS").master("local[*]").config("spark.sql.warehouse.dir", "E:/spark/wangsheng/spark-warehouse")
    //      .getOrCreate()
    val conf = new SparkConf().setAppName("GPS").setMaster("local")
    val sc = new SparkContext(conf)
    //    val sc = spark.sparkContext
    val idArray = new ArrayBuffer[String]()
    val latArray = new ArrayBuffer[Double]()
    val lonArray = new ArrayBuffer[Double]()
    val id = sc.textFile("F:\\北斗\\公交新线开通\\脚本文件\\subway_zdbm_station").map(s => s.split(",")).cache().foreach(s =>{
      val line: String = s(0)
      val arr = line.toArray
      println(arr)
      latArray += s(5).toDouble
      lonArray += s(4).toDouble
    })
    val lat = sc.textFile("F:\\北斗\\公交新线开通\\脚本文件\\subway_zdbm_station").map(s => s.split(",")(5))
    val lon = sc.textFile("F:\\北斗\\公交新线开通\\脚本文件\\subway_zdbm_station").map(s => s.split(",")(4))
    //println(rdd2array(id))
    println(rdd2array(lat).mkString(","))
    /**
      * 将存有一列字符串的rdd转为字符串数组
      * @param rdd
      */
    def rdd2array(rdd: RDD[String]): Array[String] = {
      val arr = new ArrayBuffer[String]()
      rdd.foreach( s =>{
        val line: String = s
        arr += line
        arr.toArray
      })
      arr.toArray
    }
    def getLatandLon(card_id: String): (Double, Double) = {
      (0.1,0.1)
    }
  }
}

//where are you?
//myname is kim, I'm your master
//so who am I? 
