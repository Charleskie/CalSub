package wangsheng.spark
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}


object timeformat {

  /***
    *
    * @param string ISO时间格式
    * @return  分时段时间：早晚高峰
    */
  def isTime(string: String): String = {
    var isTime = ""
    if((string.substring(11,13) == "07" || string.substring(11,13) == "08") || (string.substring(11,13) == "09" && string.substring(14,15) == "0" || string.substring(14,15) == "1" || string.substring(14,15) == "2")){
      isTime = "mor"
    }else if((string.substring(11,13) == "17" || string.substring(11,13) == "18" || string.substring(11,13) == "19") || (string.substring(11,13) == "16" && (string.substring(14,15) == "3" || string.substring(14,15) == "4" || string.substring(14,15) == "5"))){
      isTime = "eve"
    }else{ isTime = "flat" }
    isTime
  }

  /***
    * 2016-06-01 17:49:05
    * 20170602073442
    * @param time 普通格式的时间
    * @return ISO格式的时间
    * 2016-06-02T00:05:05.000Z
    */

  def CustomFormatToISO(time: String): String = {
    var ISO = ""
    var year = ""
    var month = ""
    var date = ""
    var hour = ""
    var min = "" 
    var sec = ""
    val mark = ".000Z"
    if(time.split(" ").length == 1 && time.split("T").length == 1){
      year = time.substring(0,4)
      month = time.substring(4,6)
      date = time.substring(6,8)
      hour = time.substring(8,10)
      min = time.substring(10,12)
      sec = time.substring(12,14)
      ISO = year + "-" + month + "-" +date + "T" + hour + ":" + min  + ":" + sec + mark
    }else if( time.split(" ").length == 2 && (time.split("-").length == time.split(":").length)){
      ISO = time.split(" ")(0) + "T" + time.split(" ")(1) + mark
    }else{ ISO = "" }
    ISO
  }

  /***
    * 
    * @param time ISO时间格式
    * @param num  以num分钟间隔
    * @return 返回按num分钟间隔之后的时间
    */

  def changetime(time: String, num: Int): String = {
    val minToNum = time.substring(14, 16).toInt
    val dev: Int = minToNum / num
    val min: Int = dev * num
    var minToString = ""
    if (min < 10) {
      minToString = "0" + min.toString
    } else {
      minToString = min.toString
    }
    val changeTime = time.substring(0, 14) + minToString + ":00"
    changeTime
  }
  
}