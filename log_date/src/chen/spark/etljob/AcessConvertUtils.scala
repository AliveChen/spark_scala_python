package chen.spark.etljob

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{LongType,StringType,StructType,StructField}

object AcessConvertUtils {
  
  val structType = StructType(
      Array(
          StructField("url",StringType),
          StructField("cmsType", StringType),
          StructField("cmsId", StringType),
          StructField("traffic", StringType),
          StructField("ip", StringType),
          StructField("score",StringType),
          StructField("city", StringType),
          StructField("time", StringType),
          StructField("day", StringType)
          )
          )
          
   /**
    * parse the input for output
    * @param
    * @return
    */
   def parseLog(acessLog:String) = {
    
    try{
      val logSplit = acessLog.split("\t")
      val url =logSplit(0)
      val cmsType =logSplit(1)
      val cmsId = logSplit(2)
      val traffic = logSplit(3)
      val ip = logSplit(4)
      val score = logSplit(5)
      val city = logSplit(6)
      val time = logSplit(7)
      val day = "Null"
      
      Row(url,cmsType,cmsId, traffic, ip, score,city, time, day)
    }
    catch{
      case e:Exception=>{
        println("parse error")
        Row(0)
      }
    }
    }
  
  def main(args:Array[String]) {
    val a = "https+url	gosubject	16	74	221.226.3.141	639	30city	2019-04-12 20:10:09"
    println(1)
    //parseLog(a)
  }
  
  
}