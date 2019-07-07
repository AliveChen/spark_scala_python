package chen.spark.etljob

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

import scala.collection.mutable.ListBuffer
import org.apache.spark.SparkConf

object TopNStatJob {
  def main(args:Array[String]):Unit ={
    val conf = new SparkConf()
          .setMaster("local[*]")
          .setAppName("TopNStatJob")
    
    val sc = new SparkContext(conf)
    
    val hiveContext = new HiveContext(sc)
    
    val dataDF = hiveContext.read.parquet("data/testnew/*")
    
    //dataDF.show()
    
    
    TopNCity(hiveContext, dataDF)
  }

  def TopNCity(hiveContext:HiveContext,dataDF:DataFrame)={
    // cout the acess of city groupby day
    val cityTopN = dataDF.groupBy(col("city"),col("time").substr(1,10).as("day"))
          .agg(count("city").as("topncity"))
          .orderBy(col("topncity").desc)
    cityTopN.show()
    
    //
    try{cityTopN.foreachPartition(cityPartion =>{
      val list = new ListBuffer[CityAccessTopn]
            
      cityPartion.foreach(line => {
        val day = line.getAs[String](0)
        val city = line.getAs[String](1)
        val acessNum = line.getAs[Long](2)
              
        list.append(CityAccessTopn(day,city,acessNum)) })
            
        CityAccessTopnDao.insertCityTopn(list)    
    })
  }catch{
     case e:Exception => e.printStackTrace()
   }

  }

// object
}



  