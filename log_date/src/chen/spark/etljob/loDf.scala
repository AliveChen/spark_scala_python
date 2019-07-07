package chen.spark.etljob


import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext,sql}
import org.apache.spark.SparkConf

/**
 * parse the logfile,and create DataFrame
 */
object loDf {
  def main(args:Array[String]){
    val conf = new SparkConf()
          .setMaster("local[*]")
          .setAppName("logDf")
    
    val sc = new SparkContext(conf)
    
    val acessRDD = sc.textFile("data/log.txt")
    
    // create sqlContext:HiveContext
    val hiveContext = new HiveContext(sc)
    
    // rdd => Df
    val acessDF = hiveContext.createDataFrame(
        acessRDD.map(line => (
            AcessConvertUtils.parseLog(line)
            )
            ),AcessConvertUtils.structType)
    acessDF.printSchema()
    acessDF.show()
    
    //SAVE the dfdate for parquetfile
    acessDF.write.mode("Append").format("parquet").save("data/testnew/")        
            
            
  }
  
}