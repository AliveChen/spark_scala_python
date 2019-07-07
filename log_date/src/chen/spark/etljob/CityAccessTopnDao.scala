package chen.spark.etljob

import java.sql.{Connection,DriverManager, PreparedStatement}

import scala.collection.mutable.ListBuffer

/**
  * 将访问的城市信息存入数据的DAO类
  */
object CityAccessTopnDao {

  var connection:Connection=null
  var pstm:PreparedStatement=null

  def insertCityTopn(list:ListBuffer[CityAccessTopn]): Unit ={
    try{
      connection=MysqlUtils.getConnection()
      connection.setAutoCommit(false)       //设置为手动提交
      val  sql="insert into city_topn(day,city,time) values(?,?,?)"
      pstm=connection.prepareStatement(sql)

      for(city<-list){
        pstm.setString(1,city.day)
        pstm.setString(2,city.city)
        pstm.setLong(3,city.times)
        pstm.addBatch()          //加入批处理操作
      }
      pstm.executeBatch()
      connection.commit()

    }catch {
      case e:Exception=>e.printStackTrace()
    }finally {
        MysqlUtils.release(connection,pstm)
    }
  }

  /**
    * 向数据库中插入统计的结果
    * @param list
    */
  def main(args:Array[String]):Unit = {
    println(1)
   }
} 


  
  