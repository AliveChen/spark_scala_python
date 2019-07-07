package chen.spark.etljob

import java.sql.{Connection, DriverManager, PreparedStatement}

object MysqlUtils {
  def getConnection():Connection ={
    Class.forName("com.mysql.cj.jdbc.Driver");
    DriverManager.getConnection("jdbc:mysql://localhost:3306/dbtest?user=root&password=123456")
  }

  def release(connection:Connection,psmt:PreparedStatement): Unit ={
    try{
      if(psmt!=null){
        psmt.close()
      }
    }catch {
      case e:Exception=>e.printStackTrace()
    }finally {
      if(connection!=null){
        connection.close()
      }
    }
  }

  def main(args: Array[String]): Unit = {
    println(getConnection())
  }  
}