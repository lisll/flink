package com.dinglicom.flink.stream.batch

import java.sql.DriverManager
import java.sql.Connection
import java.sql.PreparedStatement

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction

class MysqlDBSink[Tuple2[String, Long]] extends RichSinkFunction[Tuple2[String, Long]] {

  var connection: Connection = null
  var preparedStatement: PreparedStatement = null
  val username: String = "root"
  val password: String = "dinglicom"
  val drivername: String = "com.mysql.jdbc.Driver"
  val dburl: String = "jdbc:mysql://172.16.50.61:3306/flink_db?useSSL=false"

  /*
	override def invoke(item: IN): Unit = {
		val tuple = item.asInstanceOf[(String, Long)]
	  
	}*/

  override def invoke(item: Tuple2[String, Long]): Unit = {
    try {
      val tuple = item.asInstanceOf[(String, Long)]
      if (connection == null) {
        Class.forName(drivername)
        connection = DriverManager.getConnection(dburl, username, password)
      }
      //replace into user_traffic_info(msisdn,traffic) values(?,?)
      var sql = "replace into user_traffic_info(msisdn,traffic) values(?,?)"
      preparedStatement = connection.prepareStatement(sql)
      preparedStatement.setString(1, tuple._1)
      preparedStatement.setLong(2, tuple._2)
      preparedStatement.executeUpdate()
    } catch {
      case ex: Exception => {
        throw ex
      }
    }
  }

  override def open(parameters: Configuration) {
    Class.forName(drivername)
    connection = DriverManager.getConnection(dburl, username, password)
  }

  override def close() {
    if (preparedStatement != null) {
      preparedStatement.close()
    }

    if (connection != null) {
      connection.close()
    }

  }
}