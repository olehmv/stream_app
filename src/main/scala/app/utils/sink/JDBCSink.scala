package app.utils.sink

import java.sql.{ Connection, DriverManager, Statement }

import org.apache.spark.sql.ForeachWriter

class JDBCSink(url: String, user: String, pwd: String, sql: String)
    extends ForeachWriter[(String, Long)] {
  val driver                 = "com.mysql.jdbc.Driver"
  var connection: Connection = _
  var statement: Statement   = _

  def open(partitionId: Long, version: Long): Boolean = {
    Class.forName(driver)
    connection = DriverManager.getConnection(url, user, pwd)
    statement = connection.createStatement
    true
  }

  def process(value: (String, Long)): Unit =
    statement.executeUpdate(sql)

  def close(errorOrNull: Throwable): Unit =
    connection.close
}
