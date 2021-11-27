import java.sql.{Connection, DriverManager}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import scala.io.Source

class AuditManager {
  val key_values = Source.fromFile("src/main/configs/db_configs.txt").getLines().map(line=> line.split("=")).toArray
  val keys = key_values.map(line=>line(0)).toArray
  val values= key_values.map(line=>line(1)).toArray
  val configs = keys.zip(values).toMap

  var conn: Connection = null
  val url = "jdbc:mysql://localhost:3306/mysql"
  val driver = "com.mysql.jdbc.Driver"
  try {
    Class.forName(driver)
    conn = DriverManager.getConnection(url, configs("USER"), configs("PASSWORD"))
  }
  catch {
    case e => {
      throw e
    }
  }

  def checkLog(checkSum: String): Boolean = {
    val statement = conn.createStatement()
    statement.executeQuery(s"SELECT 1 FROM audit_db.stg_audit_table WHERE checksum = '$checkSum' AND endLoadDate IS NOT NULL LIMIT 1").next()
  }

  def startStgAudit(filename: String, checkSum: String, startLoadDate: String, market: String): Unit = {
    try {
      val statement = conn.createStatement
      val queryString =
        s"""INSERT INTO audit_db.stg_audit_table(filename,`checksum`,startLoadDate,endLoadDate,task,market,countOfInsertedRows)
                            VALUES ('$filename','$checkSum','$startLoadDate',NULL,'load data from source to staging','$market',NULL)"""
      statement.executeUpdate(queryString)
    }
    catch {
      case e: Exception => {
        println(e)
      }
    }
  }

  def endStgAudit(checkSum: String, startLoadDate: String, count: Long): Unit = {
    val endLoadDate = LocalDateTime.now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
    try {
      val statement = conn.createStatement
      val queryString =
        s"""UPDATE audit_db.stg_audit_table
                            SET endLoadDate = '$endLoadDate',
                                countOfInsertedRows = $count
                            WHERE startLoadDate = '$startLoadDate' AND checksum = '$checkSum'"""
      statement.executeUpdate(queryString)
    }
    catch {
      case e: Exception => {
        println(e)
      }
    }
  }

  def startFundAudit(startLoadDate: String, market: String): Unit = {
    try {
      val statement = conn.createStatement
      val queryString =
        s"""INSERT INTO audit_db.fund_audit_table(startLoadDate,endLoadDate,task,market)
                            VALUES ('$startLoadDate',NULL,'load data from stg to destination','$market')"""
      statement.executeUpdate(queryString)
    }
    catch {
      case e: Exception => {
        println(e)
      }
    }
  }

  def endFundAudit(startLoadDate: String): Unit = {
    val endLoadDate = LocalDateTime.now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
    try {
      val statement = conn.createStatement
      val queryString =
        s"""UPDATE audit_db.fund_audit_table
                            SET endLoadDate = '$endLoadDate'
                            WHERE startLoadDate = '$startLoadDate'"""
      statement.executeUpdate(queryString)
    }
    catch {
      case e: Exception => {
        println(e)
      }
    }
  }

  def getLastUpdateDate: LocalDateTime = {
    try {
      val statement = conn.createStatement
      val queryString =
        s"""SELECT MAX(startLoadDate)
            FROM audit_db.fund_audit_table
            WHERE endLoadDate IS NOT NULL"""
      val rs = statement.executeQuery(queryString)
      //if (rs.isAfterLast) {
      //println("ku")
      rs.next()
      rs.getTimestamp(1).toLocalDateTime
      //}
      //else {
      //  null
      //}
    }
    catch {
      case e => {
        println(e)
        //println("ku")
        null
      }
    }
  }

  def getSuccessLoadDates(market: String): Seq[LocalDateTime] = {
    try {
      val statement = conn.createStatement
      val queryString =
        s"""SELECT startLoadDate
                                FROM audit_db.stg_audit_table
                                WHERE endLoadDate IS NOT NULL AND market = '$market'"""
      val rs = statement.executeQuery(queryString)
      var resultSeq: Seq[LocalDateTime] = Seq()
      while (rs.next()) {
        resultSeq = resultSeq :+ rs.getTimestamp(1).toLocalDateTime
      }
      resultSeq
    }
    catch {
      case e: Exception => {
        println(e)
        Seq()
      }
    }
  }
}
