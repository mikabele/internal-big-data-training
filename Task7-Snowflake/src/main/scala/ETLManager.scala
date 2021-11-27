import java.io.{File, FileInputStream}
import java.security.{DigestInputStream, MessageDigest}
import java.sql.{Connection, Statement}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object ETLManager {
  var conn: Connection = null

  var dataDirectory: String = null
  var market: String = null
  var typeOfLoad: String = null

  def checkSumSha256(path: String): String = {

    val buffer = new Array[Byte](8192)
    val sha256 = MessageDigest.getInstance("SHA-256")

    val dis = new DigestInputStream(new FileInputStream(path), sha256)
    try {
      while (dis.read(buffer) != -1) {}
    }
    finally {
      dis.close()
    }

    sha256.digest.map("%02x".format(_)).mkString
  }

  def checkHashSum(checkSum: String): Boolean = {
    val statement = conn.createStatement()
    val res = statement.executeQuery(s"SELECT 1 FROM audit_db.public.stg_audit_table WHERE checksum = '$checkSum' AND endLoadDate IS NOT NULL LIMIT 1").next()
    statement.close()
    res
  }

  def etlFromStgToDestination() : Unit = {
    var stat: Statement = null
    try {
      stat = conn.createStatement()
      var queryString: String = null

      if(typeOfLoad == "full") {
        queryString =
          """TRUNCATE TABLE stg_fund_db.public.percents_per_month_table;"""
        stat.executeUpdate(queryString)
        queryString =
          """TRUNCATE TABLE fund_db.public.monthly_table;"""
        stat.executeUpdate(queryString)
      }

      val startTimestamp: String = LocalDateTime.now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))

      queryString = s"CALL audit_db.public.usp_start_fund_audit('${startTimestamp}','${market}')"
      stat.executeQuery(queryString)

      queryString = s"CALL stg_fund_db.public.usp_calculate_percents_per_month('${typeOfLoad}','${market}','${startTimestamp}')"
      stat.executeQuery(queryString)

      queryString = s"CALL fund_db.public.usp_monthly_overview('${typeOfLoad}','${market}','${startTimestamp}')"
      stat.executeQuery(queryString)

      queryString = s"CALL audit_db.public.usp_end_fund_audit('${startTimestamp}')"
      stat.executeQuery(queryString)
    }
    catch {
      case e => {
        throw e
      }
    }
    finally {
      if (stat != null) {
        stat.close()
      }
    }
  }

  def etlFromSourceToStg(): Unit = {
    var stat: Statement = null
    try {
      stat = conn.createStatement()
      var queryString: String = null

      if (typeOfLoad == "full") {
        queryString =
          """TRUNCATE TABLE stg_fund_db.public.stg_fund_table;"""
        stat.executeUpdate(queryString)
      }
      //println(new File(dataDirectory).getPath)
      val directory = new File(dataDirectory).listFiles()
      //println(directory.mkString(";"))
      for (file <- directory) {
        if (file.isFile) {

          val startTimestamp: String = LocalDateTime.now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
          val checkSum = checkSumSha256(file.getPath)
          val checkResult = checkHashSum(checkSum)

          if (typeOfLoad == "full" || !checkResult) {
            queryString =
              """TRUNCATE TABLE landing_fund_db.public.temp_fund_table;"""
            stat.executeUpdate(queryString)
            val filename = file.getPath.replace("\\", "/").split("/").takeRight(1)(0)
            queryString =
              s"""COPY INTO landing_fund_db.public.temp_fund_table FROM @landing_fund_db.public.landing_fund_stage FILES = ('${filename}.gz');"""
            stat.executeQuery(queryString)
            queryString = s"CALL audit_db.public.usp_start_stg_audit('${file.getPath}','${checkSum}', '${startTimestamp}','${market}')"
            stat.executeQuery(queryString)
            queryString = s"CALL stg_fund_db.public.usp_etl_from_source_to_stg('$startTimestamp','$market')"
            stat.executeQuery(queryString)
            queryString = s"SELECT COUNT(*) FROM stg_fund_db.public.stg_fund_table WHERE loadDate = '$startTimestamp';"
            val rs = stat.executeQuery(queryString)
            var count: Long = 0
            if (rs.next()) {
              count = rs.getLong(1)
            }
            queryString = s"CALL audit_db.public.usp_end_stg_audit('${checkSum}','${startTimestamp}',${count})"
            stat.executeQuery(queryString)
          }
        }
      }
    }
    catch {
      case e => {
        throw e

      }
    }
    finally {
      if (stat != null) {
        stat.close()
      }
    }
  }

  def main(args: Array[String]): Unit = {
    var i = 0
    //println(args.mkString(";"))
    while (i < args.length) {
      args(i) match {
        case "-d" => {
          dataDirectory = args(i + 1)
          i += 1
        }
        case "-m" => {
          market = args(i + 1)
          i += 1
        }
        case "-t" => {
          if (args(i + 1) != "full" && args(i + 1) != "incremental") {
            throw new Exception("Type of load can be only full or incremental")
          }
          typeOfLoad = args(i + 1)
          i += 1
        }
        case _ => throw new Exception("Unknown parameter " + args(i))
      }
      i += 1
    }
    conn = SnowflakeConnector.getConnection()
    //println(conn)
    etlFromSourceToStg()
    etlFromStgToDestination()
  }
}
