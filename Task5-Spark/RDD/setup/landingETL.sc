import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.io.File
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import java.security.{MessageDigest, DigestInputStream}
import java.nio.file.{Files, Paths}

def checkSumSha256(path: Path): String = {
  val hdfs = FileSystem.get(new Configuration())

  val buffer = new Array[Byte](8192)
  val sha256 = MessageDigest.getInstance("SHA-256")

  val dis = new DigestInputStream(hdfs.open(path), sha256)
  try {
    while (dis.read(buffer) != -1) {}
  }
  finally {
    dis.close()
  }

  sha256.digest.map("%02x".format(_)).mkString
}

def startAudit(filename: String, checkSum: String, startTimestamp: String, market: String) : Unit = {
  val fs = FileSystem.get(new Configuration())
  var rddLandingAudit = sc.textFile("/tmp/audit/landing_audit_table/part*").map(line => line.split(";"))
  val rddNewStartRecord = sc.parallelize(Array(Array(filename, checkSum, startTimestamp, market, "load data from files", "landing_fund_db", null, null)))
  rddLandingAudit = rddLandingAudit.union(rddNewStartRecord)

  rddLandingAudit.map(line => line.mkString(";")).saveAsTextFile("/tmp/tmp_file")
  fs.delete(new Path("/tmp/audit/landing_audit_table"), true)
  fs.rename(new Path("/tmp/tmp_file"), new Path("/tmp/audit/landing_audit_table"))
}

def endAudit(filename: String, checkSum: String, startTimestamp: String, market: String, count: String) : Unit = {
  val fs = FileSystem.get(new Configuration())
  val endTimestamp = LocalDateTime.now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
  var rddLandingAudit = sc.textFile("/tmp/audit/landing_audit_table/part*").map(line => line.split(";"))
  val rddNewEndRecord = sc.parallelize(Array(Array(filename, checkSum, startTimestamp, market, "load data from files", "landing_fund_db", endTimestamp, count)))
  rddLandingAudit = rddLandingAudit.filter(line => line(2) != startTimestamp)
  rddLandingAudit = rddLandingAudit.union(rddNewEndRecord)

  rddLandingAudit.map(line => line.mkString(";")).saveAsTextFile("/tmp/tmp_file")
  fs.delete(new Path("/tmp/audit/landing_audit_table"), true)
  fs.rename(new Path("/tmp/tmp_file"), new Path("/tmp/audit/landing_audit_table"))
}

def etlFromSourceToLanding(market: String, full: Boolean): Unit = {
  var startTimestamp: String = null
  val fs = FileSystem.get(new Configuration())
  val files = fs.listStatus(new Path("/tmp/data"))
  var rddLandingFund: org.apache.spark.rdd.RDD[Array[String]] = null
  var count: Long = 0L
  var rddLandingAudit = sc.textFile("/tmp/audit/landing_audit_table/part*").map(line => line.split(";"))

  if (!full) {
    rddLandingFund = sc.textFile("/tmp/landing_fund/landing_fund_table/part*").map(line => line.split(";"))
  }

  for (file <- files) {
    startTimestamp = LocalDateTime.now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
    val checkSum = checkSumSha256(file.getPath)
    val checkLog = rddLandingAudit.filter(line => line(1) == checkSum).count()

    if (full || checkLog == 0) {
      startAudit(file.getPath.toString, checkSum, startTimestamp, market)

      if (rddLandingFund == null) {
        rddLandingFund = sc.textFile(file.getPath.toString).map(line => line.split(";")).map(line => line ++ Array(startTimestamp, market))
        count = rddLandingFund.count()
      }
      else {
        val rddTemp = sc.textFile(file.getPath.toString).map(line => line.split(";")).map(line => line ++ Array(startTimestamp, market))
        val yearMarketPairs = rddTemp.map(line => (line(0), line(6))).distinct().collect()
        rddLandingFund = rddLandingFund.filter(line => !(yearMarketPairs contains(line(0), line(6))))
        rddLandingFund = rddLandingFund.union(rddTemp)
        count = rddTemp.count()
      }
      rddLandingFund.map(line => line.mkString(";")).saveAsTextFile("/tmp/tmp_file")
      fs.delete(new Path("/tmp/landing_fund/landing_fund_table"), true)
      fs.rename(new Path("/tmp/tmp_file"), new Path("/tmp/landing_fund/landing_fund_table"))

      endAudit(file.getPath.toString, checkSum, startTimestamp, market, count.toString)
    }
  }
}

//def main(args: Array[String]): Unit = {
//  var MARKET: String = null
//  var FULL: Boolean = false
//  var INCREMENTAL: Boolean = false
//  for (i <- args.indices) {
//    args(i) match {
//      case "-m" => {
//        MARKET = args(i + 1)
//      }
//      case "-i" => {
//        INCREMENTAL = true
//      }
//      case "-f"=> {
//        FULL = true
//      }
//    }
//  }
//  if (FULL == INCREMENTAL) {
//    throw new Exception("Choose only one load option")
//  }
etlFromSourceToLanding("XAUUSD", false)
//}
