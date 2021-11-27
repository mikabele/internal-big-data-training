import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.io.File
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import java.nio.file.{Files, Paths}
import java.text.SimpleDateFormat
import java.util.Date

def startAudit(startTimestamp: String): Unit = {
  val fs = FileSystem.get(new Configuration())
  var rddStgAudit = sc.textFile("/tmp/audit/stg_audit_table/part*").map(line => line.split(";"))
  val rddNewStartRecord = sc.parallelize(Array(Array(startTimestamp, null, "load data from landing to staging", 0.toString)))
  rddStgAudit = rddStgAudit.union(rddNewStartRecord)

  rddStgAudit.map(line => line.mkString(";")).saveAsTextFile("/tmp/tmp_file")
  fs.delete(new Path("/tmp/audit/stg_audit_table"), true)
  fs.rename(new Path("/tmp/tmp_file"), new Path("/tmp/audit/stg_audit_table"))
}

def endAudit(startTimestamp: String, count: String): Unit = {
  val fs = FileSystem.get(new Configuration())
  var rddStgAudit = sc.textFile("/tmp/audit/stg_audit_table/part*").map(line => line.split(";"))
  val endTimestamp = LocalDateTime.now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
  val rddNewEndRecord = sc.parallelize(Array(Array(startTimestamp, endTimestamp, "load data from landing to staging", count)))
  rddStgAudit = rddStgAudit.filter(line => line(0) != startTimestamp)
  rddStgAudit = rddStgAudit.union(rddNewEndRecord)

  rddStgAudit.map(line => line.mkString(";")).saveAsTextFile("/tmp/tmp_file")
  fs.delete(new Path("/tmp/audit/stg_audit_table"), true)
  fs.rename(new Path("/tmp/tmp_file"), new Path("/tmp/audit/stg_audit_table"))
}

def etlFromLandingToStaging(full: Boolean): Unit = {
  val rddStgAudit = sc.textFile("/tmp/audit/stg_audit_table/part*").map(line => line.split(";"))
  val recordCount = rddStgAudit.filter(line => line(1) != "null").count()
  val fs = FileSystem.get(new Configuration())
  val startTimestamp = LocalDateTime.now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
  var lastStartDate: Date = null
  if (recordCount != 0) {
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    lastStartDate = format.parse(rddStgAudit.filter(line => line(1) != "null").collect().last(1))
  }
  startAudit(startTimestamp)
  val rddLandingFund = sc.textFile("/tmp/landing_fund/landing_fund_table/part*").map(line => line.split(";"))
  val rddLandingAudit = sc.textFile("/tmp/audit/landing_audit_table/part*").map(line => line.split(";"))
  val rddNewLandingFund = rddLandingFund.map(line => {
    val market = line(7)
    val loadDate = line(6)
    val time = line(0)
    val open = line(1)
    val close = line(2)
    ((market, loadDate), (time, open, close))
  })
  val rddPrepareToJoinAudit = rddLandingAudit.map(line => {
    val startTime = line(2)
    val market = line(3)
    val endTime = line(6)
    ((market, startTime), endTime)
  })
  var rddJoinedTimestamp = rddNewLandingFund.join(rddPrepareToJoinAudit).filter(line => {
    val endTime = line._2._2
    endTime != null
  })
  if (!full && recordCount != 0) {
    rddJoinedTimestamp = rddJoinedTimestamp.filter(line => {
      val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val startTime = format.parse(line._1._2)
      startTime.after(lastStartDate)
    })
  }
  val rddClearFund = rddJoinedTimestamp.map(line => {
    try {
      val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val time = format.parse(line._2._1._1)
      val open: Float = line._2._1._2.replace(",", ".").toFloat
      val close: Float = line._2._1._3.replace(",", ".").toFloat
      val market = line._1._1
      val startTimestamp = format.parse(line._1._2)
      (time, open, close, market, startTimestamp)
    }
    catch {
      case e => {
        (null, 0f, 0f, null, null)
      }
    }
  })
  val count = rddClearFund.count()
  var rddStgFund : org.apache.spark.rdd.RDD[(java.util.Date, Float, Float, String, java.util.Date)] = null
  val marketYearPairs = rddClearFund.map(line => (line._3, line._4)).distinct().collect()
  if (full) {
    rddStgFund = rddClearFund
  }
  else {
    rddStgFund = sc.textFile("/tmp/stg_fund/stg_fund_table/part*").map(line => {
      try {
        val values = line.split(";")
        val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val time = format.parse(values(0))
        val open: Float = values(1).toFloat
        val close: Float = values(2).toFloat
        val market = values(3)
        val startTimestamp = format.parse(values(4))
        (time, open, close, market, startTimestamp)
      }
      catch {
        case e => {
          (null, 0f, 0f, null, null)
        }
      }
    }).filter(line => !(marketYearPairs contains(line._3, line._4)))
    rddStgFund = rddStgFund.union(rddClearFund)
  }
  rddStgFund = rddStgFund.filter(line => line._1 != null && line._2 != 0 && line._3 != 0 && line._4 != null && line._5 != null)
  rddStgFund.map(line => {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val date = dateFormat.format(line._1)
    val startTimestamp = dateFormat.format(line._5)
    (date, line._2, line._3, line._4, startTimestamp).productIterator.mkString(";")
  }).saveAsTextFile("/tmp/tmp_file")
  fs.delete(new Path("/tmp/stg_fund/stg_fund_table"), true)
  fs.rename(new Path("/tmp/tmp_file"), new Path("/tmp/stg_fund/stg_fund_table"))
  endAudit(startTimestamp, count.toString)
}

//def main(args: Array[String]): Unit = {
//  var FULL: Boolean = false
//  var INCREMENTAL: Boolean = false
//  for (i <- args.indices) {
//    args(i) match {
//      case "-i" || "--incremental" => {
//        INCREMENTAL = true
//      }
//      case "-f" || "--full" => {
//        FULL = true
//      }
//    }
//  }
//  if (FULL == INCREMENTAL) {
//    throw new Exception("Choose only one load option")
//  }
etlFromLandingToStaging(false)
//}