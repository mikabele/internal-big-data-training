import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.io.File
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import java.nio.file.{Files, Paths}
import java.text.SimpleDateFormat
import java.util.Date
import java.util.Calendar

def startAudit(startTimestamp: String, market: String): Unit = {
  val fs = FileSystem.get(new Configuration())
  var rddFundAudit = sc.textFile("/tmp/audit/fund_audit_table/part*").map(line => line.split(";"))
  val rddNewFundAuditRecord = sc.parallelize(Array(Array(startTimestamp, null, "monthly overview", market)))
  rddFundAudit = rddFundAudit.union(rddNewFundAuditRecord)

  rddFundAudit.map(line => line.mkString(";")).saveAsTextFile("/tmp/tmp_file")
  fs.delete(new Path("/tmp/audit/fund_audit_table"), true)
  fs.rename(new Path("/tmp/tmp_file"), new Path("/tmp/audit/fund_audit_table"))
}

def endAudit(startTimestamp: String, market: String): Unit = {
  val fs = FileSystem.get(new Configuration())
  var rddFundAudit = sc.textFile("/tmp/audit/fund_audit_table/part*").map(line => line.split(";"))
  val endTimestamp = LocalDateTime.now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
  val rddNewEndRecord = sc.parallelize(Array(Array(startTimestamp, endTimestamp, "monthly overview", market)))
  rddFundAudit = rddFundAudit.filter(line => line(0) != startTimestamp)
  rddFundAudit = rddFundAudit.union(rddNewEndRecord)

  rddFundAudit.map(line => line.mkString(";")).saveAsTextFile("/tmp/tmp_file")
  fs.delete(new Path("/tmp/audit/fund_audit_table"), true)
  fs.rename(new Path("/tmp/tmp_file"), new Path("/tmp/audit/fund_audit_table"))
}

def etlFromStgToDestination(full: Boolean, market: String, openClose: Boolean): Unit = {
  val startTimestamp = LocalDateTime.now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
  val fs = FileSystem.get(new Configuration())
  startAudit(startTimestamp, market)
  var rddMonthly: org.apache.spark.rdd.RDD[(String, String, Float, Float, Float, Float, Float, Float, Float, Float, Float, Float, Float, Float, Float, Boolean, String)] = null
  var rddStgFund = sc.textFile("/tmp/stg_fund/stg_fund_table/part*").map(line => {
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
  })
  if (!full) {
    rddMonthly = sc.textFile("/tmp/fund/monthly_table/part*").map(line => {
      val values = line.split(";")
      (values(0), values(1), values(2).toFloat, values(3).toFloat, values(4).toFloat, values(5).toFloat,
        values(6).toFloat, values(7).toFloat, values(8).toFloat, values(9).toFloat, values(10).toFloat,
        values(11).toFloat, values(12).toFloat, values(13).toFloat, values(14).toFloat, values(15).toBoolean, values(16))
    })
    val dates = rddMonthly.filter(line => {
      val monthlyMarket = line._1
      val monthlyOpenClose = line._16
      monthlyMarket == market && monthlyOpenClose == openClose
    }).map(line => {
      val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val time = format.parse(line._17)
      time
    }).collect()
    var lastDate: Date = null
    if (!dates.isEmpty) {
      lastDate = dates.reduceLeft((x, y) => {
        if (x.after(y)) {
          x
        }
        else {
          y
        }
      })
    }
    val years = rddStgFund.filter(line => {
      val stgMarket = line._4
      val startTimestamp = line._5
      stgMarket == market && (lastDate == null || startTimestamp.after(lastDate))
    }).map(line => {
      val cal = Calendar.getInstance
      cal.setTime(line._1)
      cal.get(Calendar.YEAR)
    }).distinct().collect()
    rddMonthly = rddMonthly.filter(line => ! {
      val monthlyMarket = line._1
      val year = line._2
      (year == "total" || (years contains year.toInt)) && monthlyMarket == market
    })
    rddStgFund = rddStgFund.filter(line => {
      val stgMarket = line._4
      val cal = Calendar.getInstance
      cal.setTime(line._1)
      (years contains cal.get(Calendar.YEAR)) && stgMarket == market
    })
  }
  else {
    rddStgFund = rddStgFund.filter(line => {
      val stgMarket = line._4
      stgMarket == market
    })
  }
  val rddNewData = rddStgFund.map(line => {
    val cal = Calendar.getInstance
    cal.setTime(line._1)
    ((cal.get(Calendar.YEAR), cal.get(Calendar.MONTH)), (line._1, line._2, line._3, line._4))
  })
  val rddMinDateInMonth = rddNewData.reduceByKey((x, y) => {
    if (x._1.before(y._1)) {
      x
    }
    else {
      y
    }
  })
  val rddMaxDateInMonth = rddNewData.reduceByKey((x, y) => {
    if (x._1.after(y._1)) {
      x
    }
    else {
      y
    }
  })
  val rddOpenClose = rddMinDateInMonth.join(rddMaxDateInMonth).sortByKey()
  var prevClose: Float = -1
  val rddPrevClose = rddOpenClose.map(line => {
    val temp = prevClose
    prevClose = line._2._2._3
    if (temp != -1) {
      (line._1, line._2, temp)
    }
    else {
      (line._1, line._2, line._2._1._2)
    }
  })
  var rddPercents: org.apache.spark.rdd.RDD[(Int, (Int, Float))] = null
  if (openClose) {
    rddPercents = rddOpenClose.map(line => (line._1._1, (line._1._2, (line._2._2._3 - line._2._1._2) / line._2._1._2)))
  }
  else {
    rddPercents = rddPrevClose.map(line => (line._1._1, (line._1._2, (line._2._2._3 - line._3) / line._3)))
  }
  val rddPivotTable = rddPercents.groupByKey().mapValues(value => value.toList).map(line => {
    var jan, feb, march, april, may, june, july, aug, sep, oct, nov, dec, total: Float = 0
    for (value <- line._2) {
      value._1 match {
        case 0 => jan += value._2
        case 1 => feb += value._2
        case 2 => march += value._2
        case 3 => april += value._2
        case 4 => may += value._2
        case 5 => june += value._2
        case 6 => july += value._2
        case 7 => aug += value._2
        case 8 => sep += value._2
        case 9 => oct += value._2
        case 10 => nov += value._2
        case 11 => dec += value._2
      }
      total += value._2
    }
    (market, line._1.toString, jan, feb, march, april, may, june, july, aug, sep, oct, nov, dec, total, openClose, startTimestamp)
  })
  var rddResult = rddPivotTable
  if (rddMonthly != null) {
    rddResult = rddResult.union(rddMonthly)
  }
  val count = rddResult.filter(line => {
    val monthlyMarket = line._1
    val monthlyOpenClose = line._16
    monthlyMarket == market && monthlyOpenClose == openClose
  }).count()
  val rddTotal = sc.parallelize(Array(rddResult.filter(line => {
    val monthlyMarket = line._1
    val monthlyOpenClose = line._16
    monthlyMarket == market && monthlyOpenClose == openClose
  }).reduce((x, y) => (market, "total", (x._3 + y._3) / count, (x._4 + y._4) / count, (x._5 + y._5) / count,
    (x._6 + y._6) / count, (x._7 + y._7) / count, (x._8 + y._8) / count, (x._9 + y._9) / count, (x._10 + y._10) / count,
    (x._11 + y._11) / count, (x._12 + y._12) / count, (x._13 + y._13) / count, (x._14 + y._14) / count,
    (x._15 + y._15) / count, openClose, startTimestamp))))
  rddResult = rddResult.union(rddTotal)
  rddResult.map(line => line.productIterator.mkString(";")).saveAsTextFile("/tmp/tmp_file")
  fs.delete(new Path("/tmp/fund/monthly_table"), true)
  fs.rename(new Path("/tmp/tmp_file"), new Path("/tmp/fund/monthly_table"))
  endAudit(startTimestamp, market)
}

//def main(args: Array[String]): Unit = {
//  var MARKET: String = null
//  var FULL: Boolean = false
//  var INCREMENTAL: Boolean = false
//  var OPEN_CLOSE: Boolean = false
//  var CLOSE_CLOSE: Boolean = false
//  for (i <- args.indices) {
//    args(i) match {
//      case "-m" || "--market" => {
//        MARKET = args(i + 1)
//      }
//      case "-i" || "--incremental" => {
//        INCREMENTAL = true
//      }
//      case "-f" || "--full" => {
//        FULL = true
//      }
//      case "-oc" || "--open_close" => {
//        OPEN_CLOSE = true
//      }
//      case "-cc" || "--close_close" => {
//        CLOSE_CLOSE = true
//      }
//    }
//  }
//  if (FULL == INCREMENTAL) {
//    throw new Exception("Choose only one load option")
//  }
//  if (OPEN_CLOSE == CLOSE_CLOSE) {
//    throw new Exception("Choose only one previous value option")
//  }
etlFromStgToDestination(false, "XAUUSD", false)
//}