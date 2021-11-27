import java.io.{File, FileInputStream}
import java.nio.file.{Files, Paths, StandardOpenOption}
import java.security.{DigestInputStream, MessageDigest}
import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneId}
import java.util.{Calendar, Date}

import scala.collection.mutable
import scala.io.Source

object ETLManager {

  var dataDirectory: String = null
  var market: String = null
  var typeOfLoad: String = null
  var auditManager: AuditManager = null
  val stgFundDataPath = "stgFundData.csv"
  val monthlyFundDataPath = "monthlyData.csv"
  var stgFundData: mutable.ArraySeq[(LocalDateTime, BigDecimal, BigDecimal, LocalDateTime, String)] = null

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

  def extractNewData(): (mutable.ArraySeq[(Int, Int, LocalDateTime, BigDecimal, BigDecimal)], mutable.ArraySeq[(String, String, BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal, Boolean, LocalDateTime)]) = {
    val lastUpdateDate = auditManager.getLastUpdateDate
    //println(lastUpdateDate)
    val successLoadDates = auditManager.getSuccessLoadDates(market)
    //println(successLoadDates.mkString(";"))
    if (successLoadDates.isEmpty) {
      return (null, null)
    }
    var monthlyData: mutable.ArraySeq[(String, String, BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal, Boolean, LocalDateTime)] = null
    var newData = stgFundData.filter(line => (successLoadDates contains line._4.withNano(0)) && (line._5 == market))
    if (typeOfLoad != "full" && lastUpdateDate != null) {
      val yearsToUpdate = newData.filter(line => {
        val (time, open, close, loadDate, market) = line
        loadDate.isAfter(lastUpdateDate)
      }).map(line => {
        val (time, open, close, loadDate, market) = line
        val cal = Calendar.getInstance()
        cal.setTime(Date.from(time.atZone(ZoneId.systemDefault()).toInstant))
        cal.get(Calendar.YEAR)
      }).distinct.toArray
      newData = newData.filter(line => {
        val (time, open, close, loadDate, market) = line
        val cal = Calendar.getInstance()
        cal.setTime(Date.from(time.atZone(ZoneId.systemDefault()).toInstant))
        yearsToUpdate contains cal.get(Calendar.YEAR)
      })
      monthlyData = Source.fromFile(monthlyFundDataPath).getLines().map(line => {
        val Array(rawMarket, rawYear, rawJan, rawFeb, rawMar, rawApril, rawMay, rawJune, rawJuly, rawAug, rawSep, rawOct, rawNov, rawDec, rawTotal, rawType, rawLoadDate) = line.split(";")
        (rawMarket, rawYear, BigDecimal(rawJan), BigDecimal(rawFeb), BigDecimal(rawMar), BigDecimal(rawApril), BigDecimal(rawMay),
          BigDecimal(rawJune), BigDecimal(rawJuly), BigDecimal(rawAug), BigDecimal(rawSep), BigDecimal(rawOct), BigDecimal(rawNov),
          BigDecimal(rawDec), BigDecimal(rawTotal), rawType.toBoolean, LocalDateTime.parse(rawLoadDate))
      }).filter(line => {
        val (rawMarket, rawYear, rawJan, rawFeb, rawMar, rawApril, rawMay, rawJune, rawJuly, rawAug, rawSep, rawOct, rawNov, rawDec, rawTotal, rawType, rawLoadDate) = line
        (rawMarket == market && (rawYear != "total" && !(yearsToUpdate contains rawYear.toInt)) || (rawMarket != market))
      }).toArray
    }
    val transformedNewData = newData.map(line => {
      val (time, open, close, loadDate, market) = line
      val cal = Calendar.getInstance()
      cal.setTime(Date.from(time.atZone(ZoneId.systemDefault()).toInstant))
      (cal.get(Calendar.YEAR), cal.get(Calendar.MONTH), time, open, close)
    })
    (transformedNewData, monthlyData)
  }

  def transformFromSgToDestination(newData: mutable.ArraySeq[(Int, Int, LocalDateTime, BigDecimal, BigDecimal)],
                                   monthlyData: mutable.ArraySeq[(String, String, BigDecimal, BigDecimal, BigDecimal,
                                     BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal,
                                     BigDecimal, BigDecimal, BigDecimal, Boolean, LocalDateTime)],
                                   startTimestamp: LocalDateTime):
  Array[(String, String, BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal,
    BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal, Boolean, LocalDateTime)] = {

    val openCloseData = newData.groupBy(line => {
      val (year, month, time, open, close) = line
      (year, month)
    }).map(line => {
      val (key, values) = line
      val (minYear, minMonth, minTime, minOpen, minClose) = values.reduceLeft((first, second) => {
        val (fyear, fmonth, ftime, fopen, fclose) = first
        val (syear, smonth, stime, sopen, sclose) = second
        if (ftime.isBefore(stime)) {
          first
        }
        else {
          second
        }
      })
      val (maxYear, maxMonth, maxTime, maxOpen, maxClose) = values.reduceLeft((first, second) => {
        val (fyear, fmonth, ftime, fopen, fclose) = first
        val (syear, smonth, stime, sopen, sclose) = second
        if (ftime.isAfter(stime)) {
          first
        }
        else {
          second
        }
      })
      (minYear, minMonth, minOpen, maxClose)
    })
    //println(openCloseData.toArray.foreach(//println))

    val sortedOpenCloseData = openCloseData.toList.sortWith((first, second) => {
      first._1 < second._1 || (first._1 == second._1 && first._2 < second._2)
    })
    val prevCloseData = sortedOpenCloseData.zip(List((null, null, null, null)) ++ sortedOpenCloseData.dropRight(1)).map(line => {
      val ((year, month, open, close), (_, _, prevOpen, prevClose)) = line
      if (prevClose != null)
        (year, month, open, close, prevClose)
      else
        (year, month, open, close, open)
    }).toArray
    ////println(prevCloseData)

    val percents = prevCloseData.flatMap(line => {
      val (year, month, open, close, prevClose) = line
      Array((year, month, (close - open) / open * BigDecimal(100), true), (year, month, (close - prevClose) / prevClose * BigDecimal(100), false))
    })
    //println(percents)

    val pivotTable = percents.groupBy(line => {
      val (year, month, percent, typeOfPreviousValue) = line
      (typeOfPreviousValue, year)
    }).map(line => {
      val ((typeOfPreviousValue, year), values) = line
      var jan, feb, march, april, may, june, july, aug, sep, oct, nov, dec, total: BigDecimal = 0
      for (value <- values) {
        val (year, month, percent, typeOfPreviousValue) = value
        month match {
          case 0 => jan += percent
          case 1 => feb += percent
          case 2 => march += percent
          case 3 => april += percent
          case 4 => may += percent
          case 5 => june += percent
          case 6 => july += percent
          case 7 => aug += percent
          case 8 => sep += percent
          case 9 => oct += percent
          case 10 => nov += percent
          case 11 => dec += percent
        }
        total += percent
      }
      (market, year.toString, jan, feb, march, april, may, june, july, aug, sep, oct, nov, dec, total, typeOfPreviousValue, startTimestamp)
    }).toArray

    var resultWithoutTotal = pivotTable
    if (monthlyData != null) {
      resultWithoutTotal = resultWithoutTotal.union(monthlyData).toArray
    }

    val total = resultWithoutTotal.filter(line => {
      val (rowMarket, year, jan, feb, march, april, may, june, july, aug, sep, oct, nov, dec, total, typeOfPreviousValue, rowStartTimestamp) = line
      rowMarket == market
    }).groupBy(line => {
      val (rowMarket, year, jan, feb, march, april, may, june, july, aug, sep, oct, nov, dec, total, typeOfPreviousValue, rowStartTimestamp) = line
      typeOfPreviousValue
    }).map(line => {
      val (_, values) = line
      val count = BigDecimal(values.length)
      val (rowMarket, year, jan, feb, march, april, may, june, july, aug, sep, oct, nov, dec, total, typeOfPreviousValue, rowStartTimestamp) = values.reduce((first, second) => (first._1, first._2, first._3 + second._3, first._4 + second._4, first._5 + second._5,
        first._6 + second._6, first._7 + second._7, first._8 + second._8, first._9 + second._9, first._10 + second._10, first._11 + second._11,
        first._12 + second._12, first._13 + second._13, first._14 + second._14, first._15 + second._15, first._16, first._17))
      (rowMarket, "total", jan / count, feb / count, march / count, april / count, may / count, june / count,
        july / count, aug / count, sep / count, oct / count, nov / count, dec / count, total / count, typeOfPreviousValue, rowStartTimestamp)
    })
    val resultWithTotal = resultWithoutTotal ++ total
    resultWithTotal
  }

  def etlFromStgToDestination(): Unit = {
    val startTimestamp = LocalDateTime.now()
    val parsedStartTimestamp: String = startTimestamp.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
    try {
      //auditManager.startFundAudit(parsedStartTimestamp, market)
      val (newData, monthlyData) = extractNewData()
      //println(newData)
      //println(monthlyData)
      val result = transformFromSgToDestination(newData, monthlyData, startTimestamp)
      val f = new File(monthlyFundDataPath)
      if (f.exists()) {
        f.delete()
      }
      result.foreach(line => Files.write(Paths.get(monthlyFundDataPath), (line.productIterator.mkString(";") + "\n").getBytes,
        StandardOpenOption.CREATE, StandardOpenOption.APPEND))
      //auditManager.endFundAudit(parsedStartTimestamp)
    }
    catch {
      case e => println(e)
    }
  }

  def etlFromSourceToStg(): Unit = {
    val parsedStartTimestamp = LocalDateTime.now()
    val startTimestamp: String = parsedStartTimestamp.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
    val directory = new File(dataDirectory)
    val files = directory.listFiles()

    try {
      val f = new File(stgFundDataPath)
      if (typeOfLoad == "full" && f.exists()) {
        f.delete()
      }

      if (typeOfLoad != "full" && Files.exists(Paths.get(stgFundDataPath))) {
        stgFundData = Source.fromFile(stgFundDataPath).getLines().map(line => {
          try {
            val Array(rawTime, rawOpen, rawClose, rawLoadDate, rawMarket) = line.split(";")
            val time = LocalDateTime.parse(rawTime)
            val open = BigDecimal(rawOpen)
            val close = BigDecimal(rawClose)
            val parsedStartTimestamp = LocalDateTime.parse(rawLoadDate)
            (time, open, close, parsedStartTimestamp, rawMarket)
          }
          catch {
            case _ => {
              (null, null, null, null, null)
            }
          }
        }).filter(line => line._1 != null && line._2 != null && line._3 != null && line._4 != null && line._5 != null && line._5 == market).toArray
      }

      val newFiles = files.filter(f => {
        f.isFile && (typeOfLoad == "full" || !auditManager.checkLog(checkSumSha256(f.getPath)))
      })
      //println(newFiles.mkString("Array(", ", ", ")"))
      newFiles.foreach(f => auditManager.startStgAudit(f.getPath, checkSumSha256(f.getPath), startTimestamp, market))

      val extractedSourceData = newFiles.flatMap(f => {
        Source.fromFile(f.getPath).getLines()
      })
      ////println(extractedSourceData.mkString("Array(", ", ", ")"))

      val transformedSourceData = extractedSourceData.map(line => {
        try {
          val Array(rawTime, rawOpen, rawHigh, rawLow, rawClose, rawVolume) = line.split(";")
          val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
          val time = LocalDateTime.ofInstant(format.parse(rawTime).toInstant, ZoneId.systemDefault())
          val open = BigDecimal(rawOpen.replace(",", "."))
          val close = BigDecimal(rawClose.replace(",", "."))
          (time, open, close, parsedStartTimestamp, market)
        }
        catch {
          case _ => {
            (null, null, null, null, null)
          }
        }
      }).filter(line => line._1 != null && line._2 != null && line._3 != null && line._4 != null && line._5 != null)

      ////println(transformedSourceData.mkString("Array(", ", ", ")"))

      if (stgFundData == null) {
        stgFundData = transformedSourceData.toArray
      }
      else {
        stgFundData = stgFundData.union(transformedSourceData)
      }

      //println(stgFundData.mkString(";"))

      transformedSourceData.foreach(line => Files.write(Paths.get(stgFundDataPath), (line.productIterator.mkString(";") + "\n").getBytes, StandardOpenOption.CREATE, StandardOpenOption.APPEND))
      newFiles.foreach(f => {
        val count = Source.fromFile(f.getPath).getLines().drop(1).length
        auditManager.endStgAudit(checkSumSha256(f.getPath), startTimestamp, count)
      })
      etlFromStgToDestination()
    }
    catch {
      case e => {
        throw e
      }
    }
  }

  def main(args: Array[String]): Unit = {
    var i = 0
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
    auditManager = new AuditManager()
    etlFromSourceToStg()
  }
}

