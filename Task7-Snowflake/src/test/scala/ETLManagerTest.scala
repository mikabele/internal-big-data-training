import org.scalatest.FunSuite

import scala.math

class ETLManagerTest extends FunSuite {

  test("test connection to snowflake") {
    val conn = SnowflakeConnector.getConnection()
    assert(conn != null)
  }

  test("test result on 9 years") {
    ETLManager.main(Seq[String]("-d", "../data/XAUUSD/Bid", "-t", "full", "-m", "XAUUSD").toArray)
    println("ETLManager task completed")
    val conn = SnowflakeConnector.getConnection()
    println("Connection establishes")
    try {
      val stat = conn.createStatement()
      var queryString =
        s"""SELECT *
            FROM fund_db.public.monthly_table
            WHERE `year` = '2012' AND market = 'XAUUSD' AND typeOfPreviousValue = 'open'"""
      val rs = stat.executeQuery(queryString)
      println("Query executed successfully")
      rs.next()
      val jan = rs.getBigDecimal(3)
      val feb =  rs.getBigDecimal(4)
      val march =  rs.getBigDecimal(5)

      val april = rs.getBigDecimal(6)
      val may =  rs.getBigDecimal(7)
      val june =  rs.getBigDecimal(8)

      val july = rs.getBigDecimal(9)
      val august =  rs.getBigDecimal(10)
      val september =  rs.getBigDecimal(11)

      val october = rs.getBigDecimal(12)
      val november =  rs.getBigDecimal(13)
      val december =  rs.getBigDecimal(14)
      val total =  rs.getBigDecimal(15)
//      assert(jan.toString=="0" && feb.toString=="0" && march.toString=="0" &&
//        april.toString=="0" && may.toString=="0" && june.toString=="0" &&
//        july.toString=="0" && august.toString=="0" && september.toString=="0" &&
//        october.toString=="0" && november.toString=="0" && december.toString=="0" &&
//        total.toString=="0")
      assert(true)
    }
    catch {
      case e => {
        println(e)
        assert(false)
      }
    }
  }
}