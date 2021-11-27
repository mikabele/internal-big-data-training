import java.sql.{Connection, DriverManager}
import scala.io.Source

object SnowflakeConnector {
  def getConnection() : Connection ={
    val key_values = Source.fromFile("configs/snowflake_configs.txt").getLines().map(line=> line.split("=")).toArray
    val keys = key_values.map(line=>line(0)).toArray
    val values= key_values.map(line=>line(1)).toArray
    val configs = keys.zip(values).toMap
    DriverManager.getConnection(s"jdbc:snowflake://${configs("ACCOUNT")}.snowflakecomputing.com/", configs("USERNAME"), configs("SNOWSQL_PWD"));
  }
}
