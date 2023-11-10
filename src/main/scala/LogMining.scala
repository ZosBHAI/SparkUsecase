import java.time.LocalDateTime
import java.time.format.{DateTimeFormatter, DateTimeParseException}

import org.apache.spark.sql.SparkSession

object LogMining extends App{
  case class Log(time: LocalDateTime, level: String)

  val logPattern = "^(.{19})\\s([A-Z]+).*".r
  val dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
  def parseLog(line: String): Option[Log] = {
    //println("inside Parse logs" + line)
    line match {
      case logPattern(timeString, level) => {
        println("Pattern Matched" + timeString)
        val timeOption = try {
          Some(LocalDateTime.parse(timeString, dateTimeFormatter))
        } catch {
          case _: DateTimeParseException => None
        }
        timeOption.map(Log(_, level))
      }
      case _ => {
        None
      }
    }
  }
  val spark = SparkSession.builder().appName("LogMiningRDD").master("local[*]").getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  val  logInput =   spark.sparkContext.textFile("D:\\Spark_Scala\\data\\exactly-once-logdata\\logFile.txt")
    val result = logInput.map(_.toString).flatMap(parseLog)
  println("Printing the Parse Log Content")
      result.foreach(println)

}
