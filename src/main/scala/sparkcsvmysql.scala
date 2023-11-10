
import org.apache.spark.sql.{SaveMode, SparkSession}
import java.sql._
import java.util.Properties

import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions


object sparkcsvmysql extends App {

  val spark = SparkSession.builder().appName("MysqlExample").master("local[*]").getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  val dfcsv = spark.read.option("header","true").csv("D:\\Spark_Scala\\data\\JDBCsample\\school.csv")
  dfcsv.show()
  dfcsv.printSchema()


  //loading the csv data to mysql table
  val user = "root"
  val passwd = "godgodgod59"
  //val  url = "jdbc:mysql://127.0.0.1:3306/?serverTimezone=ESTSEDT"
  val  url = "jdbc:mysql://127.0.0.1:3306/mydb"
  val jdbcDriver = "com.mysql.cj.jdbc.Driver"


  val connectionprop = new Properties()


  connectionprop.put("user","root")
  connectionprop.put("password",passwd)
  connectionprop.put("jdbcUrl",url)
  connectionprop.put("jdbcDriver",jdbcDriver)
  connectionprop.put("dbname","mydb")



  //Register JDBC to dynamically load driver's class file into memory

  Class.forName(jdbcDriver)

  //Write  to table from dataframe
  /*Issue faced -  Intially when i declared all the column as varchar ; there we no rows inserted into the table
  * Column Payer is of type TeXT, so when i changed the datatype in mysql from varchar to text -it worked
  *
  *
  * */

    println("Writing to table")
  dfcsv.write.mode(SaveMode.Append).jdbc(url=url,table = "patient",connectionprop)


}
