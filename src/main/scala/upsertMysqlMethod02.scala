import java.util.Properties
import java.sql._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
object upsertMysqlMethod02 extends App {

  /*Using the mysql below construt
  * INSERT ...ON DUPLICATE KEY UPDATE
  * TEXT datatype in Mysql cannot be used for index
  * Effienct method for updating and inserting
  * Note:advisable  not to use above construts when there are multiple rows are to be updated.This is because only one row is updated when multiple rows has
      to be updated*/
  val spark = SparkSession.builder().master("local[*]").appName("Upsert Insert Update").getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

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
  val updatemysqlDF = spark.read.option("header","true").csv("D:\\Spark_Scala\\data\\JDBCsample\\updatedPayment01.csv")
  updatemysqlDF.show()
  updatemysqlDF.printSchema()

  //broadcast the connect
  val brConnect = spark.sparkContext.broadcast(connectionprop)

  //import spark.implicits._
  //Upsert
  updatemysqlDF.repartition(3).foreachPartition(partition => {
    println("Inside Paartition")
    val connectionProperties = brConnect.value
    val jdbcURL = connectionProperties.getProperty("jdbcUrl")
    val username = connectionProperties.getProperty("user")
    val password = connectionProperties.getProperty("password")

    //Register JDBC to dynamically load driver's class file into memory
    Class.forName(jdbcDriver)

    val dbc: Connection = DriverManager.getConnection(jdbcURL, username, password)
    //dbc.setAutoCommit(false)
    val batchSize = 2
    var pst: PreparedStatement = null

    //Browse through eadh roe in the partition
    partition.grouped(batchSize).foreach(batch => {
      println("Inside batch")
      batch.foreach{row => {
        // batch.foreach{row => {
        println("Inside row")
        //get the  column detaisl
        val physicianidIdx = row.fieldIndex("physician_id")
        val physician_id = row.getString(physicianidIdx)

        val recordidIdx = row.fieldIndex("record_id")
        val record_id = row.getString(recordidIdx)

        val payerIdx = row.fieldIndex("payer")
        val payer = row.getString(payerIdx)

        val nopIdx = row.fieldIndex("Nature_of_payment")
        val nop = row.getString(nopIdx)
        val sqlInsertDuplicate = " insert into  mydb.patient (physician_id,record_id,payer,Nature_of_payment) values (?,?,?,?) on " +
          "duplicate key update record_id = values(record_id),payer = values(payer),Nature_of_payment = values(Nature_of_payment)"
        println("Query" + sqlInsertDuplicate)
        pst = dbc.prepareStatement(sqlInsertDuplicate)
        pst.setString(1, physician_id)
        pst.setString(2, record_id)
        pst.setString(3, payer)
        pst.setString(4, nop)
        //pstmt.setString(5, record_id)
       //pstmt.setString(6, payer)
        //pstmt.setString(7, nop)
        println("physicianID ======>" + physician_id)
        pst.addBatch()

      }
        println("execute the batch")
        pst.executeBatch()}
      //}
      //println("execute the batch")
      //pst.executeBatch()
      //dbc.commit()
     //pst.close()
    })

    dbc.close()
  })

}
