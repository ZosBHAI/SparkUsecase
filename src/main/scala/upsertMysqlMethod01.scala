import java.util.Properties
import java.sql._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
/*Using Select,Insert,Update
* TEXT datatype in Mysql cannot be used for index; so we should switch for Varchar
* */

object upsertMysqlMethod01 extends App {
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





  //Reading from Mysql table
/*
  val updatemysqlDF = spark.read.jdbc(url=url,table = "patient",connectionprop)
  */
val updatemysqlDF = spark.read.option("header","true").csv("D:\\Spark_Scala\\data\\JDBCsample\\updatedPayment01.csv")
  updatemysqlDF.show()
  updatemysqlDF.printSchema()

  //broadcast the connect
  val brConnect = spark.sparkContext.broadcast(connectionprop)

   //import spark.implicits._
  //Upsert
  updatemysqlDF.repartition(2).foreachPartition(partition =>{
    println("Inside Paartition")
    val connectionProperties = brConnect.value
    val jdbcURL = connectionProperties.getProperty("jdbcUrl")
    val username = connectionProperties.getProperty("user")
    val password = connectionProperties.getProperty("password")

    //Register JDBC to dynamically load driver's class file into memory
    Class.forName(jdbcDriver)

    val dbc:Connection = DriverManager.getConnection(jdbcURL,username,password)
    val batchSize = 2
    var st:PreparedStatement = null

    //Browse through eadh roe in the partition
    partition.grouped(batchSize).foreach(batch=> {
        println("Inside batch")
      //batch.foreach(row => {
        batch.foreach { row => {
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


          //check the dataframe record exist in table
          val whereCol = List("physician_id")
          val sqlSelect = " Select * from mydb.patient where physician_id =?"
          val pstmt: PreparedStatement = dbc.prepareStatement(sqlSelect)
          pstmt.setString(1, physician_id)

          val rs = pstmt.executeQuery()
          var count = 0
          while (rs.next()) {
            count = +1
          }
          var dbOper = "NULL"
          if (count > 0) {
            dbOper = "UPDATE"
          }
          else {
            dbOper = "INSERT"
          }

          //Prepare the statement for INSERT and UPDATE
          if (dbOper == "UPDATE") {
            val updateSqlString = "update mydb.patient SET record_id=?,payer=?,Nature_of_payment=? where physician_id =?"
            st = dbc.prepareStatement(updateSqlString)
            st.setString(1, record_id)
            st.setString(2, payer)
            st.setString(3, nop)
            st.setString(4, physician_id)
          }

          else if (dbOper == "INSERT") {
            val insertSqlString = "insert into  mydb.patient (physician_id,record_id,payer,Nature_of_payment) values (?,?,?,?)"
            st = dbc.prepareStatement(insertSqlString)
            st.setString(1, physician_id)
            st.setString(2, record_id)
            st.setString(3, payer)
            st.setString(4, nop)

          }
          st.addBatch() //add all the dataframe records to batch
          println("physicianID ======>" + physician_id + "========" + dbOper)

        }
          println("executeBatch")
          st.executeBatch()
        }

      })
    //})
    println("Partition")
   //dbc.commit()
    dbc.close()
  })



}
