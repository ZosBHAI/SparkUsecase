import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.functions.expr

object CSVtoJSONusingDS extends App {

//Define the JSON Schema
case class Payment(physician_id: String, date_payment: String, record_id: String, payer: String, amount: Double, physician_specialty: String, nature_of_payment: String) extends Serializable

  case class PaymentwId(_id: String, physician_id: String, date_payment: String, payer: String, amount: Double, physician_specialty: String,
                        nature_of_payment: String) extends Serializable

  def createPaymentwId(p: Payment): PaymentwId = {
    val id = p.physician_id + '_' + p.date_payment + '_' + p.record_id
    PaymentwId(id, p.physician_id, p.date_payment, p.payer, p.amount, p.physician_specialty, p.nature_of_payment)
  }

  //Reading the CSV file
val spark = SparkSession.builder().appName("CSVtoJson").master("local[*]").getOrCreate()
  val inputDF = spark.read.option("header","true").csv("D:\\Spark_Scala\\data\\mapr\\payment\\payments.csv")
  spark.sparkContext.setLogLevel("ERROR")

  //Select   the list of columns
  import spark.implicits._
  val amountConversionDF = inputDF.withColumn("Amount", col("Total_Amount_of_Payment_USDollars").
                            cast("Double"))
  amountConversionDF.createOrReplaceTempView("payments0324")
  val paymentInputDS = spark.sql(
    """ select Physician_Profile_ID as physician_id, Date_of_Payment as
      |date_payment,record_id,Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name as payer,  amount, Physician_Specialty,
      |Nature_of_Payment_or_Transfer_of_Value as Nature_of_payment
      |from payments0324
      |""".stripMargin).as[Payment]

  //Alternative to above query
  val alterativeDF = amountConversionDF.selectExpr("Physician_Profile_ID as physician_id " ,
    " Date_of_Payment as date_payment" ,"record_id","Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name as payer",
    " amount","Physician_Specialty","Nature_of_Payment_or_Transfer_of_Value as Nature_of_payment"
  )

  //what are the number of payments with payment > 1000 with count
  alterativeDF.filter("amount > 1000").groupBy("Nature_of_payment").count().orderBy($"count".desc)

  //what are the top 5 nature of payment  by count
  alterativeDF.groupBy("Nature_of_payment").count().orderBy($"count".desc).show(5)

  //top 5 nature of payment by total amount
  alterativeDF.groupBy("Nature_of_payment").agg(expr("sum(amount) as total")).orderBy($"total".desc)

  //top 5 physician speciality by total amount where the physician speciality have null value;it should be removed
  alterativeDF.where($"Physician_Specialty".isNotNull).groupBy("Physician_Specialty").agg(expr("sum(amount) as total")).orderBy($"total".desc)



  paymentInputDS.show(10)
  paymentInputDS.write.option("header","true").mode(SaveMode.Overwrite).csv("D:\\Spark_Scala\\data\\mapr\\payment\\csvoutput")

/*
  //converting to JSON
  val jsonDS = paymentInputDS.map(x => createPaymentwId(x)).toJSON
  //write the JSON to file
  jsonDS.write.json("D:\\Spark_Scala\\data\\mapr\\payment\\output")

*/
}
