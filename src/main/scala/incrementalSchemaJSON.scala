import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object incrementalSchemaJSON extends App{
  val spark = SparkSession.builder().master("local[*]").appName("json that changestheSchem").getOrCreate()
  val inputjsonDF = spark.read.option("multiline","true").json("D:\\Spark_Scala\\data\\sampleExplode.json")
  inputjsonDF.show()
  inputjsonDF.printSchema()

  //Ref:https://medium.com/datadriveninvestor/handling-complexity-in-big-data-process-nested-json-with-changing-schema-tags-699972dc17e4
  //Method 1
  import scala.util.Try
  import org.apache.spark.sql.DataFrame
  var counts=0
  for(name <- inputjsonDF.select("response.*").schema.fieldNames) {
    if (Try(name.toInt).isSuccess == true)
    {
      counts+=1
    }
  }
  println("Count" + counts)

  //Method 2 using Explode
  val inputjsonDFtmp = inputjsonDF.withColumn("temp",explode(array(col("response.*"))))
  inputjsonDFtmp.show()
  val inputjsonDFflat = inputjsonDFtmp.withColumn("Currency",explode(array(col("temp.currency"))))
    .withColumn("Rate",explode(array("temp.rate")))
  inputjsonDFflat.show()
  spark.stop()





}
