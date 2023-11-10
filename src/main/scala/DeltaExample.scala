import org.apache.spark.sql.SparkSession

object DeltaExample extends App {

  val spark = SparkSession.builder().appName("DeltaExample").master("local[*]").getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
 /* val deltacsv = spark.read.option("header","true").option("inferschema","true").csv("D:\\Spark_Scala\\data\\DeltaExample\\deltaSample.csv")
  deltacsv.show()
  deltacsv.printSchema()
  deltacsv.write.format("parquet").mode("append").save("D:\\Spark_Scala\\data\\DeltaExample\\output\\")
  val df2 = spark.read.parquet("D:\\Spark_Scala\\data\\DeltaExample\\output\\")*/
   /*spark.sparkContext.setLogLevel("ERROR")
  case class addresss (name: String ,gender : String, age:Int, area:String,city:String)
  val jsonDF = spark.read.json("D:\\Spark_Scala\\data\\JSON\\addresss.json")
  jsonDF.show()
  jsonDF.filter("age = 20").show()
  import spark.implicits._
  jsonDF.as[addresss].filter(x=>x.age > 20).show()*/

  val ouptDF = spark.read.parquet("C:\\pyspark\\outputStreaming")
  ouptDF.show(false)
  println("Number of rows" + ouptDF.count())
  



}
