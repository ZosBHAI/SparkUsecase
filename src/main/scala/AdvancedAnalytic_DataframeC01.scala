import AdvancedAnalytic_DataframeC01.{schemadf, summarydf}
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.SizeEstimator
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.sql.types._

object AdvancedAnalytic_DataframeC01 extends App {
  val spark = SparkSession.builder().master("local[*]").appName("RecordLinkage").getOrCreate()
  spark.sparkContext.setLogLevel("WARN")
 /* val inputrdd = spark.sparkContext.textFile("D:\\Spark_Scala\\data\\AdvancedAnalytics\\donation\\block_10")
  val rddsize = inputrdd.collect()
  println("RDD Size" + SizeEstimator.estimate(inputrdd))
  println("RDD Size" + SizeEstimator.estimate(rddsize))
  inputrdd.take(10).foreach(println)*/
  val inputdf =  spark.read.option("inferschema","true").option("nullValue","?").option("header" , "true").
    csv("D:\\Spark_Scala\\data\\AdvancedAnalytics\\donation\\block_10")
 println(" the total number of partitions are "+ inputdf.rdd.getNumPartitions)

  val sampledf = inputdf.show(10)
  /*val dsSize = inputdf.collect()
  println("Dataframe Size" + SizeEstimator.estimate(inputdf))
  println("Dataframe Size" + SizeEstimator.estimate(dsSize))*/
  //inputdf.cache()
  val summarydf = inputdf.summary()
  //val summarydf = sampledf.summary()
  val schemadf = summarydf.schema
 /* schemadf.fields.foreach(println)
  println("print the schema Value" + schemadf(0).name)
  println("print the schema Value" + schemadf(1).name)*/

  summarydf.collect().foreach(row1 => println(row1))
 /*import spark.implicits._
  val longFormDF =  summarydf.flatMap {row =>
   val metric = row.getString(0)
   (1 until row.size).map {i =>
    (metric, schemadf(i).name, row.getString(i).toDouble)
   }
  }.toDF("metric","ColumnName","value")*/

 val matchesDFSummary = inputdf.filter("is_match == true").describe()
 val missDFSummary = inputdf.filter("is_match == false").describe()


 println("Match Dataframe Summary is")
 matchesDFSummary.show()
 val matchSummaryT = pivotSummary(matchesDFSummary)
 val missSummaryT = pivotSummary(missDFSummary)
 println("MatchSummaryT and MissSummaryT")
 matchSummaryT.show()

 def pivotSummary(desc:DataFrame):DataFrame = {
  val summarydf = desc.summary()
  val schemadf = desc.summary().schema
  import desc.sparkSession.implicits._
  val longFormDF = summarydf.flatMap { row =>
   val metric = row.getString(0)
   (1 until row.size).map { i =>
    (metric, schemadf(i).name, row.getString(i).toDouble)
   }
  }.toDF("metric", "ColumnName", "value")
  val wideDF = longFormDF.groupBy("ColumnName").pivot("metric", Seq("count", "min", "max", "stddev", "mean")).
    agg(first("value"))
  return wideDF

 }
 val inputdf1 = inputdf.repartition(10)
inputdf1.write.csv("D:\\Spark_Scala\\data\\AdvancedAnalytics\\donation\\output")

}
