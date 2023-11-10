import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object WorkingExampleScala extends App{
  val spark = SparkSession.builder().master("local[*]").appName("AAD_01").getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  val parsed = spark.read.option("inferschema","true").option("header","true").option("nullValue","?").
    format("csv").load("D:\\Spark_Scala\\data\\AdvancedAnalytics\\donation\\block_10")

  val summaryDF = parsed.describe()

  val schemaOfSummary = summaryDF.schema
  import spark.implicits._
  val  longDf = summaryDF.flatMap(x=>
  {
    val metrics = x.getString(0)
    (1 until x.size).map(i =>   {
        (metrics,schemaOfSummary(i).name,x.getString(i).toDouble)
    })

  }).toDF("metrics","columnName","value")
  //longDf.show(20)
  ///Converting it back to WideDF
  val wideDF = longDf.groupBy("columnName").pivot("metrics",Seq("count", "mean", "stddev", "min", "max")).agg(first("value"))
  //wideDF.show(20)

  //creating function
  def pivotSummary(desc:DataFrame):DataFrame = {
    //val summaryDF = desc.describe()
    val schemaOfSummary = desc.schema
    import desc.sparkSession.implicits._
    val  longDf = desc.flatMap(x=>
    {
      val metrics = x.getString(0)
      (1 until x.size).map(i =>   {
        (metrics,schemaOfSummary(i).name,x.getString(i).toDouble)
      })

    }).toDF("metrics","columnName","value")
    val wideDF = longDf.groupBy("columnName").pivot("metrics",Seq("count", "mean", "stddev", "min", "max")).agg(first("value"))
    wideDF

  }
  val matchesDFSummary = parsed.filter("is_match == true").describe()
  val matchSummaryT = pivotSummary(matchesDFSummary)
  matchSummaryT.show(20)
}
