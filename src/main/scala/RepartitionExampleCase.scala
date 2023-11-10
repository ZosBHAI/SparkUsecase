import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * try in python
 * https://mungingdata.com/apache-spark/partitionby/
 */
object RepartitionExampleCase extends App{

  val spark = SparkSession.builder().master("local[*]").appName("RecordLinkage").getOrCreate()
  spark.sparkContext.setLogLevel("WARN")
  val inputDF = spark.read.option("inferSchema","true").
    option("header","true").csv("D:\\Spark_Scala\\data\\repartiton\\person_data.csv")
  //inputDF.show()

  import org.apache.spark.sql.functions.{rand,col}

  /*inputDF
    .repartition(8)
    .write.mode(SaveMode.Overwrite)
    .partitionBy("person_country")
    .csv("D:\\Spark_Scala\\data\\repartiton\\output_repartition")*/
  //import scala.util.Random
inputDF
    .repartition(8,col("person_country"), rand)
    .write.mode(SaveMode.Overwrite)
    .partitionBy("person_country")
    .csv("D:\\Spark_Scala\\data\\repartiton\\output_repartition")


  val countDF = inputDF.groupBy("person_country").count()
  countDF.show()
  val desiredRowsPerPartition = 10
  val joinedDF = inputDF
    .join(countDF, Seq("person_country"))
    .withColumn(
      "my_secret_partition_key",
      (rand(10) * col("count") / desiredRowsPerPartition).cast(IntegerType)
    )

  joinedDF.show()


}
