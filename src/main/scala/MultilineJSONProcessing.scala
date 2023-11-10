import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructField, StructType,IntegerType,StringType,ArrayType}

object MultilineJSONProcessing extends App {

  val spark = SparkSession.builder().appName("MultiLine").master("local[*]").getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  val multilineJSON = spark.read.option("multiline","True").json("D:\\Spark_Scala\\data\\JSON\\multiLine.json")
  multilineJSON.printSchema()
  multilineJSON.show()
  import spark.implicits._

  val multillineSchema = new  StructType().add("RecordNumber", IntegerType)
    .add("Zipcode", IntegerType)
    .add("ZipCodeType",StringType)
    .add("City",StringType)
    .add("State",StringType)
   val multijsondataDS = Seq("""[{
                          "RecordNumber": 2,
                          "Zipcode": 704,
                         "ZipCodeType": "STANDARD",
                          "City": "PASEO COSTA DEL SUR",
                           "State": "PR"
                       },
                        {
                          "RecordNumber": 10,
                          "Zipcode": 709,
                         "ZipCodeType": "STANDARD",
                          "City": "BDA SAN LUIS",
                          "State": "PR"
                       }]""").toDS()


  //multijsondataDS.show()
  val multilinejson = spark.read.schema(multillineSchema).option("multiline","true").json(multijsondataDS)
  multilinejson.show()

}
