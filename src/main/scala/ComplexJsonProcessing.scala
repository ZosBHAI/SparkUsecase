import org.apache.spark.sql.types.{DoubleType, LongType, MapType, TimestampType}
import org.apache.spark.sql.{SaveMode, SparkSession}
//Ref:https://docs.databricks.com/_static/notebooks/complex-nested-structured.html
//get_json_object
object ComplexJsonProcessing extends App {
  val spark = SparkSession.builder().master("local[*]").appName("DatabricksComplexJSON").getOrCreate()
  spark.sparkContext.setLogLevel(logLevel = "INFO")
  import spark.implicits._

  case class DeviceData (id: Int, device: String)
  // create some sample data
  val eventsDS = Seq (
    (0, """{"device_id": 0, "device_type": "sensor-ipad", "ip": "68.161.225.1", "cca3": "USA", "cn": "United States", "temp": 25, "signal": 23, "battery_level": 8, "c02_level": 917, "timestamp" :1475600496 }"""),
    (1, """{"device_id": 1, "device_type": "sensor-igauge", "ip": "213.161.254.1", "cca3": "NOR", "cn": "Norway", "temp": 30, "signal": 18, "battery_level": 6, "c02_level": 1413, "timestamp" :1475600498 }"""),
    (2, """{"device_id": 2, "device_type": "sensor-ipad", "ip": "88.36.5.1", "cca3": "ITA", "cn": "Italy", "temp": 18, "signal": 25, "battery_level": 5, "c02_level": 1372, "timestamp" :1475600500 }"""),
    (3, """{"device_id": 3, "device_type": "sensor-inest", "ip": "66.39.173.154", "cca3": "USA", "cn": "United States", "temp": 47, "signal": 12, "battery_level": 1, "c02_level": 1447, "timestamp" :1475600502 }"""),
    (4, """{"device_id": 4, "device_type": "sensor-ipad", "ip": "203.82.41.9", "cca3": "PHL", "cn": "Philippines", "temp": 29, "signal": 11, "battery_level": 0, "c02_level": 983, "timestamp" :1475600504 }"""),
    (5, """{"device_id": 5, "device_type": "sensor-istick", "ip": "204.116.105.67", "cca3": "USA", "cn": "United States", "temp": 50, "signal": 16, "battery_level": 8, "c02_level": 1574, "timestamp" :1475600506 }"""),
    (6, """{"device_id": 6, "device_type": "sensor-ipad", "ip": "220.173.179.1", "cca3": "CHN", "cn": "China", "temp": 21, "signal": 18, "battery_level": 9, "c02_level": 1249, "timestamp" :1475600508 }"""),
    (7, """{"device_id": 7, "device_type": "sensor-ipad", "ip": "118.23.68.227", "cca3": "JPN", "cn": "Japan", "temp": 27, "signal": 15, "battery_level": 0, "c02_level": 1531, "timestamp" :1475600512 }"""),
    (8 ,""" {"device_id": 8, "device_type": "sensor-inest", "ip": "208.109.163.218", "cca3": "USA", "cn": "United States", "temp": 40, "signal": 16, "battery_level": 9, "c02_level": 1208, "timestamp" :1475600514 }"""),
    (9,"""{"device_id": 9, "device_type": "sensor-ipad", "ip": "88.213.191.34", "cca3": "ITA", "cn": "Italy", "temp": 19, "signal": 11, "battery_level": 0, "c02_level": 1171, "timestamp" :1475600516 }"""),
    (10,"""{"device_id": 10, "device_type": "sensor-igauge", "ip": "68.28.91.22", "cca3": "USA", "cn": "United States", "temp": 32, "signal": 26, "battery_level": 7, "c02_level": 886, "timestamp" :1475600518 }"""),
    (11,"""{"device_id": 11, "device_type": "sensor-ipad", "ip": "59.144.114.250", "cca3": "IND", "cn": "India", "temp": 46, "signal": 25, "battery_level": 4, "c02_level": 863, "timestamp" :1475600520 }"""),
    (12, """{"device_id": 12, "device_type": "sensor-igauge", "ip": "193.156.90.200", "cca3": "NOR", "cn": "Norway", "temp": 18, "signal": 26, "battery_level": 8, "c02_level": 1220, "timestamp" :1475600522 }"""),
    (13, """{"device_id": 13, "device_type": "sensor-ipad", "ip": "67.185.72.1", "cca3": "USA", "cn": "United States", "temp": 34, "signal": 20, "battery_level": 8, "c02_level": 1504, "timestamp" :1475600524 }"""),
    (14, """{"device_id": 14, "device_type": "sensor-inest", "ip": "68.85.85.106", "cca3": "USA", "cn": "United States", "temp": 39, "signal": 17, "battery_level": 8, "c02_level": 831, "timestamp" :1475600526 }"""),
    (15, """{"device_id": 15, "device_type": "sensor-ipad", "ip": "161.188.212.254", "cca3": "USA", "cn": "United States", "temp": 27, "signal": 26, "battery_level": 5, "c02_level": 1378, "timestamp" :1475600528 }"""),
    (16, """{"device_id": 16, "device_type": "sensor-igauge", "ip": "221.3.128.242", "cca3": "CHN", "cn": "China", "temp": 10, "signal": 24, "battery_level": 6, "c02_level": 1423, "timestamp" :1475600530 }"""),
    (17, """{"device_id": 17, "device_type": "sensor-ipad", "ip": "64.124.180.215", "cca3": "USA", "cn": "United States", "temp": 38, "signal": 17, "battery_level": 9, "c02_level": 1304, "timestamp" :1475600532 }"""),
    (18, """{"device_id": 18, "device_type": "sensor-igauge", "ip": "66.153.162.66", "cca3": "USA", "cn": "United States", "temp": 26, "signal": 10, "battery_level": 0, "c02_level": 902, "timestamp" :1475600534 }"""),
    (19, """{"device_id": 19, "device_type": "sensor-ipad", "ip": "193.200.142.254", "cca3": "AUT", "cn": "Austria", "temp": 32, "signal": 27, "battery_level": 5, "c02_level": 1282, "timestamp" :1475600536 }""")).toDF("id", "device").as[DeviceData]


  // to get the id, device_type,ip and cca3
  import org.apache.spark.sql.functions.get_json_object
  import org.apache.spark.sql.functions.col
  val selectedEventColumn = eventsDS.select(col("id"),get_json_object(col("device"),"$.device_type").alias("device_type")
  ,get_json_object(col("device"),"$.ip"),get_json_object(col("device"),"$.cca3").alias("cca3"))
  selectedEventColumn.show()


  //write  the JSON part alone as file
  val deviceDF = eventsDS.select($"device")
  println("Displaying only the JSON part")

  deviceDF.show()


  println("Writing it as JSON file as " + deviceDF.rdd.getNumPartitions)
  println("Writing to single file")
  //deviceDF.coalesce(1).write.format("json").mode("overwrite").save("D:\\Spark_Scala\\data\\JSON\\complexDatabricks.json")

  deviceDF.coalesce(1).write.mode(SaveMode.Overwrite).json("D:\\\\Spark_Scala\\\\data\\\\JSON\\\\complexDatabricks.json")


  println("Reading the Device jsonfile")
  val readDeviceDF = spark.read.json("D:\\\\Spark_Scala\\\\data\\\\JSON\\\\complexDatabricks.json")
  readDeviceDF.show()


  //Extract the  dto get the id, device_type,ip and cca3 using from_json

  import org.apache.spark.sql.types.{StructType,StructField,StringType,IntegerType}
  import org.apache.spark.sql.functions.from_json

  val jsonSchema = new StructType()
    .add("battery_level", LongType)
    .add("c02_level", LongType)
    .add("cca3",StringType)
    .add("cn", StringType)
    .add("device_id", LongType)
    .add("device_type", StringType)
    .add("signal", LongType)
    .add("ip", StringType)
    .add("temp", LongType)
    .add("timestamp", TimestampType)


  val  fromJsonReadDeviceDF =   readDeviceDF.select(from_json(col("device"),jsonSchema).alias("deviceJson"))
  fromJsonReadDeviceDF.show()
  fromJsonReadDeviceDF.select($"deviceJson.device_type",$"deviceJson.ip",$"deviceJson.cca3").show()


    //Processing nested Structure  to be continued
    val dataDS = Seq("""
{
"dc_id": "dc-101",
"source": {
    "sensor-igauge": {
      "id": 10,
      "ip": "68.28.91.22",
      "description": "Sensor attached to the container ceilings",
      "temp":35,
      "c02_level": 1475,
      "geo": {"lat":38.00, "long":97.00}
    },
    "sensor-ipad": {
      "id": 13,
      "ip": "67.185.72.1",
      "description": "Sensor ipad attached to carbon cylinders",
      "temp": 34,
      "c02_level": 1370,
      "geo": {"lat":47.41, "long":-122.00}
    },
    "sensor-inest": {
      "id": 8,
      "ip": "208.109.163.218",
      "description": "Sensor attached to the factory ceilings",
      "temp": 40,
      "c02_level": 1346,
      "geo": {"lat":33.61, "long":-111.89}
    },
    "sensor-istick": {
      "id": 5,
      "ip": "204.116.105.67",
      "description": "Sensor embedded in exhaust pipes in the ceilings",
      "temp": 40,
      "c02_level": 1574,
      "geo": {"lat":35.93, "long":-85.46}
    }
  }
}""").toDS()

  println("Printing the nested JSON dataset")
  dataDS.printSchema()
  val nestedSchema = new StructType().
    add("dc_id",StringType).
    add("source",MapType(StringType,new StructType().
      add("id",IntegerType).
      add("ip",StringType).
      add("description",StringType).
      add("temp",IntegerType).
      add("co2_level",IntegerType).
      add("geo",MapType(StringType,DoubleType))))

  val nestedJsonDF = spark.read.schema(nestedSchema).json(dataDS)
  println("Nested JSON DF schema")
  nestedJsonDF.printSchema()
  nestedJsonDF.show()

  //OR above schema can be mentioned in a different way , above MapType will be replaced by Struct Type
  val nestedSchema1 = new StructType()
    .add("dc_id", StringType)                               // data center where data was posted to Kafka cluster
    .add("source",                                          // info about the source of alarm
      MapType(                                              // define this as a Map(Key->value)
        StringType,
        new StructType()
          .add("description", StringType)
          .add("ip", StringType)
          .add("id", LongType)
          .add("temp", LongType)
          .add("c02_level", LongType)
          .add("geo",
            new StructType()
              .add("lat", DoubleType)
              .add("long", DoubleType)
          )
      )
    )

  val nestedJsonDF1 = spark.read.schema(nestedSchema1).json(dataDS)
  println("Nested JSON DF schema -- Another representation")
  nestedJsonDF1.printSchema()
  nestedJsonDF1.show()

  //create a dataframe with  a column for devise id, sensor type, description

  import org.apache.spark.sql.functions.explode
  println("nestedJsonDF1 processing ")
  val explodenestedJsonDF1 = nestedJsonDF1.select(col("dc_id"),explode(col("source"))).
    withColumnRenamed("key","deviceType").
    withColumnRenamed("value","deviceDescription")
    explodenestedJsonDF1.show(10)
    explodenestedJsonDF1.printSchema()


  println("nestedJsonDF processing ")
  val explodenestedJsonDF = nestedJsonDF.select(col("dc_id"),explode(col("source"))).
    withColumnRenamed("key","deviceType").
    withColumnRenamed("value","deviceDescription")
  explodenestedJsonDF.show(10)
  explodenestedJsonDF.printSchema()


//Converting the map values to column - using getItem
  explodenestedJsonDF.select(col("deviceDescription.geo").getItem("lat").as("latitude"),
    col("deviceDescription.geo").getItem("long").as("longitude")).show()
  //getting the ip and geo position with device type

  val deviceDatadetailsDF = explodenestedJsonDF.
   select($"dc_id",$"deviceType",col("deviceDescription.ip"),
      col("deviceDescription.geo").getItem("lat").alias("lat"),
     col("deviceDescription.geo").getItem("long").as("long")
    )

  deviceDatadetailsDF.show()






}
