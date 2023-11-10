import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
object jsonComplexProcess extends App {
  val spark = SparkSession.builder().appName("JSONConstrcuting").master("local[*]").getOrCreate()
  import spark.implicits._
  println("Converting the JSON to dataset")
  val inputJSON = Seq("""{"name":"adarsh","gender":"male","age":20,"address":
{"area":"richmond",
"city":"bangalore"
}
} """).toDS()

  val jsonSchemaex1 = new StructType().add("name",StringType)
    .add("gender",StringType)
    .add("age", IntegerType)
    .add("address", new StructType()
    .add("area",StringType)
    .add("city",StringType))

  val flatenJsonSchemax1 = new StructType().add("name",StringType)
    .add("gender",StringType)
    .add("age", IntegerType)
    .add("address.area",StringType)
    .add("address.city",StringType)


  inputJSON.show()
  val df = spark.read.schema(jsonSchemaex1).json(inputJSON)
  df.show()
  df.printSchema()
//create a new dataframe  column  from the exisiting JSON
 val addflattenedDF = df.select($"name",$"gender",$"age",$"address.area",$"address.city")
  addflattenedDF.show()
  //writing  dataframe as JSON
  addflattenedDF.write.json("D:\\Spark_Scala\\data\\JSON\\address.json")


  //Dealing with Nested Struture and flattening it
  val schema1 = new StructType()
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
  val dataDF = spark.read.schema(schema1).json(dataDS)
  /*
root
 |-- dc_id: string (nullable = true)
 |-- source: map (nullable = true)
 |    |-- key: string
 |    |-- value: struct (valueContainsNull = true)
 |    |    |-- description: string (nullable = true)
 |    |    |-- ip: string (nullable = true)
 |    |    |-- id: long (nullable = true)
 |    |    |-- temp: long (nullable = true)
 |    |    |-- c02_level: long (nullable = true)
 |    |    |-- geo: struct (nullable = true)
 |    |    |    |-- lat: double (nullable = true)
 |    |    |    |-- long: double (nullable = true)


root
 |-- dc_id: string (nullable = true)
 |-- key: string (nullable = false)
 |-- value: struct (nullable = true)
 |    |-- description: string (nullable = true)
 |    |-- ip: string (nullable = true)
 |    |-- id: long (nullable = true)
 |    |-- temp: long (nullable = true)
 |    |-- c02_level: long (nullable = true)
 |    |-- geo: struct (nullable = true)
 |    |    |-- lat: double (nullable = true)
 |    |    |-- long: double (nullable = true)


 TO

 root
 |-- dcId: string (nullable = true)
 |-- deviceType: string (nullable = false)
 |-- ip: string (nullable = true)
 |-- deviceId: long (nullable = true)
 |-- c02_level: long (nullable = true)
 |-- temp: long (nullable = true)
 |-- lat: double (nullable = true)
 |-- lon: double (nullable = true)

   */

  val explodeDF = dataDF.select($"dc_id",explode($"source"))
  explodeDF.printSchema()
  val flatenDataDF = explodeDF.select($"dc_id",expr("key as Source"),$"value.ip",$"value.geo",$"value.geo.lat")
  flatenDataDF.show()


}
