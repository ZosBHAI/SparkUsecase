import org.apache.spark.sql.SparkSession

object flatMapMapExample extends App{
  /*
  * File user_artist_data_small - user artist play count
  * smallest_artist_data - artistId , artistname
  * artist_alias_small - artistId ,canonical Id
  * o/p:  user canonical Id count
   */
  val spark = SparkSession.builder().appName("FlatMapExampleBradCast").master("local[*]").getOrCreate()
  spark.sparkContext.setLogLevel("Error")
  import spark.implicits._
  //Reading the file
 val rawUserArtistData = spark.read.textFile("D:\\Spark_Scala\\data\\AdvancedAnalytics\\audio\\data_raw\\user_artist_data_small.txt")
  rawUserArtistData.show(10)
 import spark.implicits._
  // Extract the first 2 column of the rawUserArtistData and convert it to integer
  val UserArtistDataDF =   rawUserArtistData.map(x => {
    val Array(userId, artistID, count) = x.split(" ")
    (userId.toInt, artistID.toInt)
  }).toDF("userId","artistID")
  println("Showing the Dataframe Content")
  UserArtistDataDF.show(10)
  UserArtistDataDF.describe().show()

  val rawArtistData= spark.read.textFile("D:\\Spark_Scala\\data\\AdvancedAnalytics\\audio\\data_raw\\profiledata_06-May-2005\\smallest_artist_data.txt")
  val artistDF = rawArtistData.flatMap({line =>
    val (id, name) = line.span(_ != '\t')
    if (name.isEmpty){None}
    else {
      try {
        Some(id.toInt,name.trim)
      }
      catch{
        case _: NumberFormatException => None
      }
    }
  }
  ).toDF("id","name")
  artistDF.show(10)

  val rawArtistAlias = spark.read.textFile("D:\\Spark_Scala\\data\\AdvancedAnalytics\\audio\\data_raw\\artist_alias_small.txt")
  val artistAliasMap = rawArtistAlias.flatMap(x => {
    val Array(artist,alias) = x.split('\t')
    if (artist.isEmpty){None}
    else{Some(artist.toInt,alias.toInt)}
  }).collect().toMap
 artistAliasMap.take(10).foreach(println)

//BroadCast to replace the artistId with Canonical ID
val bcastartistAlias = spark.sparkContext.broadcast(artistAliasMap)
val finalDF = rawUserArtistData.map({x =>
  val Array(userId, artistID, count) = x.split(" ").map(_.toInt)
  val finalArtistID = bcastartistAlias.value.getOrElse(artistID,artistID)
  (userId,finalArtistID,count)
}).toDF("userid","artistID","playCount")
  finalDF.show(10)

// to do covert the above to a function

}
