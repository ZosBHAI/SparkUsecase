import org.apache.spark.sql.SparkSession
object testDataCDC {
  def main(args:Array[String])={

    val spark = SparkSession.builder().master("local[*]").appName("testandcreateCDCdataset").getOrCreate()
    val testActualData = spark.read.option("delimiter","|").format("csv").load("C:\\SparkDataset\\CDC\\test_actualdata.txt")
    testActualData.show(truncate=false)

    val testSnapshotData = spark.read.option("delimiter","|").format("csv").
      load("C:\\SparkDataset\\CDC\\test_snapshotdata.txt")
    testSnapshotData.show(truncate=false)
    import com.typesafe.config.ConfigFactory
    val conf = ConfigFactory.load("cdc-application.conf")
    val start_dt = conf.getString("cdc-type2.start_dt")
    val end_dt = conf.getString("cdc-type2.end_dt")
    val end_value = conf.getString("cdc-type2.end_value")
    val unique_columns = conf.getString("cdc-type2.unique_columns")
    val end_dt_logic = conf.getString("cdc-type2.end_dt_logic")
    val format = conf.getString("cdc-type2.format")
    val hist_path = conf.getString("cdc-type2.history_path")
    val snap_path = conf.getString("cdc-type2.snapshot_path")
    val opt_path = conf.getString("cdc-type2.output_path")

    println(opt_path)


  }

}
