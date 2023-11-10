
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import cdcType2._
object cdcMain {
  def main(args:Array[String])={

    val spark = SparkSession.builder().master("local[*]").appName("CDC").getOrCreate()

    /*
    Input Data and SnapShot data expected to have same schema and order
     */
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


    val hist = spark.read.option("delimiter","|").option("header",true).format(format).load(hist_path)
    val snap = spark.read.option("delimiter","|").option("header",true).format(format).load(snap_path)


    println("INPUT DF1 :" )
    hist.show()

    println("INPUT DF2 :")
    snap.show()

    val final_res = cdcType2Main(spark,hist,snap,start_dt,end_dt,end_value,unique_columns,end_dt_logic)
    println("Final Dataframe :")
    final_res.show()

    //final_res.write.mode("overwrite").format("parquet").save(opt_path)

  }
}
