import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

object cdcType2 {

  def cdcType2Main(spark:SparkSession, hist:DataFrame, snap:DataFrame,
    start_dt:String,end_dt:String,end_value:String,unique_columns:String,end_dt_logic:String):DataFrame={

    validation(hist,snap,start_dt,end_dt,unique_columns)

    val end_value_ts = lit(end_value).cast(TimestampType)


    val (active_records,closed_records) = splitHistActiveClosed(hist,start_dt,end_dt,end_value_ts)

    println("ACTIVE Records ")
    active_records.show(false)
    println(active_records.schema)
    println("CLOSED RECORD ")
    closed_records.show(false)

    //Just a extra check to make sure snapshot has only active records
    val active_snap = snap.filter(snap(end_dt) <=> end_value_ts)
    val active_with_cdc = cdcLogicImpl(spark,active_records,active_snap,unique_columns,
      start_dt,end_dt,end_value,end_dt_logic)

    println("ACTIVE WITH CDC ")
    active_with_cdc.show(false)

    val final_result = active_with_cdc.union(closed_records)
    final_result

  }


  def validation(hist:DataFrame,snap:DataFrame,
                 start_dt:String,end_dt:String,
                 unique_columns:String) ={
    val hist_schema = hist.schema
    val snap_schema = snap.schema
    val diff_schema = hist_schema.toSet.union(snap_schema.toSet).diff(hist_schema.toSet.intersect(snap_schema.toSet))

    println("History "+ hist_schema)
    println("Snapshot" + snap_schema)
    println("unique Columns" + unique_columns)
    if(!diff_schema.isEmpty){
      throw new Exception(
        s"""
           |Schema between History and SnapShot Did Not Match
           |History : ${hist_schema}
           |Snapshot : ${snap_schema}
           |Difference : ${diff_schema}
         """.stripMargin)
    }
    //We will let Spark throw the error java.lang.IllegalArgumentException if element NOt found
    val dummy_val = hist_schema(Set(start_dt,end_dt) ++ unique_columns.split(",").toSet)
    //val dummy_val = hist_schema(Set(start_dt))

  }

  def splitHistActiveClosed( hist:DataFrame,
                             start_dt:String,end_dt:String,end_value_ts:Column):(DataFrame,DataFrame)={

    val active_records = hist.filter(hist(end_dt) <=>  end_value_ts)
    val closed_records = hist.filter(!(hist(end_dt) <=> end_value_ts))
    (active_records,closed_records)
  }

  def cdcLogicImpl(spark:SparkSession,active_records:DataFrame,snap:DataFrame,
                   unique_columns:String,start_dt:String,end_dt:String,
                   end_value:String,end_dt_logic:String):DataFrame={
    import spark.implicits._

    /*Need to think about this,do we fail when we have two active records with same start_dt,
     currently it just just drops one of them
     */
    val union_data = active_records.union(snap)
      .dropDuplicates(start_dt :: end_dt :: unique_columns.split(",").map(x=>x.trim).toList)

    val uniq = unique_columns.split(",").map(x=> col(x))

    val wndw = Window.partitionBy(uniq:_*).orderBy(col(start_dt).asc)

    //Code will fail if actual end_dt column is also called as "new_end_date".Need to look at handling end_value null
    val cdc_data = union_data.select($"*",
      (lead(col(start_dt) - expr(s"INTERVAL ${end_dt_logic}"),1,end_value).over(wndw) ).as("new_end_date"))

    cdc_data.drop(end_dt).withColumnRenamed("new_end_date",end_dt)

  }

}


