import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Put
import java.io._



object HbaseCURDexample extends App {
  println("HBASE CURD example")
  val conf = HBaseConfiguration.create()
  conf.addResource("D:\\sandbox\\hbase-1.4.13-bin\\hbase-1.4.13\\conf\\hbase-site.xml")
  //conf.set("hbase.zookeeper.quorum", "localhost"); //Can be comma seperated if you have more than 1
 // conf.set("hbase.zookeeper.property.clientPort", "2181");
 // conf.set("hbase.root.dir","file:///D:/sandbox/hbase-1.4.13-bin/HdFile")
 // conf.set("zookeeper.znode.parent", "/hbase-unsecure");

  val hbaseConnection = ConnectionFactory.createConnection(conf)
  try{
    val table = hbaseConnection.getTable(TableName.valueOf("page_visits"))
  try {

  val infoCf = Bytes.toBytes("info");
  val  nicknameColQuaflifier = Bytes.toBytes("user_nickname");
  val user1RowKey = Bytes.toBytes("user_id-1");
  val user2RowKey = Bytes.toBytes("user_id-2");
  //Put Example
  var user1 = new Put(user1RowKey)
    .addColumn(infoCf,
      nicknameColQuaflifier,
      Bytes.toBytes("john_1985")
    );
  var user2 = new Put(user2RowKey)
    .addColumn(infoCf, nicknameColQuaflifier,
      Bytes.toBytes("lee")
    );
  println("Put record" + user1)
  table.put(user1)
  //  println("Put record" + user2)
  } catch {
    case e: IOException => e.printStackTrace();

  }finally {
    if (table != null) {

      try {

        table.close()

      } catch {

        case e: IOException =>

          e.printStackTrace();

      }

    }
    if (hbaseConnection != null) {

      try {

        //Close the Hbase connection.

        hbaseConnection.close()

      } catch {

        case e: IOException =>

          e.printStackTrace()

      }

    }
  }
  }

  //println("Closing the connections")
 // pageVisitTable.close()
 // hbaseConnection.close()

 // pageVisitTable.put(user2)




}
