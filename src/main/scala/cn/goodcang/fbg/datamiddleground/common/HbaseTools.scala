package cn.goodcang.fbg.datamiddleground.common

import org.apache.hadoop.hbase.{Cell, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Get, Result}
import org.apache.hadoop.hbase.util.Bytes

import java.util


class HbaseTools(config: HBaseConfig) extends Serializable {


  lazy val connection = HbaseTools.apply(config)

  import scala.collection.JavaConversions._

  def get(tableName: String, fields: Array[String], rowkey: String): util.List[Cell] = {

    var result: util.List[Cell] = null
    val table = connection.getTable(TableName.valueOf(tableName))
    if (rowkey != null) {

      val get = new Get(Bytes.toBytes(rowkey))

      fields.map(field => {
        get.addColumn(Bytes.toBytes(Constants.COLUMN_FAMILY), Bytes.toBytes(field))
      })

      result = table.get(get).listCells()
    }
    result
  }

  def close(): Unit = {
    connection.close()
  }
}


object HbaseTools extends Serializable {

  def apply(config: HBaseConfig): Connection = {
    val connection: Connection = ConnectionFactory.createConnection(config.get)
    connection
  }

}
