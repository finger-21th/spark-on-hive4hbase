package cn.goodcang.fbg.datacenter.common

import org.apache.hadoop.hbase.{Cell, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Get, Result, Table}
import org.apache.hadoop.hbase.util.Bytes

import java.util
import java.util.function.Consumer
import scala.collection.JavaConverters._

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


  def batchGet(tableName: String, fields: Array[String], rowkeys: List[String], batch: Int): util.ArrayList[Result] = {

    val table: Table = connection.getTable(TableName.valueOf(tableName))
    val gets: util.ArrayList[Get] = new util.ArrayList[Get]()

    val res: util.ArrayList[Result] = new util.ArrayList[Result]()

    rowkeys.forEach(new Consumer[String] {
      override def accept(rowkey: String): Unit = {

        if (rowkey != null) {

          val get = new Get(Bytes.toBytes(rowkey))

          fields.map(field => {
            get.addColumn(Bytes.toBytes(Constants.COLUMN_FAMILY), Bytes.toBytes(field))
          })

          get.setCacheBlocks(false)
          gets.add(get)

          if (gets.size() == batch) {
            res.addAll(flushGet(table, gets))
          }
        } else {
          if (gets.size() > 0)
            res.addAll(flushGet(table, gets))

          res.add(null)
        }
      }
    })

    if (gets.size() > 0)
      res.addAll(flushGet(table, gets))

    res
  }

  def flushGet(table: Table, gets: util.ArrayList[Get]): util.List[Result] = {

    val results: Array[Result] = table.get(gets)
    gets.clear()
    results.toList.asJava
  }
}


object HbaseTools extends Serializable {

  def apply(config: HBaseConfig): Connection = {
    val connection: Connection = ConnectionFactory.createConnection(config.get)
    connection
  }

}
