package cn.goodcang.fbg.datacenter.common

import org.apache.hadoop.hbase.Cell
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.StructType

import java.util
import scala.collection.mutable

object RowTools {

  import scala.collection.JavaConversions._

  def alter(fields: Array[String], result: util.List[Cell], row: Row, schema: StructType): Row = {

    val buffer: mutable.Buffer[Object] = Row.unapplySeq(row).get.map(_.asInstanceOf[Object]).toBuffer

    fields.foreach(field => {
      var flag = 0
      if (result != null) {
        result.foreach(cell => {
          val field_name = Bytes.toString(cell.getQualifierArray, cell.getQualifierOffset, cell.getQualifierLength)
          if (field.equals(field_name)) {

            buffer.add(Bytes.toString(cell.getValueArray, cell.getValueOffset, cell.getValueLength))
            flag = 1
          }
        })
      }
      if (flag == 0)
        buffer.add(null)
    })

    new GenericRowWithSchema(buffer.toArray, schema)
  }


  def alter(fields: Array[String], result: Result, row: Row, schema: StructType): Row = {

    val buffer: mutable.Buffer[Object] = Row.unapplySeq(row).get.map(_.asInstanceOf[Object]).toBuffer

    fields.foreach(field => {
      var flag = 0
      if (result != null && !result.isEmpty) {
        val cells = result.listCells()
        cells.foreach(cell => {
          val field_name = Bytes.toString(cell.getQualifierArray, cell.getQualifierOffset, cell.getQualifierLength)
          if (field.equals(field_name)) {
            buffer.add(Bytes.toString(cell.getValueArray, cell.getValueOffset, cell.getValueLength))
            flag = 1
          }
        })
      }
      if (flag == 0)
        buffer.add(null)
    })

    new GenericRowWithSchema(buffer.toArray, schema)
  }
}
