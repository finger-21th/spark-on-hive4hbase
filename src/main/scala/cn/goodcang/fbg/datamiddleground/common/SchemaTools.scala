package cn.goodcang.fbg.datamiddleground.common

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object SchemaTools {


  def alter(frame: DataFrame, fields: Array[String]): StructType  = {

    var schema: StructType = frame.schema

    (0 to fields.size - 1).foreach(i => {
      schema = schema.add(StructField(fields(i), StringType, nullable = true))
    })
    schema
  }
}
