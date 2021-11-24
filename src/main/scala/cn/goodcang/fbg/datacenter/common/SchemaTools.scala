package cn.goodcang.fbg.datacenter.common

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object SchemaTools {


  def alter(frame: DataFrame, fields: Array[String]): StructType = {

    var schema: StructType = frame.schema

    fields.foreach(field => {
      schema = schema.add(StructField(field, StringType, nullable = true))
    })
    schema
  }
}
