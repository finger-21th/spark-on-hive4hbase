package cn.goodcang.fbg.datacenter.business

import cn.goodcang.fbg.datacenter.common.ArgsTools.Args
import cn.goodcang.fbg.datacenter.common.{Constants, HBaseConfig, HbaseTools, RowTools, SchemaTools, SqlParser, StreamFunction}
import org.apache.hadoop.hbase.client.Result
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.StructType

import java.util
import scala.collection.immutable
import scala.collection.JavaConversions._

object Stage {


  def transform(source: DataFrame
                , session: SparkSession
                , arguments: Args
                , query: SqlParser.Query): DataFrame = {
    val schema: StructType = SchemaTools.alter(source, query.from_table_fields_alias)

    pipeLine(source
      , session
      , arguments
      , query
      , schema)
  }


  def pipeLine(frame: DataFrame
               , session: SparkSession
               , arguments: Args
               , query: SqlParser.Query
               , schema: StructType
              ): DataFrame = {

    val source: RDD[Row] = frame
      .repartition(Integer.valueOf(arguments.partitions), new Column(query.main_tab_foreign_key))
      .rdd
      .mapPartitions(Stage.dataFrameFromHbase(_, query, arguments, schema))

    val channel: DataFrame = session.createDataFrame(source, schema).drop(query.main_tab_foreign_key)

    val sink: DataFrame = if (query.filter != null) channel.filter(query.filter) else channel

    sink
  }


  def flush(sink: DataFrame
            , session: SparkSession
            , arguments: Args): Unit = {

    sink.registerTempTable(Constants.REGISTER_TEMPTABLE)
    session.sql(String.format("INSERT INTO %s SELECT * FROM %s", arguments.sink_table, Constants.REGISTER_TEMPTABLE))
  }

  def dataFrameFromHbase(partition: Iterator[Row]
                         , query: SqlParser.Query
                         , arguments: Args
                         , schema: StructType
                        ): Iterator[Row] = {
    val hbaseClient: HbaseTools = new HbaseTools(HBaseConfig.apply(arguments.zk))

    val rows: immutable.Seq[Row] = rddFillUpFromHbase(partition
      , query
      , hbaseClient
      , arguments
      , schema)

    hbaseClient.close()
    rows.iterator
  }

  def rddFillUpFromHbase(partition: Iterator[Row]
                         , query: SqlParser.Query
                         , hbaseClient: HbaseTools
                         , arguments: Args
                         , schema: StructType): immutable.Seq[Row] = {
    val part: Seq[Row] = partition.toList

    val rowKeyList: List[String] = rdd2RowKeyList(part, query)

    val results: util.ArrayList[Result] = rowKeyList2HResults(hbaseClient, arguments, query, rowKeyList)

    hResults2Rdd(results, query.from_table_fields, part, schema)
  }


  def rdd2RowKeyList(part: Seq[Row], query: SqlParser.Query): List[String] = {
    part.map(StreamFunction.dataRows2RowKeys(_, query)).toList
  }


  def rowKeyList2HResults(hbaseClient: HbaseTools
                          , arguments: Args
                          , query: SqlParser.Query
                          , rowKeyList: List[String]): util.ArrayList[Result] = {
    hbaseClient
      .batchGet(String.format("%s_%s", arguments.system_name, query.from_tab)
        , query.from_table_fields
        , rowKeyList
        , arguments.batch.toInt)
  }


  def hResults2Rdd(results: util.ArrayList[Result]
                   , fields: Array[String]
                   , part: Seq[Row]
                   , schema: StructType): immutable.Seq[Row] = {
    (0 to results.length - 1).map(index => {
      RowTools.alter(fields, results(index), part(index), schema)
    })
  }

}
