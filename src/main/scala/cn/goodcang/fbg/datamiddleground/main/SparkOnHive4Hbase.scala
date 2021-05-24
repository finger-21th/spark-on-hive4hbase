package cn.goodcang.fbg.datamiddleground.main

import cn.goodcang.fbg.datamiddleground.common.{ArgsTools, Constants, DefindParser, HBaseConfig, HbaseTools, RowTools, SchemaTools}
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.hbase.Cell

import scala.collection.JavaConversions._
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.types.StructType
import org.slf4j.{Logger, LoggerFactory}

import java.io.File
import java.util


object SparkOnHive4Hbase {

  /**
   *
   * @param args [0] zk
   * @param args [1] sql
   * @param args [2] system_name
   * @param args [3] sink_table
   * @param args [4] partitions
   */
  def main(args: Array[String]): Unit = {

    val logger: Logger = LoggerFactory.getLogger(SparkOnHive4Hbase.getClass)
    if (args.length != 5) {

      logger.error(String.format("paramater is not allow : %s", args))
      (0 to args.length - 1).foreach(arg_index => {
        logger.error(String.format("current arg index is %s ---and value is : %s", arg_index.toString, args(arg_index)))
      })
      return
    }

    val (
      zk
      , sql
      , system_name
      , sink_table
      , partitions
      ) = ArgsTools.parse(args)

    val query: DefindParser.Query = DefindParser.parse(sql)

    logger.warn("-----------" + query)

    val session =
      if (System.getProperties.getProperty("os.name").toUpperCase.indexOf("WINDOWS") != -1) {
        val warehouseLocation = new File("spark-warehouse").getAbsolutePath
        SparkSession
          .builder()
          .appName("Spark Hive Example")
          .master("local[1]")
          .config("spark.sql.warehouse.dir", warehouseLocation)
          .enableHiveSupport()
          .getOrCreate()
      } else {
        SparkSession
          .builder()
          .enableHiveSupport()
          .getOrCreate()
      }

    val frame: DataFrame = session.sql(query.select_main_clause)

    val fields_alias: Array[String] = query.from_table_fields_alias.filter(!_.equals(Constants.EMPTY_STRING)).toArray
    val fields: Array[String] = query.from_table_fields.filter(!_.equals(Constants.EMPTY_STRING)).toArray
    val schema: StructType = SchemaTools.alter(frame, fields_alias)

    val view = frame
      .repartition(Integer.valueOf(partitions), new Column(query.main_tab_foreign_key))
      .rdd
      .mapPartitions(partition => {
        val config: HBaseConfig = HBaseConfig.apply((Constants.ZOOKEEPER_QUORUM, zk))

        val hbaseClient = new HbaseTools(config)

        val rows = partition.map(row => {
          //通过rowkey获取hbase事实数据

          val result: util.List[Cell] = {
            if (StringUtils.isEmpty(row.getAs(query.main_tab_foreign_key))) {
              null
            } else {
              hbaseClient
                .get(String.format("%s_%s", system_name, query.from_tab), fields, row.getAs(query.main_tab_foreign_key))
            }
          }
          //hive主表数据join hbase数据
          RowTools.alter(fields, result, row, schema)
        }).toList

        hbaseClient.close()
        rows.iterator
      })


    val mid = session.createDataFrame(view, schema)
      .drop(query.main_tab_foreign_key)

    val sink = if (query.filter != null) mid.filter(query.filter) else mid

    sink.registerTempTable(Constants.REGISTER_TEMPTABLE)

    session.sql(String.format("INSERT INTO %s SELECT * FROM %s", sink_table, Constants.REGISTER_TEMPTABLE))

    session.close()
  }


}

