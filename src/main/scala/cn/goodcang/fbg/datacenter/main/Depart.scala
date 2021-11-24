package cn.goodcang.fbg.datacenter.main


import cn.goodcang.fbg.datacenter.business.Stage
import cn.goodcang.fbg.datacenter.common.ArgsTools.Args
import cn.goodcang.fbg.datacenter.common._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.slf4j.{Logger, LoggerFactory}


object Depart {

  /**
   *
   * @param args [0] zk
   * @param args [1] sql
   * @param args [2] system_name
   * @param args [3] sink_table
   * @param args [4] partitions
   */
  def main(args: Array[String]): Unit = {

    val logger: Logger = LoggerFactory.getLogger(Depart.getClass)
    Check.checkArgs(args, logger)

    val arguments = ArgsTools.parse(args)
    val query: SqlParser.Query = SqlParser.parse(arguments.sql)
    logger.info("-----------" + query)

    val session = SessionTools.initSession()
    val source: DataFrame = session.sql(query.select_main_clause)

    val sink: DataFrame = Stage.transform(source
      , session
      , arguments
      , query)

    Stage.flush(sink, session, arguments)

    session.close()
  }


}

