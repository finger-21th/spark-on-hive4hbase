package cn.goodcang.fbg.datamiddleground.common

object ArgsTools {
  def parse(args: Array[String]): (String, String, String, String,String) = {
    val zk = args(0) //zk
    val sql = args(1) //sql
    val system_name = args(2) //
    val sink_table = args(3) //
    val partitions = args(4) //
    (zk, sql, system_name, sink_table,partitions)
  }

}
