package cn.goodcang.fbg.datacenter.common

object ArgsTools {


  def parse(args: Array[String]): Args = {
    val zk = args(0) //zk
    val sql = args(1) //sql
    val system_name = args(2) //
    val sink_table = args(3) //
    val partitions = args(4) //
    val batch = args(5) //
    Args(zk, sql, system_name, sink_table, partitions, batch)
  }

  case class Args(zk: String
                  , sql: String
                  , system_name: String
                  , sink_table: String
                  , partitions: String
                  , batch: String
                 ) extends Serializable

}
