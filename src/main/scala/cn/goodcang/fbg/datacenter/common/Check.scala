package cn.goodcang.fbg.datacenter.common

import org.slf4j.Logger

object Check {


  def checkArgs(args: Array[String], logger: Logger): Unit = {
    if (args.length != 5) {
      logger.error(String.format("paramater is not allow : %s", args))
      (0 to args.length - 1).foreach(arg_index => {
        logger.error(String.format("current arg index is %s ---and value is : %s", arg_index.toString, args(arg_index)))
      })
      return
    }
  }
}
