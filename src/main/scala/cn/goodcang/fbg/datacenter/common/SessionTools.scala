package cn.goodcang.fbg.datacenter.common

import org.apache.spark.sql.SparkSession

import java.io.File

object SessionTools {

  def initSession(): SparkSession = {
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
  }

}
