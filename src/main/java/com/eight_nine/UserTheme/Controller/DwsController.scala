package com.eight_nine.UserTheme.Controller

import com.eight_nine.UserTheme.service.DwsService
import com.eight_nine.Utils.{CreateTable, HiveUtils}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object DwsController {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("test")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    val month = args(0).toInt
    val day = args(1).toInt
    CreateTable.create_dws_user(spark)
    HiveUtils.openDynamicPartition(spark)
    DwsService.dwsUser(spark,month,day)
    spark.stop()
  }
}
