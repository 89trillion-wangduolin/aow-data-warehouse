package com.eight_nine.UserTheme.Controller

import com.eight_nine.UserTheme.service.{DwsService, DwtService}
import com.eight_nine.Utils.{CreateTable, HiveUtils}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object DwtController {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("dwtController")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    val sourceMonth = args(0)
    val day = args(1).toInt
    val year=sourceMonth.substring(0,4).toInt
    val month=sourceMonth.substring(4).toInt
    CreateTable.create_dwt_user(spark)
    HiveUtils.openDynamicPartition(spark)
    DwtService.dwtUser(spark,year,month,day)
    spark.stop()
  }
}
