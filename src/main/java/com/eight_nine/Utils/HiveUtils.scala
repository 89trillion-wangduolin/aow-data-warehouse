package com.eight_nine.Utils

import org.apache.spark.sql.SparkSession

object HiveUtils {
  //开启动态分区和非严格模式
  def openDynamicPartition(spark: SparkSession) = {
    spark.sql("set hive.exec.dynamic.partition=true")
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
  }
}
