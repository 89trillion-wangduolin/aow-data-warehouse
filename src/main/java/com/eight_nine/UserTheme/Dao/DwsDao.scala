package com.eight_nine.UserTheme.Dao

import org.apache.spark.sql.SparkSession

object DwsDao {
  def getLogin(spark:SparkSession): Unit ={
    spark.sql(
      """
        |
        |""".stripMargin)
  }
}
