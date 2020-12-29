package com.eight_nine.Utils

import org.apache.spark.sql.SparkSession

object CreateTable {
  def create_dws_user(spark:SparkSession): Unit ={
    spark.sql(
      """
        |create external table if not exists dws.dws_user_theme(
        |gaid string,
        |user_id string,
        |country string,
        |refer string,
        |cvc int,
        |appid string,
        |login_count int,
        |pvpcon int,
        |pvpwcon int,
        |iap_count int,
        |iap_amount double)
        |partitioned by(dt string)
        |row format delimited fields terminated by '\t'
        |location 's3://my89dw1/dws/dws_user_theme/'
        |""".stripMargin)
  }
  def create_dwt_user(spark:SparkSession): Unit ={
    spark.sql(
      """
        |create external table if not exists dwt.dwt_user_theme(
        |gaid string,
        |user_id string,
        |country string,
        |refer string,
        |cvc int,
        |appid string,
        |login_date_first string,
        |login_date_last string,
        |login_count int,
        |login_last_30d_count int,
        |pvpcon int,
        |pvpwcon int,
        |pvp_last_30d_con int,
        |pvp_last_30d_wcon int,
        |iap_first_date string,
        |iap_last_date string,
        |iap_count int,
        |iap_amount double,
        |iap_last_30d_count int,
        |iap_last_30d_amount double)
        |partitioned by(dt string)
        |row format delimited fields terminated by '\t'
        |location 's3://my89dw1/dwt/dwt_user_theme/'
        |""".stripMargin)
  }
}
