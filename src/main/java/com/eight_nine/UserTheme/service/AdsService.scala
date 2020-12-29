package com.eight_nine.UserTheme.service

import org.apache.spark.sql.{SaveMode, SparkSession}

object AdsService {
    def getARPU(spark:SparkSession,year:Int,month:Int,day:Int): Unit ={
      spark.sql(
        s"""
          |select
          |'${year}-${month}-${day}' dt,
          |round(sum(iap_last_30d_amount)/count(*),2) ARPU
          |from
          |(
          |select
          |*
          |from
          |dwt.dwt_user_theme
          |where dt='${year}-${month}-${day}'
          |) tmp_dwt
          |""".stripMargin).coalesce(1).write.format("csv").mode(SaveMode.Append).save("s3://my89dw1/ads/ads_user_theme/ARPU/"+s"${year}-${month}-${day}")
    }
}
