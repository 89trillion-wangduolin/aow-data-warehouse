package com.eight_nine.UserTheme.service

import org.apache.spark.sql.{SaveMode, SparkSession}

object DwsService {
  def dwsUser(spark:SparkSession,month:Int,day:Int): Unit ={
    spark.sql(
      s"""
         |select
         |tmp_user_login.gaid,
         |nvl(uid,'unknown') user_id,
         |tmp_user.country,
         |tmp_user.refer,
         |tmp_user_login.cvc,
         |appid,
         |nvl(login_count,0) login_count,
         |nvl(pvpcon,0) pvpcon,
         |nvl(pvpwcon,0) pvpwcon,
         |nvl(iap_count,0) iap_count,
         |nvl(iap_amount,0) iap_amount,
         |cast(from_unixtime(unix_timestamp('${month}${day}', 'yyyyMMdd'), 'yyyy-MM-dd') as string) dt
         |from
         |(
         |select
         |gaid,
         |uid,
         |appid,
         |cvc,
         |count(*) login_count
         |from dwd.loginuser_logs
         |where month='${month}' and day='${day}'
         |group by gaid,cvc,uid,appid
         |) tmp_user_login
         |join
         |(select gaid,country,refer from dwd.newuser) tmp_user
         |on tmp_user.gaid = tmp_user_login.gaid
         |left join
         |(
         |select gaid,
         |count(*) pvpcon,
         |sum(if(user_add>0,1,0)) pvpwcon
         |from dwd.pvp_results
         |where month='${month}' and day='${day}'
         |group by gaid) tmp_pvp
         |on tmp_user.gaid = tmp_pvp.gaid
         |left join
         |(
         |select
         |gaid,
         |count(*) iap_count,
         |sum(price) iap_amount
         |from dwd.iap_history
         |where month='${month}' and day='${day}'
         |group by gaid) tmp_iap
         |on tmp_iap.gaid = tmp_user.gaid
         |""".stripMargin).coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dws.dws_user_theme")
  }
}
