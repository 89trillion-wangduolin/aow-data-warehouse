package com.eight_nine.UserTheme.service

import org.apache.spark.sql.{SaveMode, SparkSession}

object DwtService {
  def dwtUser(spark:SparkSession,year:Int,month:Int,day:Int): Unit ={
    spark.sql(
      s"""
         |select
         |if(new.gaid is null,old.gaid,new.gaid) gaid,
         |if(new.user_id is null,old.user_id,new.user_id) user_id,
         |if(new.country is null,old.country,new.country) country,
         |if(new.refer is null,old.refer,new.refer) refer,
         |if(new.cvc is null,old.cvc,new.cvc) cvc,
         |if(new.appid is null,old.appid,new.appid) appid,
         |if(old.login_date_first is null and new.login_count>0,'${year}-${month}-${day}',old.login_date_first) login_date_first,
         |if(new.login_count>0,'${year}-${month}-${day}',old.login_date_last) login_date_last,
         |if(old.login_count is null,0,old.login_count)+if(new.login_count>0,1,0) login_count,
         |if(new.login_last_30d_count is null,0,new.login_last_30d_count) login_last30_count,
         |if(old.pvpcon is null,0,old.pvpcon)+if(new.pvpcon>0,1,0) pvpcon,
         |if(old.pvpwcon is null,0,old.pvpwcon)+if(new.pvpwcon>0,1,0) pvpwcon,
         |if(new.pvpcon_last_30d_count is null,0,new.pvpcon_last_30d_count) pvp_last_30d_con,
         |if(new.pvpwcon_last_30d_count is null,0,new.pvpwcon_last_30d_count) pvp_last_30d_wcon,
         |if(old.iap_first_date is null and new.iap_count>0,'${year}-${month}-${day}',old.iap_first_date) iap_first_date,
         |if(new.iap_count>0,'${year}-${month}-${day}',old.iap_last_date) iap_last_date,
         |if(old.iap_count is null,0,old.iap_count)+if(new.iap_count>0,1,0) iap_count,
         |if(old.iap_amount is null,0,old.iap_amount)+if(new.iap_amount>0,new.iap_amount,0) iap_amount,
         |if(new.iap_last_30d_count is null,0,new.iap_last_30d_count) iap_last_30d_count,
         |if(new.iap_last_30d_amount is null,0,new.iap_last_30d_amount) iap_last_30d_amount,
         |'${year}-${month}-${day}' dt
         |from
         |(select
         |*
         |from
         |dwt.dwt_user_theme
         |where dt=date_add('${year}-${month}-${day}',-1))old
         |full join
         |(
         |select
         |gaid,
         |user_id,
         |country,
         |refer,
         |cvc,
         |appid,
         |sum(if(dt='${year}-${month}-${day}',login_count,0)) login_count,
         |sum(if(dt='${year}-${month}-${day}',pvpcon,0)) pvpcon,
         |sum(if(dt='${year}-${month}-${day}',pvpwcon,0)) pvpwcon,
         |sum(if(login_count>0,1,0)) login_last_30d_count,
         |sum(if(pvpcon>0,1,0)) pvpcon_last_30d_count,
         |sum(if(pvpwcon>0,1,0)) pvpwcon_last_30d_count,
         |sum(if(dt='${year}-${month}-${day}',iap_count,0)) iap_count,
         |sum(if(dt='${year}-${month}-${day}',iap_amount,0)) iap_amount,
         |sum(if(iap_count>0,1,0)) iap_last_30d_count,
         |sum(iap_amount) iap_last_30d_amount
         |from dws.dws_user_theme
         |where dt>=date_add('${year}-${month}-${day}',-30)
         |group by gaid,user_id,country,refer,cvc,appid
         |) new
         |on old.gaid=new.gaid
         |""".stripMargin).coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dwt.dwt_user_theme")
  }
}
