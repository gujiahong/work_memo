#!/bin/sh
# author turner koo 2015/05/28

echo "************************** running $0 START in $(date +%Y/%m/%d/%H:%M:%S) **************************"

hiveJarPath="/home/hadoop/work/recommendos/AppPvDataUDF.jar"
dataBaseName="mama100_rems_temp3"
sqoopPath="/home/hadoop/sqoop-1.4.4.bin__hadoop-2.0.4-alpha/bin"
sqoopHost="data12.module.prd"

dt=$1
 
if [ $# -lt 1 ]; then
     dt=30
fi

dateStart=`date --date="$dt days ago" +%Y%m%d`
dateStartSql=`date --date="$dt days ago" +%Y-%m-%d`
dateEnd=`date --date="1 days ago" +%Y%m%d`
dateEndSql=`date --date="1 days ago" +%Y-%m-%d`
echo "start_date:"$dateStartSql----"end_date:"$dateEndSql
tableName_1="recommend_base_"$dateStart"_"$dateEnd
tableName_2="recommend_pv_"$dateStart"_"$dateEnd
tableName_3="recommend_order_count_"$dateStart"_"$dateEnd
tableName_4="recommend_user_mark_"$dateStart"_"$dateEnd
tableName_5="recommend_analyis_result_"$dateStart"_"$dateEnd

createDB4Recom(){
  hive -e "create database if not exists $dataBaseName comment 'hive $dataBaseName database' with dbproperties('creator'='turner koo','date'='`date +%Y/%m/%d_%H:%M:%S`')"
}

getRecommendBaseTable(){
  sql_1="drop table if exists $dataBaseName.$tableName_1;add jar $hiveJarPath;create temporary function getSkuIdByVars as 'hive.udf.AppPvDataUDF';CREATE TABLE $dataBaseName.$tableName_1 AS select t1.imei,t1.uid,t1.skuid from (
select *,getSkuIdByVars(vars) as skuid from app_pvdata where vars != '' and code = '0203' and uid != ''
) t1 group by t1.imei,t1.uid,t1.skuid;"
  echo $sql_1
  hive -e "$sql_1"
}

getRecommendPvTable(){
  sql_2="drop table if exists $dataBaseName.$tableName_2;add jar $hiveJarPath;create temporary function getSkuIdByVars as 'hive.udf.AppPvDataUDF';CREATE TABLE $dataBaseName.$tableName_2 AS select t1.imei,t1.skuid,count(*) as pv from(
select *,getSkuIdByVars(vars) as skuid from app_pvdata where date >= '$dateStartSql' and date <= '$dateEndSql' and vars != '' and code = '0203'
) t1 group by t1.skuid,t1.imei;"
   echo $sql_2
  hive -e "$sql_2"
}

getRecommendOrderCountTable(){
   sql_3="drop table if exists $dataBaseName.$tableName_3;CREATE TABLE $dataBaseName.$tableName_3 AS select a.customer_id as uid,b.product_id as skuid, count(*) as order_count from (
select * from mama100_owner.common_cust_order where type = '1' and order_status = '7'
and to_date(created_date) >= '$dateStartSql' and to_date(created_date) <= '$dateEndSql') a
left join (select * from mama100_owner.common_order_split_detail) b
ON a.id = b.order_id group by a.customer_id,b.product_id;"
  echo $sql_3
  hive -e "$sql_3"
}

getRecommendUserMarkTable(){
   sql_4="drop table if exists $dataBaseName.$tableName_4;CREATE TABLE $dataBaseName.$tableName_4 AS select sku_id as skuid,customer_id as uid,max(point) as mark_point from mama100_owner.o2o_product_comment where status = '1' and to_date(create_time) >= '$dateStartSql' and to_date(create_time) <= '$dateEndSql'
group by sku_id,customer_id;"
   echo $sql_4
   hive -e "$sql_4"
}

getRecommendAnalyisResultTable(){
   statDate=`date +%Y-%m-%d`
   sql_5="drop table if exists $dataBaseName.$tableName_5;CREATE TABLE $dataBaseName.$tableName_5 row format delimited fields terminated by '|' AS
select '$statDate' as stat_date,a.imei,a.uid,a.skuid,if(b.pv is null,0,b.pv) as pv,if(c.order_count is null,0,c.order_count) as order_count,if(d.mark_point is null,0,d.mark_point) as mark_point from $dataBaseName.$tableName_1 a
left join $dataBaseName.$tableName_2 b ON a.imei = b.imei and a.skuid = b.skuid
left join $dataBaseName.$tableName_3 c on a.uid = c.uid and a.skuid = c.skuid
left join $dataBaseName.$tableName_4 d on a.uid = d.uid and a.skuid = d.skuid;"
  echo $sql_5
  hive -e "$sql_5"
}

sqoopIntoMysql(){
  exeCmd="$sqoopPath/sqoop export --connect jdbc:mysql://192.168.115.101:16052/test --username YYYYYYY --password XXXXXXXXXXXX --export-dir '/hive/"$dataBaseName".db/"$tableName_5"/' --table recommend_analyis_result --fields-terminated-by '|'"
  echo $exeCmd
  ssh $1 $exeCmd
}

isOk(){
if [ $? -eq 0 ];then
  echo "***************_"$1"_run_success!!"
else
  echo "***************_"$1"_run_fail!!"
  exit
fi
}

createDB4Recom
isOk createDB4Recom

getRecommendBaseTable
isOk getRecommendBaseTable

getRecommendPvTable
isOk getRecommendPvTable

getRecommendOrderCountTable
isOk getRecommendOrderCountTable

getRecommendUserMarkTable
isOk getRecommendUserMarkTable

getRecommendAnalyisResultTable
isOk getRecommendAnalyisResultTable

sqoopIntoMysql $sqoopHost
isOk sqoopIntoMysql

echo "************************** running $0 END in $(date +%Y/%m/%d/%H:%M:%S) **************************"


