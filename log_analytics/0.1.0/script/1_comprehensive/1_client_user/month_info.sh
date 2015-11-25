#!/bin/bash
#
# NAME: month_info.sh
#
# DESC: Extract,transform from raw data to 
#       generate cleaning formatted data
#
# AUTH: fuyishan
start_time=$(date +%Y%m%d%H%M%S)
DIR_JARS=/disk6/datacenter/1_comprehensive/1_usernum/jar
cd $DIR_JARS
end=$1
beg=$(date -d "$end -1months" +%Y/%m)
echo $beg
echo $end

if hadoop fs -test -e /dw/logs/1_comprehensive/3_user/month/format/$beg 2>&1
    then
      hadoop fs -rmr /dw/logs/1_comprehensive/3_user/month/format/$beg
fi
hadoop jar log_analytics.jar com.bi.newlold.homeserver.mergedhs.format.MergedhsExtract -i /userdata/tmp/homeserver/mergedhs/$beg -o /dw/logs/1_comprehensive/3_user/month/format/$beg

if hadoop fs -test -e /dw/logs/1_comprehensive/3_user/month/middle/$beg 2>&1
    then
      hadoop fs -rmr /dw/logs/1_comprehensive/3_user/month/middle/$beg
fi
hadoop jar log_analytics.jar com.bi.newlold.homeserver.mergedhs.middle.HistoryMonthUser -i /dw/logs/1_comprehensive/3_user/month/format/$beg -o /dw/logs/1_comprehensive/3_user/month/middle/$beg


if hadoop fs -test -e /dw/logs/1_comprehensive/3_user/month/result/$beg 2>&1
    then
      hadoop fs -rmr /dw/logs/1_comprehensive/3_user/month/result/$beg
fi
hadoop jar log_analytics.jar com.bi.newlold.homeserver.mergedhs.result.MonthUserCompute -i /dw/logs/1_comprehensive/3_user/month/middle/$beg -o /dw/logs/1_comprehensive/3_user/month/result/$beg


DIR_KETTLE=/disk6/datacenter/1_comprehensive/1_usernum/kettle
cd $DIR_KETTLE
date=$(date -d "$end -1months" +%Y%m01)
echo ${date}
date_dir=$beg
echo $date_dir
ISP_PATH="hdfs://192.168.117.20:8020/dw/logs/1_comprehensive/3_user/month/result"
input_path1=${ISP_PATH}/${date_dir}
echo ${input_path1}
# load to db
/disk8/software/data-integration/kitchen.sh \
	-file=F_NEWOLD_KETTLE.kjb \
	-level=Error \
  -param:date=${date} \
	-param:input_path1=${input_path1} \
	 -param:date_id=MONTH_ID \
	-param:db_table_name=F_NEWOLD_MONTH \
	 > F_NEWOLD_MONTH_KETTLE${date}.log
end_time=$(date +%Y%m%d%H%M%S)

echo $(($end_time-$start_time))