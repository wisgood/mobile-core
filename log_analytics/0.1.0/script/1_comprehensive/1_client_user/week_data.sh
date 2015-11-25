#!/bin/bash
#
# NAME: week_data.sh
#
# DESC: Extract,transform from raw data to 
#       generate cleaning formatted data
#
# AUTH: fuyishan
start_time=$(date +%Y%m%d%H%M%S)

DIR_JARS=/disk6/datacenter/1_comprehensive/1_usernum/jar
cd $DIR_JARS
end=$1
beg=$(date -d "$end -7days" +%Y%m%d)
DIR_WEEKDATE=$(date -d "$beg" +%Y/%m/%d)
while(($beg<$end))
do
        DIR_DATE=$(date -d "$beg" +%Y/%m/%d)
        if hadoop fs -test -e /userdata/tmp/homeserver/extractedhs/$DIR_DATE 2>&1
        then
                DIR_ORIGINDATA_EXTRACTEDHS_INPUT="/userdata/tmp/homeserver/extractedhs/$DIR_DATE,${DIR_ORIGINDATA_EXTRACTEDHS_INPUT}"
        fi
        beg=$(date -d "$beg +1days" +%Y%m%d)
done

DIR_ORIGINDATA_EXTRACTEDHS_INPUT=$(echo $DIR_ORIGINDATA_EXTRACTEDHS_INPUT | sed 's/,$//')
echo $DIR_ORIGINDATA_EXTRACTEDHS_INPUT

if hadoop fs -test -e /dw/logs/1_comprehensive/3_user/week/format/$DIR_WEEKDATE 2>&1
    then
      hadoop fs -rmr /dw/logs/1_comprehensive/3_user/week/format/$DIR_WEEKDATE
    fi
hadoop jar log_analytics.jar com.bi.newlold.homeserver.extractedhs.format.ExtractdFormatMRUTL --input $DIR_ORIGINDATA_EXTRACTEDHS_INPUT --output /dw/logs/1_comprehensive/3_user/week/format/$DIR_WEEKDATE


beg=$(date -d "$end -63days" +%Y%m%d)
echo $beg
echo $end
while(($beg<$end))
do
        DIR_DATE=$(date -d "$beg" +%Y/%m/%d)
        if hadoop fs -test -e /dw/logs/1_comprehensive/3_user/week/format/$DIR_DATE 2>&1
        then
                DIR_ORIGINDATA_WEEKINFO_INPUT="/dw/logs/1_comprehensive/3_user/week/format/$DIR_DATE,${DIR_ORIGINDATA_WEEKINFO_INPUT}"
                PARS_WEEKDATE_IDS="$beg,${PARS_WEEKDATE_IDS}"
        fi
        beg=$(date -d "$beg +7days" +%Y%m%d)
done

DIR_ORIGINDATA_WEEKINFO_INPUT=$(echo $DIR_ORIGINDATA_WEEKINFO_INPUT | sed 's/,$//')
echo $DIR_ORIGINDATA_WEEKINFO_INPUT
PARS_WEEKDATE_IDS=$(echo $PARS_WEEKDATE_IDS | sed 's/,$//')
echo $PARS_WEEKDATE_IDS
echo $DIR_WEEKDATE
if hadoop fs -test -e /dw/logs/1_comprehensive/3_user/week/f_client_user_newold_week_info/$DIR_WEEKDATE 2>&1
    then
      hadoop fs -rmr /dw/logs/1_comprehensive/3_user/week/f_client_user_newold_week_info/$DIR_WEEKDATE
    fi
hadoop jar log_analytics.jar com.bi.newlold.middle.weekinfo.WeekInfoMRUTL --input $DIR_ORIGINDATA_WEEKINFO_INPUT --output /dw/logs/1_comprehensive/3_user/week/f_client_user_newold_week_info/$DIR_WEEKDATE  --weeksday $PARS_WEEKDATE_IDS

if hadoop fs -test -e /dw/logs/1_comprehensive/3_user/week/f_client_user_newold_week_count/$DIR_WEEKDATE 2>&1
    then
      hadoop fs -rmr /dw/logs/1_comprehensive/3_user/week/f_client_user_newold_week_count/$DIR_WEEKDATE
    fi
hadoop jar log_analytics.jar com.bi.newlold.result.week.WeekNewOldCountMR --input /dw/logs/1_comprehensive/3_user/week/f_client_user_newold_week_info/$DIR_WEEKDATE --output /dw/logs/1_comprehensive/3_user/week/f_client_user_newold_week_count/$DIR_WEEKDATE

DIR_KETTLE=/disk6/datacenter/1_comprehensive/1_usernum/kettle
cd $DIR_KETTLE
date=$(date -d "$end -7days" +%Y%m%d)
date_dir=$(date -d "$date" +%Y/%m/%d)
echo $date_dir
ISP_PATH="hdfs://192.168.117.20:8020/dw/logs/1_comprehensive/3_user/"
input_path1=${ISP_PATH}/week/f_client_user_newold_week_count/${date_dir}
# load to db
/disk8/software/data-integration/kitchen.sh \
	-file=F_NEWOLD_KETTLE.kjb \
	-level=Error \
  -param:date=${date} \
	-param:input_path1=${input_path1} \
	 -param:date_id=WEEK_ID \
	-param:db_table_name=F_NEWOLD_WEEK \
	 > F_NEWOLD_WEEK_KETTLE${date}.log
end_time=$(date +%Y%m%d%H%M%S)
echo $(($end_time-$start_time))