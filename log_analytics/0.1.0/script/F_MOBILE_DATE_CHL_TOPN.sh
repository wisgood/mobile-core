#!/bin/bash
cd $(dirname "$0")

source ../etc/log_analytics.conf

input_path1=/dw/logs/mobile_result/app_mobile_date_chl_topn

date=$(date -d "yesterday" +%Y%m%d)

if [[ $# == 1 ]]
then
	date=$1
fi

last_day=$(date -d "$date -1day" +%Y%m%d)
last_week=$(date -d "$date -7day" +%Y%m%d)


echo $last_day,$last_week
year=${date:0:4}
month=${date:4:2}
day=${date:6:2}

${KETTLE_HOME}/pan.sh \
	-file=${KTR_KJB}/F_MOBILE_DATE_CHL_TOPN.ktr \
	-level=Detailed \
	-param:input_path1=${input_path1} \
	-param:last_day=${last_day} \
	-param:last_week=${last_week} \
	-param:year=${year} \
	-param:month=${month} \
	-param:day=${day} \
	 > ${KETTLE_LOG}/F_MOBILE_DATE_CHL_TOPN.log
