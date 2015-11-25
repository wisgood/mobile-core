#!/bin/bash
cd $(dirname "$0")

source ../etc/log_analytics.conf

input_path1=/dw/logs/mobile_result/app_mobile_date_chl_play_num

date=$(date -d "yesterday" +%Y%m%d)

if [[ $# == 1 ]]
then
	date=$1
fi

year=${date:0:4}
month=${date:4:2}
day=${date:6:2}

${KETTLE_HOME}/pan.sh \
	-file=${KTR_KJB}/F_MOBILE_DATE_CHL_PLAY_NUM.ktr \
	-level=Detailed \
	-param:input_path1=${input_path1} \
	-param:year=${year} \
	-param:month=${month} \
	-param:day=${day} \
	 > ${KETTLE_LOG}/F_MOBILE_DATE_CHL_PLAY_NUM.log
