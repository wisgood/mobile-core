#!/bin/bash
cd $(dirname "$0")

source ../etc/log_analytics.conf


date=$(date -d "yesterday" +%Y%m%d)

if [[ $# == 1 ]]
then
	date=$1
fi

last_day=$(date -d "$date -1day" +%Y%m%d)
last_week=$(date -d "$date -7day" +%Y%m%d)

${KETTLE_HOME}/pan.sh \
	-file=${KTR_KJB}/F_MOBILE_DAY_REPORT.ktr \
	-level=Detailed \
	-param:date=${date} \
	-param:last_day=${last_day} \
	-param:last_week=${last_week} \
	 > ${KETTLE_LOG}/F_MOBILE_DAY_REPORT.log
