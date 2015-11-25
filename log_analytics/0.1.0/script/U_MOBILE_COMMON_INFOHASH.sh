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


echo $last_day,$last_week
year=${date:0:4}
month=${date:4:2}
day=${date:6:2}
output=/tmp/output.csv

${KETTLE_HOME}/pan.sh \
	-file=${KTR_KJB}/U_MOBILE_COMMON_INFOHASH.ktr \
	-level=Detailed \
	-param:year=${year} \
	-param:month=${month} \
	-param:day=${day} \
        -param:date=${date} \
        -param:output=${output} \
	 > ${KETTLE_LOG}/U_MOBILE_COMMON_INFOHASH${date}.log
