#!/bin/bash
cd $(dirname "$0")

source ../etc/log_analytics.conf

main_path=/dw/logs/mobile/result/f_mobile_date_channel
input_path1=${main_path}/date_channel_fbuffer_count     # 观看数 vv_num
input_path2=${main_path}/date_channel_playtm_vtm_sum    # 播放时长play_length
input_path3=${main_path}/date_channel_download_count    # 下载数 down_num
input_path4=${main_path}/date_channel_fbuffer_distinctih_count  # 观看节目数 view_video_num
input_path5=${main_path}/date_channel_fbuffer_distinctmac_count # 观看用户数 view_user_num


date=$(date -d "yesterday" +%Y%m%d)

if [[ $# == 1 ]]
then
	date=$1
fi

year=${date:0:4}
month=${date:4:2}
day=${date:6:2}

${KETTLE_HOME}/pan.sh \
	-file=${KTR_KJB}/F_MOBILE_DATE_CHANNEL.ktr \
	-level=Detailed \
	-param:input_path1=${input_path1} \
	-param:input_path2=${input_path2} \
	-param:input_path3=${input_path3} \
	-param:input_path4=${input_path4} \
	-param:input_path5=${input_path5} \
	-param:year=${year} \
	-param:month=${month} \
	-param:day=${day} \
	 > ${KETTLE_LOG}/F_MOBILE_DATE_CHANNEL.log
