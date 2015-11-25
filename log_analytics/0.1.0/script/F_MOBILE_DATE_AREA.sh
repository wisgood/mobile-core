#!/bin/bash
cd $(dirname "$0")

source ../etc/log_analytics.conf

area_path=/dw/logs/mobile/result/f_mobile_date_area

input_path1=${area_path}/date_city_bootstrap_distintbymaccod_count  # 启动用户数 boot_user_num
input_path2=${area_path}/date_city_bootstrap_count   	# 启动数 boot_num
input_path3=${area_path}/date_city_new_user_count 		# 新增用户数 new_user_num
input_path4=${area_path}/date_city_activenew_user_count 	# 有效新增用户数 valid_new_user_num
input_path5=${area_path}/date_city_fbuffer_count 		# 观看数 vv_num
input_path6=${area_path}/date_city_playtm_vtm_sum 		# 播放时长 play_length 
input_path7=${area_path}/date_city_fbuffer_distinctih_count 		# 观看节目数 view_video_num
input_path8=${area_path}/date_city_fbuffer_distinctmac_count 		# 观看用户数 view_user_num
input_path9=${area_path}/date_city_download_distinctih_count 		# 下载节目数 down_num
input_path0=${area_path}/date_city_download_distinctmac_count 		# 下载用户数 down_user_num

date=$(date -d "yesterday" +%Y%m%d)

if [[ $# == 1 ]]
then
	date=$1
fi

year=${date:0:4}
month=${date:4:2}
day=${date:6:2}

${KETTLE_HOME}/pan.sh \
	-file=${KTR_KJB}/F_MOBILE_DATE_AREA.ktr \
	-level=Detailed \
	-param:input_path1=${input_path1} \
	-param:input_path2=${input_path2} \
	-param:input_path3=${input_path3} \
	-param:input_path4=${input_path4} \
	-param:input_path5=${input_path5} \
	-param:input_path6=${input_path6} \
	-param:input_path7=${input_path7} \
	-param:input_path8=${input_path8} \
	-param:input_path9=${input_path9} \
	-param:input_path0=${input_path0} \
	-param:year=${year} \
	-param:month=${month} \
	-param:day=${day} \
	 > ${KETTLE_LOG}/F_MOBILE_DATE_AREA${date}.log
