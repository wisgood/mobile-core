#!/bin/bash
cd $(dirname "$0")

source ../etc/log_analytics.conf

qudao_plat_path=/dw/logs/mobile/result/f_mobile_date_qd_plat

input_path1=${qudao_plat_path}/date_qudao_plat_bootstrap_distintbymaccod_count  # 启动用户数 boot_user_num
input_path2=${qudao_plat_path}/date_qudao_plat_bootstrap_count   	# 启动数 boot_num
input_path3=${qudao_plat_path}/date_qudao_plat_new_user_count 		# 新增用户数 new_user_num
input_path4=${qudao_plat_path}/date_qudao_plat_activenew_user_count 	# 有效新增用户数 valid_new_user_num
input_path5=${qudao_plat_path}/date_qudao_plat_fbuffer_count 		# 观看数 vv_num
input_path6=${qudao_plat_path}/date_qudao_plat_playtm_vtm_sum 		# 播放时长 play_length 
input_path7=${qudao_plat_path}/date_qudao_plat_fbuffer_distinctih_count 		# 观看节目数 view_video_num
input_path8=${qudao_plat_path}/date_qudao_plat_fbuffer_distinctmac_count 		# 观看用户数 view_user_num
input_path9=${qudao_plat_path}/date_qudao_plat_download_distinctih_count 		# 下载节目数 down_num
input_path0=${qudao_plat_path}/date_qudao_plat_download_distinctmac_count 		# 下载用户数 down_user_num

echo ${input_path1}
echo ${input_path2}
echo ${input_path3}
echo ${input_path4}
echo ${input_path5}
echo ${input_path6}
echo ${input_path7}
echo ${input_path8}
echo ${input_path9}
echo ${input_path0}

date=$(date -d "yesterday" +%Y%m%d)

if [[ $# == 1 ]]
then
	date=$1
fi

year=${date:0:4}
month=${date:4:2}
day=${date:6:2}
echo $year,$month,$day

${KETTLE_HOME}/pan.sh \
	-file=${KTR_KJB}/F_MOBILE_DATE_QD_PLAT.ktr \
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
	 > ${KETTLE_LOG}/F_MOBILE_DATE_QD_PLAT${date}.log
