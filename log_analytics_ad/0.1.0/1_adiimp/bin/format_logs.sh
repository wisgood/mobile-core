#!/bin/bash
if [[ $# -eq 1 ]]
then
    log_type=$1 
    date=$(date -d "yesterday" +%Y%m%d)
elif [[ $# -eq 2 ]]
then
    log_type=$1
    date=$2
else
    echo "$(basename $0) <log type>[yyyymmdd]" 1>&2
    exit 1    
fi

if [[ $log_type != "monitor" ]] && [[ $log_type != "deliver" ]] && [[ $log_type != "click" ]]
then
   echo "Log type is error, only<monitor,deliver,click> are acceptable" 1>&2
   exit 1
fi


echo "$log_type fromat start at $(date +%T)"
#initialize variables
cd $(cd $(dirname $0)/.. && pwd)

. etc/ADIIMP.conf 
LOG_type=$(echo "$log_type" | sed 's/^\w/\U&/')
date_slash=$(date -d "$date" +%Y/%m/%d)


#check input directory
output_dir=$HDFS_FORMAT_DIR/$log_type/$date_slash
if $INSTALL_HADOOP/hadoop fs -test -e $output_dir 2>/dev/null
then
    $INSTALL_HADOOP/hadoop fs -rmr $output_dir   2>&1
    if [[ $? -ne 0 ]]
    then
        echo "Hadoop remove $output_dir Failed " 2>&1
        exit 1
    fi
fi

#check output directory 
input_dirs=""
warnings=""
log_dirs="$($INSTALL_HADOOP/hadoop fs -ls $HDFS_LOGS_DIR/$date_slash \
                            2>/dev/null | awk 'NF == 8{print $8}')"
for hour in $(seq -w 0 23 )
do
    if echo $log_dirs | grep -q $HDFS_LOGS_DIR/$date_slash/$hour  
    then
        input_dirs="${input_dirs}$HDFS_LOGS_DIR/$date_slash/$hour,"
    else
        warnings="${warnings}$HDFS_LOGS_DIR/$date_slash/$hour not exist\n"
    fi
done

input_dirs=$(echo $input_dirs |sed 's/,$//')
if [[ -z "$input_dirs" ]]
then
    echo "$log_type logs input directory is empty" 1>&2
    exit 1
fi


if [[ ! -z "$warnings" ]]
then
    echo -e "$warnings" 
    warnings="${warnings}------------------------------------------------------\n"
    warnings="${warnings}Script:$(basename $0) at $date                        \n"
    warnings="${warnings}------------------------------------------------------\n"
    echo -e "$warnings" | newmail -s "Warnings: adiimp" ${MAIL_RECEIVER[@]}
fi

#run format MR
files="files/ad_info,files/adp_info,files/ip_table,files/live_info,files/mat_info,files/media_info"
$INSTALL_HADOOP/hadoop jar jar/AD_DW.jar com.bi.ad.logs.$log_type.format.${LOG_type}LogFormatMR \
                                                    -files $files                                  \
                                                    -D input_dir=$input_dirs                       \
                                                    -D output_dir=$output_dir                      \
                                                    -D job_name="${LOG_type}LogFormat-$date" 2>&1

if [[ $? -ne 0 ]]
then
    echo "Hadoop ${LOG_type}LogFormatMR failed" 1>&2
    exit 1
fi

echo "$log_type format end at $(date +%T)";
