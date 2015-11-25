#!/bin/bash

if [[ $# -eq 0 ]]
then
    date=$(date -d "yesterday" +%Y%m%d)
elif [[ $# -eq 1 ]]
then
    date=$1
else
    echo "$(basename $0)[yyyymmdd]" 2>&1
    exit 1
fi

cd $(cd $(dirname $0)/.. && pwd)
.  etc/ADIIMP.conf

date_slash=$(date -d"$date" +%Y/%m/%d)

echo "imp start at $(date +%T)"
#check output dirctory
output_dir=$HDFS_FACT_DIR/f_ad_hermes_imp_day/$date_slash
if $INSTALL_HADOOP/hadoop fs -test -e $output_dir 2>/dev/null
then
    $INSTALL_HADOOP/hadoop fs -rmr $output_dir 2>&1
    if [[ $? -ne 0 ]]
    then
        echo "Hadoop remove $output_dir Failed " 2>&1
        exit 1
    fi
fi

#check input directory
log_types={monitor,click}
input_dirs=""
warnings=""
format_dirs="$($INSTALL_HADOOP/hadoop fs -ls $HDFS_FORMAT_DIR/$log_types/$date_slash \
                        2>/dev/null | awk '/SUCCESS/{sub(/\/[^/]*$/, "", $8); print $8}')"

for type in $(echo $log_types | sed 's/[{}]//g; s/,/ /g')
do
    if echo $format_dirs | grep -q $HDFS_FORMAT_DIR/$type/$date_slash
    then
        input_dirs="${input_dirs}$HDFS_FORMAT_DIR/$type/$date_slash/format,"
    else
        warnings="${warnings}$HDFS_LOGS_DIR/$type/$date_slash/format not exist\n"
    fi
done

input_dirs=$(echo $input_dirs |sed 's/,$//')
if [[ -z "$input_dirs" ]]
then
    echo "format input directory is empty" 1>&2
    exit 1
fi


if [[ ! -z "$warnings" ]]
then
   echo -e "$warnings" 
   warnings="${warnings}------------------------------------------------------\n"
   warnings="${warnings}Script:$(basename $0) at $date                        \n"
   warnings="${warnings}------------------------------------------------------\n"
   echo -e "$warnings" | newmail -s "Warnings: adiimp"  ${MAIL_RECEIVER[@]}
   exit 1
fi


#rum mapreduce 
$INSTALL_HADOOP/hadoop jar jar/AD_DW.jar com.bi.ad.fact.adiimp.IMPMR                      \
                                            -files files/area_mapping_info,files/adp_info \
                                            -D input_dir=$input_dirs                      \
                                            -D output_dir=$output_dir                     \
                                            -D reduce_num=30                              \
                                            -D stat_date=$date                            \
                                            -D job_name="IMPMR-$date" 2>&1

if [[ $? -ne 0 ]]
then
    echo "Hadoop fact ADIIMPMR failed" 1>&2
    exit 1
fi

[[ ! -d logs/$date ]] && mkdir -p logs/$date
${KETTLE_HOME}/kitchen.sh  -file=$PWD/kettle/ADI_IMP_JOB.kjb                            \
                           -level=Error                                                 \
                           -param:specified_ktr=$PWD/kettle/F_AD_HERMES_IMP_DAY.ktr     \
                           -param:date=${date}                                          \
                           -param:db_table_name=F_AD_HERMES_IMP_DAY                 \
                           -param:input_path="hdfs://192.168.117.20:8020${output_dir}"  \
                           > logs/$date/F_AD_HERMES_IMP_DAY.log
if [[ $? -ne 0 ]]
then
    echo "F_AD_HERMES_ADI_IMP_DAY.ktr failed" 1>&2
    exit 1
fi
echo "imp end at $(date +%T)"
