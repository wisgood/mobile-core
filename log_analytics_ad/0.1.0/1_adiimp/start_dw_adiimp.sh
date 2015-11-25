#!/bin/bash
#----------------------------------------------------------------------------
#Filename   : start_dw_adiimp.sh
#Author     : wangzhe <wangzhe@funshion.com>
#CreateDate : 20130531
#Description: it is used to start up all the scripts of adiimp            
#ChangeList :
#-----------------------------------------------------------------------------
Usage='
       sh start_dw_adiimp.sh [-d] [-s]
       -d : set the specifed date to calculate,[yyyymmdd] 
       -s : to skip the format step.
       if none of the options were set, create it all.
 '

date=$(date -d"yesterday" +%Y%m%d)
skip=1

cd $(cd $(dirname $0) && pwd) 
. etc/ADIIMP.conf

while getopts ":d:s" opt
do
    case "$opt" in 
        d)  
            if [[ $OPTARG == -* ]] || [[ $OPTARG == "" ]]
            then
                echo "-d need an argument"
                echo "$Usage"             
                exit 1
            fi
            date=$OPTARG
            ;;
        s)  skip=0 
            ;;
        \?) echo "Invalid option: -$OPTARG" 
            echo "$Usage" 
            exit 1
            ;;
    esac
done


log_dir=logs/$date
[[ ! -d $log_dir ]] && mkdir -p $log_dir
elog=$log_dir/fact_error.log
olog=$log_dir/fact_output.log
>$elog
>$olog

function error(){
    if [[ -s $elog ]]
    then
        echo "----------------------------------------------------------" >> $elog
        echo "Error scripts:$PWD/$1                                     " >> $elog
        echo "----------------------------------------------------------" >> $elog
        echo "$(date)"                                                    >> $elog
        cat $elog | newmail -s "ADIIMP error " ${MAIL_RECEIVER[@]}
        exit 1
    fi
}

sh bin/prepare_dimension.sh     2>$elog   1>$olog
error bin/prepare_dimension.sh

#format logs
function format(){
    type=$1
    local elog=$log_dir/${type}_error.log
    local olog=$log_dir/${type}_output.log
    sh bin/format_logs.sh $type $date 2>$elog  1>$olog 
    error "bin/format_logs.sh $type"
}

if [[ $skip -eq 1 ]]
then
    format monitor &
    format deliver &
    format click   &
    wait
fi

sh bin/adiimp_fact.sh  $date  2>>$elog  1>>$olog
error bin/adiimp_fact.sh

sh bin/imp_fact.sh     $date  2>>$elog  1>>$olog
error bin/imp_fact.sh 

[[ -d logs/$(date -d "$date - 30 days" +%Y%m%d) ]] && rm -r logs/$(date -d "$date - 30 days" +%Y%m%d)
