#!/bin/bash

if [[ $# -eq 0 ]]
then
    start_date=$(date -d "yesterday" +%Y%m%d)
    end_date=$start_date
elif [[ $# -eq 1 ]]
then
    start_date=$1
    end_date=$1

elif [[ $# -eq 2 ]]
then
    start_date=$1
    end_date=$2
else
    echo "Wrong parameters"
    echo "$(basename $0):[yyyymmdd] [yyyymmdd]"
    exit 1
fi

while (($start_date <= $end_date))
do
    sh start_dw_adiimp.sh -d $start_date
    start_date=$(date -d"$start_date + 1day" +%Y%m%d)
done
    
