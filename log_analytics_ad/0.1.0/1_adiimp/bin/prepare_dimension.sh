#!/bin/bash

cd $(cd $(dirname $0)/.. && pwd)

. etc/ADIIMP.conf

#1 get advertise list
sql="select id from ad_base"
mysql $CONNECT_MICROLENS -N -e "$sql" > files/ad_info

#2 get advertise position information
sql="select id, code, type_id, is_optimizer from adp_base"
mysql $CONNECT_MICROLENS -N -e "$sql" > files/adp_info

#3 get live channel list
sql="select id from dim_live_category" 
mysql $CONNECT_MICROLENS -N -e "$sql" > files/live_info

#4 get material information
sql="select id, play_dur from mat_base"
mysql $CONNECT_MICROLENS -N -e "$sql" > files/mat_info

#5 get area mapping information & #6 get media information

${KETTLE_HOME}/pan.sh  -file=kettle/GET_DIM_FROM_ORACLE.ktr \
                       -level=Error                         \
                       -param:file_path=files                \
                       1>/dev/null 2>&1

if [[ $? -ne 0 ]]
then
    echo "update dim failed" 1>&2
    exit 1
fi
