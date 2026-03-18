#!/bin/bash
# 昨天
if [[ $1 == "" ]];then
   TD_DATE=`date -d '1 days ago' "+%Y-%m-%d"`
else
   TD_DATE=$1
fi

PRESTO_HOME=/export/server/presto/bin/presto
${PRESTO_HOME} --catalog mysql \
               --server 192.168.88.80:3306 \
			   #--schema yp_rpt
			   #--execute "select * from yp_dws.dws_sale_daycount limit 10;"
			   --file /mycodeexport_presto2mysql_presto.sql
