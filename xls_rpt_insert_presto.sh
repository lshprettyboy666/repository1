#!/bin/bash
# 昨天
if [[ $1 == "" ]];then
   TD_DATE=`date -d '1 days ago' "+%Y-%m-%d"`
else
   TD_DATE=$1
fi

PRESTO_HOME=/export/server/presto/bin/presto
${PRESTO_HOME} --catalog hive \
               --server 192.168.88.80:8090 \
			   --file /mycode/insert_rpt_sale.sql
			   
			   
${PRESTO_HOME} --catalog hive \
               --server 192.168.88.80:8090 \
			   --file /mycode/insert_rpt_sku.sql
			   
			   
${PRESTO_HOME} --catalog hive \
               --server 192.168.88.80:8090 \
			   --file /mycode/insert_rpt_user.sql