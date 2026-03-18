#!/bin/bash
# 昨天
if [[ $1 == "" ]];then
   TD_DATE=`date -d '1 days ago' "+%Y-%m-%d"`
else
   TD_DATE=$1
fi

PRESTO_HOME=/export/server/presto/bin/presto
${PRESTO_HOME} --catalog mysql \
               --server 192.168.88.80:8090 \
			   --file /mycode/create_rpt_sale_tables_presto.sql
			   
			   
${PRESTO_HOME} --catalog hive \
               --server 192.168.88.80:8090 \
			   --file /mycode/create_rpt_sku_tables_presto.sql
			   
			   
${PRESTO_HOME} --catalog hive \
               --server 192.168.88.80:8090 \
			   --file /mycode/create_rpt_user_tables_presto.sql