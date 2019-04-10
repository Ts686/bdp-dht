#!/bin/bash
#drop去重旧表和导出临时表
hive_dbs=(dc_ods)
hive_src=(dc_src)
source /etc/profile
dropTimeStamp=`date -d -10day +%Y%m%d%H%M%S` 
echo "$dropTimeStamp"
for db_name in "${hive_dbs[@]}"; do
    hive -S --database $db_name -e "show tables;" | while read table_name; do
        if [ `echo $table_name|grep '_bak'|wc -l` -eq 1 ];then
            timeStr=${table_name##*_bak}
            dropTimeStampStr="$dropTimeStamp"
             if [[ "$timeStr" < "$dropTimeStampStr" ]];then
            echo "drop table  $table_name;"
            fi
        fi
    done | hive --database $db_name
done

for db_name in "${hive_dbs[@]}"; do
    hive -S --database $db_name -e "show tables;" | while read table_name; do
        if [ `echo $table_name|grep '_stg'|wc -l` -eq 1 ];then
            echo "drop table  $table_name;"
        fi
    done | hive --database $db_name
done

for db_name in "${hive_src[@]}"; do
    hive -S --database $db_name -e "show tables;" | while read table_name; do
        if [ `echo $table_name|grep '_stg'|wc -l` -eq 1 ];then
            echo "drop table  $table_name;"
        fi
    done | hive --database $db_name
done