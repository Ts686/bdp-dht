#!/bin/bash
#####
## merge small files in hdfs
#####
TABLE_SUBFIX_WONHIGH=_wonhigh
HIVE_DB_NAME_ODS=dc_ods
source /etc/profile

$HIVE_HOME/bin/hive -S --database $HIVE_DB_NAME_ODS -e 'show tables' | while read table_name; do
  if [[ "$table_name" =~ _stg$ || "$table_name" =~ _tmp$ || "$table_name" =~ _temp$ || "$table_name" =~ _cln$ ]]; then
    echo "the table is not required"
  else
    #1.drop table
    $HIVE_HOME/bin/hive -S --database $HIVE_DB_NAME_ODS -e "drop table if exists ${table_name}${TABLE_SUBFIX_WONHIGH}"
    #2.create table
    $HIVE_HOME/bin/hive -S --database $HIVE_DB_NAME_ODS -e "create table if not exists ${table_name}${TABLE_SUBFIX_WONHIGH} like $table_name"
    #3.move data into temp table
    $HIVE_HOME/bin/hive -S --database $HIVE_DB_NAME_ODS -e "insert into table ${table_name}${TABLE_SUBFIX_WONHIGH} partition(biz_date) select * from $table_name"
    #4.clear data in table
    $HIVE_HOME/bin/hive -S --database $HIVE_DB_NAME_ODS -e "truncate table $table_name"
    #5.move data into table from temp table
    $HIVE_HOME/bin/hive -S --database $HIVE_DB_NAME_ODS -e "insert into table $table_name partition(biz_date) select * from ${table_name}${TABLE_SUBFIX_WONHIGH}"
    #6.drop temp table
    $HIVE_HOME/bin/hive -S --database $HIVE_DB_NAME_ODS -e "drop table if exists ${table_name}${TABLE_SUBFIX_WONHIGH}"
  fi
done