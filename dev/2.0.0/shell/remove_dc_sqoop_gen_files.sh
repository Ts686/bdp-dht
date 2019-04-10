#!/bin/bash
# 每天定时[晚上22:00]删除sqoop产生的jar 、class 、java临时文件，BEFORE_DAYS默认为两天前临时文件
# crontab command
# 0 22 * * * /usr/local/wonhigh/dc/client/shell_test/remove_dc_sqoop_gen_files_test.sh >> \
# /data/logs/wonhigh/dc/client/remove_dc_sqoop_gen_files.log 2>&1
BEFORE_DAYS=2
pivot_date_str=$(date -d "-${BEFORE_DAYS} days" +%Y%m%d)

function deleteSqoopGenFiles() {
  dirFiles=$1

  #获取文件日期
  for genFile in $(ls $dirFiles)
    do
      if [ `echo $genFile | grep '.java'` ]
      then
        fileDateStr="${genFile:(-13):8}"
      elif [ `echo $genFile | grep '.jar'` ]
      then
        fileDateStr="${genFile:(-12):8}"
      elif [ `echo $genFile | grep '.class'` ]
      then
        fileDateStr="${genFile:(-14):8}"
      fi

        
        if echo $fileDateStr | egrep -q '^[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]$'
          then
            pivot_date=`date -d "$pivot_date_str" +%s`
            file_date=`date -d "$fileDateStr" +%s`

            if [ $pivot_date -gt $file_date ]
              then
                rm -f $genFile
            fi
        fi
    done
}

echo start to clean dc sqoop generated imported java,class and jar generated files at `date`.
deleteSqoopGenFiles '/home/hive/bindir/import_*.class'
deleteSqoopGenFiles '/home/hive/bindir/import_*.java'
deleteSqoopGenFiles '/home/hive/bindir/import_*.jar'

deleteSqoopGenFiles '/home/hive/outdir/import_*.class'
deleteSqoopGenFiles '/home/hive/outdir/import_*.java'
deleteSqoopGenFiles '/home/hive/outdir/import_*.jar'
echo done with cleaning dc sqoop generated imported java,class and jar generated files at `date`.

echo start to clean dc sqoop generated exported java,class and jar generated files at `date`.
deleteSqoopGenFiles '/home/hive/bindir/*_export_*.class'
deleteSqoopGenFiles '/home/hive/bindir/*_export_*.java'
deleteSqoopGenFiles '/home/hive/bindir/*_export_*.jar'

deleteSqoopGenFiles '/home/hive/outdir/*_export_*.class'
deleteSqoopGenFiles '/home/hive/outdir/*_export_*.java'
deleteSqoopGenFiles '/home/hive/outdir/*_export_*.jar'
echo done with cleaning dc sqoop generated exported java,class and jar generated files at `date`.

echo start to clean hadoop job /tmp/hadoop-unjar temp files at `date`.
find /tmp/ -name 'hadoop-unjar?*' 2>/dev/null | xargs rm -rf
echo done with cleaning hadoop job /tmp/hadoop-unjar temp files at `date`.