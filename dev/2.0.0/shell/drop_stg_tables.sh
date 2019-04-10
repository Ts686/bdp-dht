#!/bin/bash
# It is a shell for delete hive（src/ods）stg table, and saving last two file
# It is has 4 feacth

# 设置保存个数，默认为2
save_count=2
# 设置临时文件存储位置
stg_path=/home/hive/drop_stg/
# 临时文件的后缀名称
tag="_tag"
# hive表的登记脚本
hive_tableName="hiveNames"

source /etc/profile


# 在每次执行该脚本之前，先删除该路径下的所有文件
clearStgPath(){
if [ -d ${stg_path} ]
then
	rm -rf ${stg_path}*
	#echo "clear file path:" ${stg_path}
else
	mkdir -p ${stg_path}
	#echo "create file path:" ${stg_path}
fi	
echo "1/4 - clearStgPath "
}

# 查询hive 数据库，将_src _ods层所有的表写到：/home/hive/drop_stg/hiveNames
getSuffixStgTableName(){
hive_src=dc_src
hive_ods=dc_ods

for db_name in dc_src dc_ods
do
    hive -S --database $db_name -e "show tables;" | while read table_name; do
 	echo ${table_name} >> $stg_path${hive_tableName}
    done
done
echo "2/4 - getSuffixStgTableName "
}


#
getDropSuffixStgTableName(){
# 读取/home/hive/drop_stg/hiveNames文件中的内容，从中筛选出—_src层的 *transaction_history_log*和_ods层的 _ods结尾的表名称
# 然后依据/home/hive/drop_stg/hiveNames 下的文件进行过滤，分组到单独表名下的文件
# cat /home/hive/drop_stg/hiveNames | grep  '_ods$'
#cat ${stg_path}${hive_tableName}| grep  '_ods$'| while read ods_table
cat ${stg_path}${hive_tableName}| grep -E "*ods$|*transaction_history_log*"| while read ods_table
do
	for suffix in _sp _rt all
	do
		if [ `cat ${stg_path}${hive_tableName} |grep $ods_table$suffix |wc -l` -gt $save_count ]
		then
			cat  ${stg_path}${hive_tableName} |grep $ods_table$suffix |sort -n >  $stg_path$ods_table$suffix$tag
		fi
	done
done 

# 清空/home/hive/drop_stg/hiveNames文件
cat /dev/null > $stg_path${hive_tableName}

# 生产删除的 hive 脚本
ls  $stg_path|grep '_tag$'| gawk '{print $NF}' | while read filterLines
do
	size=$((`cat ${stg_path}${filterLines} |wc -l`))
	limit=$((${size} - ${save_count}))

	
	#if [ $limit -le 0 ]
	#then 
	#	contains
	#if
	
	i=0
	cat ${stg_path}${filterLines} |sort -n|while read line
	do
		i=$(($i+1))
		if [ $i -le $limit ]
		then
			if [ `echo ${line} | grep 'transaction_history_log'|wc -l` -eq 1 ]
			then
				echo "drop table dc_src."${line}";"
				echo "drop table dc_src."${line}";" >> $stg_path${hive_tableName}
			else
				echo "drop table dc_ods."${line}";"
				echo "drop table dc_ods."${line}";" >> $stg_path${hive_tableName}
			fi
		fi
	done	
done
	echo "3/4 - getDropSuffixStgTableName "
}

# 执行删除任务
dropHiveSuffixStgTable(){
	hive -f ${stg_path}${hive_tableName}
	echo "4/4 - dropHiveSuffixStgTable "
}


# 1 清空目录
clearStgPath
# 2 读取hive数据库（src/ods）层表
getSuffixStgTableName
# 3 生成删除计划
getDropSuffixStgTableName
# 4 开始执行删除任务
dropHiveSuffixStgTable

