package cn.wonhigh.dc.client.common.model;

import cn.wonhigh.dc.client.common.util.DateUtils;
import org.apache.log4j.Logger;

import java.io.Serializable;
import java.util.*;

/**
 * 任务属性配置
 * 
 * @author wang.w
 * 
 */
public class TaskPropertiesConfig implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7618748015852121407L;

	private static final Logger logger = Logger.getLogger(TaskPropertiesConfig.class);
	/** 任务主键id */
	private Integer id;

	/** 任务类型 100.sqoop-导入 200.hive--导出 */
	private Integer taskType;

	/** 组名 */
	private String groupName;

	/** 调度任务名称 */
	private String triggerName;

	/** 源数据库id */
	private Integer sourceDbId;

	/** 源数据库配置信息 */
	private TaskDatabaseConfig sourceDbEntity;

	/** 源数据库父表 */
	private String sourceParentTableId;

	/** 源数据库主表 */
	private String sourceTable;

	/** 源数据库历史备份表 */
	private String hisSourceTable;
	/** 依赖的关系表id集合 */
	private List<Integer> dependencyTaskIds;

	/** 依赖的关系表实体集合 */
	private List<TaskPropertiesConfig> dependencyTaskList;

	/** 关联字段 */
	private List<String> relationColumns;

	/** 源表主键列表 */
	private List<String> primaryKeys;

	/** 需要查询的字段 逗号分隔 */
	private List<String> selectColumns;

	/** 需要查询历史表的字段 逗号分隔，如果真实表和历史表字段不一致需要填此字段 */
	private List<String> hisSelectColumns;

	/** 特殊字段类型 */
	private List<String> specialColumnTypeList;

	/** 同步时间戳字段 */
	private List<String> syncTimeColumn;

	/** 目标数据库id */
	private Integer targetDbId;

	/** 目标据库配置信息 */
	private TaskDatabaseConfig targetDbEntity;

	/** 目标数据表 */
	private String targetTable;

	/** 目标数据库字段 */
	private List<String> targetColumns;

	/** 执行频率 默认值：3600秒 */
	private Integer syncFreqSeconds = 3600;

	/** 是否直接使用sql语句 0.不使用 1.使用 */
	private Integer useSqlFlag = 0;

	/** 是否主从表 0.主表 1.从表 */
	private Integer isSlaveTable;

	/** 是否全量覆盖 0.不覆盖 1.覆盖 */
	private Integer isOverwrite = 0;

	/** 是否物理删除 0.逻辑删除 1.物理删除 */
	private Integer isPhysicalDel = 0;

	/** 过滤条件 */
	private List<String> filterConditions;

	/** 版本号 */
	private String version;

	/** 任务内容 可能存在多个子任务 */
	private SortedMap<Integer, String> taskContent;

	//	/** 重复同步时间 */
	//	private String importDurationPara;
	//
	//	/** 并发数量 */
	//	private String tableConcurrent;

	/** 全量导出数据时间 */
	private String fullExportTimeSpan;

	/** 去重月份 */
	private Integer selectPreMonth;

	/** 导出日期计划 */
	private Integer exportDateSpan;

	/** 导入map 个数 */
	private String importMapSize;

	/** 导出map 个数 */
	private String exportMapSize;

	/** 导出 OpenExpDirect */
	private String openExpDirect;
	/**
	 * 清洗（对hive参数的修改）
	 */
	private Map<String,String> hiveParam;

	/** 数据修复**/
	private Integer repairData;

	/** 历史表的 主键列表**/
	private Set<Integer> hisPrimaryKeys = new LinkedHashSet<Integer>();

	/**
	 * 导入导出（对sqoop参数的修改）
	 */
	private Map<String,String> sqoopParam;
	
	/**
	 * 同步/清洗类型（默认为 0）
	 * 0：orc清洗规则
	 * 1：parquet清洗规则
	 */
	private String syncType = "0";

	public String getSyncType() {
		return syncType;
	}

	public void setSyncType(String syncType) {
		this.syncType = syncType;
	}

	public Integer getSelectPreMonth() {
		return selectPreMonth;
	}

	public Integer getExportDateSpan() {
		return exportDateSpan;
	}

	public void setExportDateSpan(Integer exportDateSpan) {
		this.exportDateSpan = exportDateSpan;
	}

	public String getImportMapSize() {
		return importMapSize;
	}

	public void setImportMapSize(String importMapSize) {
		this.importMapSize = importMapSize;
	}

	public String getExportMapSize() {
		return exportMapSize;
	}

	public void setExportMapSize(String exportMapSize) {
		this.exportMapSize = exportMapSize;
	}

	public String getOpenExpDirect() {
		return openExpDirect;
	}

	public void setOpenExpDirect(String openExpDirect) {
		this.openExpDirect = openExpDirect;
	}

	public void setSelectPreMonth(Integer selectPreMonth) {
		this.selectPreMonth = selectPreMonth;
	}

	//	public String getTableConcurrent() {
	//		return tableConcurrent;
	//	}
	//
	//	public void setTableConcurrent(String tableConcurrent) {
	//		this.tableConcurrent = tableConcurrent;
	//	}
	//
	//	public String getImportDurationPara() {
	//		return importDurationPara;
	//	}
	//
	//	public void setImportDurationPara(String importDurationPara) {
	//		this.importDurationPara = importDurationPara;
	//	}
	public String getFullExportTimeSpan() {
		return fullExportTimeSpan;
	}

	public void setFullExportTimeSpan(String fullExportTimeSpan) {
		this.fullExportTimeSpan = fullExportTimeSpan;
	}

	public Integer getId() {
		return id;
	}

	public void setId(Integer id) {
		this.id = id;
	}

	public Integer getTaskType() {
		return taskType;
	}

	public void setTaskType(Integer taskType) {
		this.taskType = taskType;
	}

	public String getGroupName() {
		return groupName;
	}

	public void setGroupName(String groupName) {
		this.groupName = groupName;
	}

	public Integer getSyncFreqSeconds() {
		return syncFreqSeconds;
	}

	public void setSyncFreqSeconds(Integer syncFreqSeconds) {
		this.syncFreqSeconds = syncFreqSeconds;
	}

	public Integer getIsPhysicalDel() {
		return isPhysicalDel;
	}

	public void setIsPhysicalDel(Integer isPhysicalDel) {
		this.isPhysicalDel = isPhysicalDel;
	}

	public List<String> getSpecialColumnTypeList() {
		return specialColumnTypeList;
	}

	public void setSpecialColumnTypeList(List<String> specialColumnTypeList) {
		this.specialColumnTypeList = specialColumnTypeList;
	}

	public String getTriggerName() {
		return triggerName;
	}

	public void setTriggerName(String triggerName) {
		this.triggerName = triggerName;
	}

	public Integer getSourceDbId() {
		return sourceDbId;
	}

	public void setSourceDbId(Integer sourceDbId) {
		this.sourceDbId = sourceDbId;
	}

	public Integer getRepairData() {
		return repairData;
	}

	public void setRepairData(Integer repairData) {
		this.repairData = repairData;
	}

	// 获取历史表主键集合
	public Boolean hasPrimaryKey(Integer partition_date) { return hisPrimaryKeys.contains(partition_date);}
	// 设置历史表主键集合
	public void setHisPrimaryKeys(Integer hisPrimaryKey) {
		this.hisPrimaryKeys.add(hisPrimaryKey);
		Calendar calendar = Calendar.getInstance();
		calendar.add(Calendar.YEAR, - 1);
		Date current = calendar.getTime();
		String beforeOneYearStr = DateUtils.formatDatetime(current, "yyyyMMdd");
		Integer beforeOneYear = Integer.valueOf(beforeOneYearStr);
		List<Integer> delete = new LinkedList<Integer>();
		Iterator<Integer> it =  hisPrimaryKeys.iterator();
		while (it.hasNext()){
			Integer date = it.next();

			if(beforeOneYear.compareTo(date)  > 0 ){
				delete.add(date);
				logger.debug("beforeOneYear:"+ beforeOneYear+" > date :" + date+" and add to delete");
			}else {
				logger.debug("beforeOneYear:"+ beforeOneYear+" <= date :"+date + " not add to delete");
			}
		}
		logger.debug("begin at the end of size: "+ hisPrimaryKeys.size());
		for(Integer del : delete){
			logger.debug("组名："+getGroupName()+" 任务名："+getTargetTable()+" 删除大于一年的缓存：" + del);
			hisPrimaryKeys.remove(del);
		}
		logger.debug("End at the end of size: "+ hisPrimaryKeys.size());
		it =  hisPrimaryKeys.iterator();
		while (it.hasNext()){
			Integer date = it.next();
			logger.debug("======>"+date);
		}
	}

	public String getSourceParentTableId() {
		return sourceParentTableId;
	}

	public void setSourceParentTableId(String sourceParentTableId) {
		this.sourceParentTableId = sourceParentTableId;
	}

	public String getSourceTable() {
		return sourceTable;
	}
	public String getHisSourceTable() {
		return hisSourceTable;
	}
	public void setSourceTable(String sourceTable) {
		this.sourceTable = sourceTable;
	}

	public void sethisSourceTable(String hisSourceTable) {
		this.hisSourceTable = hisSourceTable;
	}

	public List<Integer> getDependencyTaskIds() {
		return dependencyTaskIds;
	}

	public void setDependencyTaskIds(List<Integer> dependencyTaskIds) {
		this.dependencyTaskIds = dependencyTaskIds;
	}

	public List<TaskPropertiesConfig> getDependencyTaskList() {
		return dependencyTaskList;
	}

	public void setDependencyTaskList(List<TaskPropertiesConfig> dependencyTaskList) {
		this.dependencyTaskList = dependencyTaskList;
	}

	public List<String> getPrimaryKeys() {
		return primaryKeys;
	}

	public String getPrimaryKeysStr() {
		StringBuffer sbBuffer = new StringBuffer();
		if (primaryKeys != null && primaryKeys.size() > 0) {
			for (String str : primaryKeys) {
				sbBuffer.append(str);
				sbBuffer.append(",");
			}
			sbBuffer = sbBuffer.deleteCharAt(sbBuffer.length() - 1);
		}
		return sbBuffer.toString();
	}

	public void setPrimaryKeys(List<String> primaryKeys) {
		this.primaryKeys = primaryKeys;
	}

	public List<String> getRelationColumns() {
		return relationColumns;
	}

	public void setRelationColumns(List<String> relationColumns) {
		this.relationColumns = relationColumns;
	}

	public List<String> getSelectColumns() {
		return selectColumns;
	}

	public List<String> getHisSelectColumns() {
		return hisSelectColumns;
	}

	public String getSelectColumnsStr() {
		StringBuffer sbBuffer = new StringBuffer();
		if (selectColumns != null && selectColumns.size() > 0) {
			for (String str : selectColumns) {
				sbBuffer.append(str);
				sbBuffer.append(",");
			}
			sbBuffer = sbBuffer.deleteCharAt(sbBuffer.length() - 1);
		}
		return sbBuffer.toString();
	}

	public String getHisSelectColumnsStr() {
		StringBuffer sbBuffer = new StringBuffer();
		if (hisSelectColumns != null && hisSelectColumns.size() > 0) {
			for (String str : hisSelectColumns) {
				sbBuffer.append(str);
				sbBuffer.append(",");
			}
			sbBuffer = sbBuffer.deleteCharAt(sbBuffer.length() - 1);
		}
		return sbBuffer.toString();
	}
	public void setSelectColumns(List<String> selectColumns) {
		this.selectColumns = selectColumns;
	}

	public void setHisSelectColumns(List<String> hisSelectColumns) {
		this.hisSelectColumns = hisSelectColumns;
	}


	public List<String> getTargetColumns() {
		return targetColumns;
	}

	public void setTargetColumns(List<String> targetColumns) {
		this.targetColumns = targetColumns;
	}

	public List<String> getSyncTimeColumn() {
		return syncTimeColumn;
	}

	public String getSyncTimeColumnStr() {
		StringBuffer sbBuffer = new StringBuffer();
		if (syncTimeColumn != null && syncTimeColumn.size() > 0) {
			/*for(String str:syncTimeColumn){
				sbBuffer.append(str);
				sbBuffer.append(",");
			}
			sbBuffer = sbBuffer.deleteCharAt(sbBuffer.length()-1);*/
			sbBuffer.append(syncTimeColumn.get(0));
		}
		return sbBuffer.toString();
	}

	public void setSyncTimeColumn(List<String> syncTimeColumn) {
		this.syncTimeColumn = syncTimeColumn;
	}

	public Integer getTargetDbId() {
		return targetDbId;
	}

	public void setTargetDbId(Integer targetDbId) {
		this.targetDbId = targetDbId;
	}

	public String getTargetTable() {
		return targetTable;
	}

	public void setTargetTable(String targetTable) {
		this.targetTable = targetTable;
	}

	public Integer getUseSqlFlag() {
		return useSqlFlag;
	}

	public void setUseSqlFlag(Integer useSqlFlag) {
		this.useSqlFlag = useSqlFlag;
	}

	public Integer getIsSlaveTable() {
		return isSlaveTable;
	}

	public void setIsSlaveTable(Integer isSlaveTable) {
		this.isSlaveTable = isSlaveTable;
	}

	public Integer getIsOverwrite() {
		return isOverwrite;
	}

	public void setIsOverwrite(Integer isOverwrite) {
		this.isOverwrite = isOverwrite;
	}

	public String getVersion() {
		return version;
	}

	public void setVersion(String version) {
		this.version = version;
	}

	public TaskDatabaseConfig getSourceDbEntity() {
		return sourceDbEntity;
	}

	public void setSourceDbEntity(TaskDatabaseConfig sourceDbEntity) {
		this.sourceDbEntity = sourceDbEntity;
	}

	public TaskDatabaseConfig getTargetDbEntity() {
		return targetDbEntity;
	}

	public void setTargetDbEntity(TaskDatabaseConfig targetDbEntity) {
		this.targetDbEntity = targetDbEntity;
	}

	public SortedMap<Integer, String> getTaskContent() {
		return taskContent;
	}

	public void setTaskContent(SortedMap<Integer, String> taskContent) {
		this.taskContent = taskContent;
	}

	public List<String> getFilterConditions() {
		return filterConditions;
	}
	
	public String getFilterConditionsStr(){
		StringBuffer sbBuffer = new StringBuffer();
		if (filterConditions != null && filterConditions.size() > 0) {
			for (String str : filterConditions) {
				if(str.contains("and")){
					sbBuffer.append(str);
				}else{
					sbBuffer.append(" and ");
					sbBuffer.append(str);
				}
			}
		}
		return sbBuffer.toString();
	}

	public void setFilterConditions(List<String> filterConditions) {
		this.filterConditions = filterConditions;
	}

	public Map<String, String> getHiveParam() {
		return hiveParam;
	}

	public void setHiveParam(Map<String, String> hiveParam) {
		this.hiveParam = hiveParam;
	}

	public Map<String, String> getSqoopParam() {
		return sqoopParam;
	}

	public void setSqoopParam(Map<String, String> sqoopParam) {
		this.sqoopParam = sqoopParam;
	}
	
}
