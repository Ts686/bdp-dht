package cn.wonhigh.dc.client.common.model;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

/**
 * sql任务属性配置
 * 
 * @author xiao.py
 * 
 */
public class TaskExeSQLPropertiesConfig implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 9095690954002620181L;

	/** 任务主键id */
	private Integer id;
	
	/** 组名 */
	private String groupName;

	/** 调度任务名称 */
	private String triggerName;

	/** 执行sql数据库id */
	private Integer sourceDbId;
	
	/**
	 * 是否创建临时udf，1是创建，0是不创建
	 */
	private Integer isAddTemporaryUdf;
	
	/**
	 * 临时udf名
	 */
	private String temporaryUdfName;
	
	/**
	 * 临时udf jar存放路径
	 */
	private String temporaryUdfPath;
	
	/**
	 * 临时udf 类的全路径
	 */
	private String temporaryUdfClass;
	
	
	/** 源数据库配置信息 */
	private TaskDatabaseConfig sourceDbEntity;

	/** 依赖的关系id集合 */
	private List<Integer> dependencyTaskIds;
	

	/** 依赖的执行sql集合 */
	private List<TaskExeSQLPropertiesConfig> dependencyTaskList;
	
	/**
	 * 调度时间
	 */
	private String schedulTimeparameter;

	/**
	 * 需执行的sql
	 */
	private String executeSql;
	
	/**
	 * 是否下载表数据到本地，1是下载，0不下载
	 */
	//private Integer isDownload;

	/**
	 * 表名
	 */
	//private String DownloadTableName;
	
	/**
	 * 需要下载到本地的路径
	 */
	//private String downloadPath;
	
	/** 执行频率 默认值：3600秒 */
	private Integer syncFreqSeconds = 3600;
	
	/** 版本号 */
	private String version;
	
	
	

	/** 目标数据库id */
	private Integer targetDbId;

	/** 目标据库配置信息 */
	private TaskDatabaseConfig targetDbEntity;

	/** 目标数据表 */
	private String targetTable;

	/** 任务内容 可能存在多个子任务 */
	private SortedMap<Integer, String> taskContent;

	/** 全量导出数据时间 */
	private String fullExportTimeSpan;

	/**
	 * 清洗（对hive参数的修改）
	 */
	private Map<String,String> hiveParam;
	
	
	
	public String getSchedulTimeparameter() {
		return schedulTimeparameter;
	}

	public void setSchedulTimeparameter(String schedulTimeparameter) {
		this.schedulTimeparameter = schedulTimeparameter;
	}

	public String getTemporaryUdfClass() {
		return temporaryUdfClass;
	}

	public void setTemporaryUdfClass(String temporaryUdfClass) {
		this.temporaryUdfClass = temporaryUdfClass;
	}

	public String getTemporaryUdfPath() {
		return temporaryUdfPath;
	}

	public void setTemporaryUdfPath(String temporaryUdfPath) {
		this.temporaryUdfPath = temporaryUdfPath;
	}

	public String getVersion() {
		return version;
	}

	public void setVersion(String version) {
		this.version = version;
	}

	public Integer getIsAddTemporaryUdf() {
		return isAddTemporaryUdf;
	}

	public void setIsAddTemporaryUdf(Integer isAddTemporaryUdf) {
		this.isAddTemporaryUdf = isAddTemporaryUdf;
	}

	public String getTemporaryUdfName() {
		return temporaryUdfName;
	}

	public void setTemporaryUdfName(String temporaryUdfName) {
		this.temporaryUdfName = temporaryUdfName;
	}

	public String getExecuteSql() {
		return executeSql;
	}

	public void setExecuteSql(String executeSql) {
		this.executeSql = executeSql;
	}

	/*public Integer getIsDownload() {
		return isDownload;
	}

	public void setIsDownload(Integer isDownload) {
		this.isDownload = isDownload;
	}

	public String getDownloadTableName() {
		return DownloadTableName;
	}

	public void setDownloadTableName(String downloadTableName) {
		DownloadTableName = downloadTableName;
	}

	public String getDownloadPath() {
		return downloadPath;
	}

	public void setDownloadPath(String downloadPath) {
		this.downloadPath = downloadPath;
	}*/

	public static long getSerialversionuid() {
		return serialVersionUID;
	}

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

	

	public List<Integer> getDependencyTaskIds() {
		return dependencyTaskIds;
	}

	public void setDependencyTaskIds(List<Integer> dependencyTaskIds) {
		this.dependencyTaskIds = dependencyTaskIds;
	}

	public List<TaskExeSQLPropertiesConfig> getDependencyTaskList() {
		return dependencyTaskList;
	}

	public void setDependencyTaskList(List<TaskExeSQLPropertiesConfig> dependencyTaskList) {
		this.dependencyTaskList = dependencyTaskList;
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
	
	public Map<String, String> getHiveParam() {
		return hiveParam;
	}

	public void setHiveParam(Map<String, String> hiveParam) {
		this.hiveParam = hiveParam;
	}
	
}
