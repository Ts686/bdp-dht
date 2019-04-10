package cn.wonhigh.dc.client.common.model;

import java.io.Serializable;

/**
 * 存储过程任务属性配置表 
 * 
 * @author  jiang.pl
 * 
 */
public class TaskProPropertiesConfig implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7618748015852121407L;

	/** 任务主键id */
	private Integer id;

	/** 任务类型 100.sqoop-导入 200.hive--导出  300  存储过程*/
	private Integer taskType;

	/** 组名 */
	private String groupName;

	/** 调度任务名称 */
	private String triggerName;

	/** 源数据库id */
	private Integer sourceDbId;

	/** 源数据库配置信息 */
	private TaskDatabaseConfig sourceDbEntity;

	 
	/** 存储过程名称*/
	private String prcedurename;
   

	/** 执行频率 默认值：3600秒 */
	private Integer syncFreqSeconds = 3600;
 
    private Integer  isreturn=1;
    
    private String  returntype="int";

	/** 是否物理删除 0.逻辑删除 1.物理删除 */
	private Integer isPhysicalDel = 0;

	/** 版本号 */
	private String version;

 

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

	public String getPrcedurename() {
		return prcedurename;
	}

	public void setPrcedurename(String prcedurename) {
		this.prcedurename = prcedurename;
	}

	public Integer getIsreturn() {
		return isreturn;
	}

	public void setIsreturn(Integer isreturn) {
		this.isreturn = isreturn;
	}

	public String getReturntype() {
		return returntype;
	}

	public void setReturntype(String returntype) {
		this.returntype = returntype;
	}

	 

}
