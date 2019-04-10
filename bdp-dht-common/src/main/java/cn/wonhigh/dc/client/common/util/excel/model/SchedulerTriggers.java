package cn.wonhigh.dc.client.common.util.excel.model;

import cn.wonhigh.dc.client.common.util.excel.Excel;
import cn.wonhigh.dc.client.common.util.excel.ExcelCell;

@Excel
public class SchedulerTriggers extends SchedulerTriggersKey {
    /**
     * 分组名
     */
    @ExcelCell(value = "分组名") 
    private String triggerGroup;    
    
	/**
	 * 触发器名
	 */
	@ExcelCell(value = "触发器名")
    private String triggerName;
	
    /**
     * 任务编号   
     */
    @ExcelCell(value = "任务编号")
    private String jobNumber;	
    
    /**
     * Cron表达式
     */
    @ExcelCell(value = "Cron表达式")
    private String cronException; 
    
    /**
     * Mbean地址
     */
    @ExcelCell(value = "Mbean地址")
    private String mbeanurl;    
    
    /**
     * URL
     */
    @ExcelCell(value = "URL")
    private String url;    
    
    /**
     * 超时
     */
    @ExcelCell(value = "超时")
    private String timeout;        
    
    /**
     * 心跳超时时间
     */
    @ExcelCell(value = "心跳超时时间")
    private int checkInTimeOut;    
    
    /**
     * 任务失败重试次数  
     */
    @ExcelCell(value = "重试次数")
    private int tryTime;       
    
    /**
     * 是否可并行
     */
    @ExcelCell(value = "是否可并行")
    private int isCanParallel;    
    
    /**
     * 任务类型
     */
    @ExcelCell(value = "任务类型")
    private int jobType;
    
    /**
     * 备注
     */
    @ExcelCell(value = "备注")
    private String remark;
    
    /**
     * 调度区间
     */
    @ExcelCell(value = "调度区间")
    private String range;

	public String getTriggerGroup() {
		return triggerGroup;
	}

	public void setTriggerGroup(String triggerGroup) {
		this.triggerGroup = triggerGroup;
	}

	public String getTriggerName() {
		return triggerName;
	}

	public void setTriggerName(String triggerName) {
		this.triggerName = triggerName;
	}

	public String getJobNumber() {
		return jobNumber;
	}

	public void setJobNumber(String jobNumber) {
		this.jobNumber = jobNumber;
	}

	public String getCronException() {
		return cronException;
	}

	public void setCronException(String cronException) {
		this.cronException = cronException;
	}

	public String getMbeanurl() {
		return mbeanurl;
	}

	public void setMbeanurl(String mbeanurl) {
		this.mbeanurl = mbeanurl;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getTimeout() {
		return timeout;
	}

	public void setTimeout(String timeout) {
		this.timeout = timeout;
	}

	public int getCheckInTimeOut() {
		return checkInTimeOut;
	}

	public void setCheckInTimeOut(int checkInTimeOut) {
		this.checkInTimeOut = checkInTimeOut;
	}

	public int getTryTime() {
		return tryTime;
	}

	public void setTryTime(int tryTime) {
		this.tryTime = tryTime;
	}

	public int getIsCanParallel() {
		return isCanParallel;
	}

	public void setIsCanParallel(int isCanParallel) {
		this.isCanParallel = isCanParallel;
	}

	public int getJobType() {
		return jobType;
	}

	public void setJobType(int jobType) {
		this.jobType = jobType;
	}

	public String getRemark() {
		return remark;
	}

	public void setRemark(String remark) {
		this.remark = remark;
	}

	public String getRange() {
		return range;
	}

	public void setRange(String range) {
		this.range = range;
	}
    
}