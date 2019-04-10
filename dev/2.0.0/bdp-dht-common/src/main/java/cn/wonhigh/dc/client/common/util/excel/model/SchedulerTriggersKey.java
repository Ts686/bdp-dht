package cn.wonhigh.dc.client.common.util.excel.model;

import cn.wonhigh.dc.client.common.util.excel.Excel;
import cn.wonhigh.dc.client.common.util.excel.ExcelCell;

@Excel
public class SchedulerTriggersKey {
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
	
	@ExcelCell(value = "任务")
	private String task;
	
	@ExcelCell(value = "依赖任务")
	private String parentTask;	

    public String getTriggerName() {
        return triggerName;
    }

    public void setTriggerName(String triggerName) {
        this.triggerName = triggerName;
    }

    public String getTriggerGroup() {
        return triggerGroup;
    }

    public void setTriggerGroup(String triggerGroup) {
        this.triggerGroup = triggerGroup;
    }

	public String getTask() {
		return task;
	}

	public void setTask(String task) {
		this.task = task;
	}

	public String getParentTask() {
		return parentTask;
	}

	public void setParentTask(String parentTask) {
		this.parentTask = parentTask;
	}
}