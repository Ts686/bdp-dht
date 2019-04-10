package cn.wonhigh.dc.client.service;

import java.util.Date;
import java.util.List;

import cn.wonhigh.dc.client.common.model.TaskLog;

import com.yougou.logistics.base.common.enums.JobBizStatusEnum;

public interface TaskLogService {

	public List<TaskLog> findTaskLog(String triggerName, String groupName,
			Date createTime);

	public JobBizStatusEnum getTaskStatus(String jobId, String triggerName,
			String groupName);

}
