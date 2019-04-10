package cn.wonhigh.dc.client.service;

import java.util.Date;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import cn.wonhigh.dc.client.common.model.TaskLog;
import cn.wonhigh.dc.client.dal.mapper.TaskLogMapper;

import com.yougou.logistics.base.common.enums.JobBizStatusEnum;

@Service("taskLogService")
public class TaskLogServiceImpl implements TaskLogService {

	@Autowired
	private TaskLogMapper taskLogMapper;

	@Override
	public List<TaskLog> findTaskLog(String triggerName, String groupName,
			Date createTime) {
		return taskLogMapper.selectByCreateTime(triggerName, groupName,
				createTime);
	}

	@Override
	public JobBizStatusEnum getTaskStatus(String jobId, String triggerName,
			String groupName) {
		Integer taskStatus = taskLogMapper.getTaskStatus(jobId, triggerName,
				groupName);
		JobBizStatusEnum bizStatus = JobBizStatusEnum.INITIALIZING;
		if (taskStatus == 1) {
			bizStatus = JobBizStatusEnum.RUNNING;
		} else if (taskStatus == 2) {
			bizStatus = JobBizStatusEnum.PAUSED;
		} else if (taskStatus == 6) {
			bizStatus = JobBizStatusEnum.FINISHED;
		} else if (taskStatus == 9) {
			bizStatus = JobBizStatusEnum.INTERRUPTED;
		}
		return bizStatus;
	}

}
