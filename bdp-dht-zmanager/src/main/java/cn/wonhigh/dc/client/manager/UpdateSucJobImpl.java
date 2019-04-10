package cn.wonhigh.dc.client.manager;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Resource;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.jmx.export.annotation.ManagedOperationParameter;
import org.springframework.jmx.export.annotation.ManagedOperationParameters;
import org.springframework.jmx.export.annotation.ManagedResource;
import org.springframework.stereotype.Service;

import cn.wonhigh.dc.client.common.model.ClientTaskStatusLog;
import cn.wonhigh.dc.client.common.util.CheckJobsUtils;
import cn.wonhigh.dc.client.common.util.DataValidationUtil;
import cn.wonhigh.dc.client.common.util.DateUtils;
import cn.wonhigh.dc.client.common.util.GernerateUuidUtils;
import cn.wonhigh.dc.client.common.util.ParseXMLFileUtil;
import cn.wonhigh.dc.client.common.util.PropertyFile;
import cn.wonhigh.dc.client.manager.cache.RinseStatusAndLogCache;
import cn.wonhigh.dc.client.manager.jms.SendMsg2AMQ;
import cn.wonhigh.dc.client.service.ClientTaskStatusLogService;
import cn.wonhigh.dc.client.service.EmailSenderService;
import cn.wonhigh.dc.client.service.jms.JmsClusterMgr;

import com.yougou.logistics.base.common.enums.JobBizStatusEnum;
import com.yougou.logistics.base.common.exception.ManagerException;
import com.yougou.logistics.base.common.interfaces.RemoteJobServiceExtWithParams;
import com.yougou.logistics.base.common.model.JobBizLog;
import com.yougou.logistics.base.common.vo.scheduler.RemoteJobInvokeParamsDto;

/**
 * 更新成功任务数
 * 
 * @author jiang.pl
 * 
 */
@Service
@ManagedResource(objectName = UpdateSucJobImpl.MBEAN_NAME, description = "更新成功任务数")
public class UpdateSucJobImpl implements RemoteJobServiceExtWithParams {

	private static final Logger logger = Logger.getLogger(UpdateSucJobImpl.class);
	
	public static final String MBEAN_NAME = "dc:client=UpdateSucJobImpl";

	/**
	 * 调度日志
	 */
	private static final List<JobBizLog> JOB_BIZ_LOG = new ArrayList<JobBizLog>();

	@Resource
	private ClientTaskStatusLogService clientTaskStatusLogService;
	@Resource
	private EmailSenderService emailSenderService;

	@Autowired
	private JmsClusterMgr jmsClusterMgr;

	@Override
	public void executeJobWithParams(String jobId, String triggerName, String groupName,
 	    RemoteJobInvokeParamsDto remoteDto) {
 	    String remoteParams = "jobId=" + jobId + ", triggerName=" + triggerName + ", groupName=" + groupName;
 		logger.info("远程方法被调用: " + MBEAN_NAME + " { " + remoteParams + " }");
 		JobBizStatusEnum jobBizStatusEnum = JobBizStatusEnum.INITIALIZING;
 		String message = "正在初始化";
 		logger.info(message);
         // 任务状态日志入库
 		addTaskLog(jobId, triggerName, groupName, jobBizStatusEnum, message);
 		// 发送MQ
 		 SendMsg2AMQ.updateStatusAndSend(jobId, jobBizStatusEnum, jmsClusterMgr);
 		// 返回调度中心日志
 		saveJobBizLog(" INFO " + message, triggerName, groupName, jobBizStatusEnum);
		/*
		 * 开始任务
		 */
		jobBizStatusEnum = JobBizStatusEnum.INITIALIZED;

		try {
			jobBizStatusEnum = JobBizStatusEnum.RUNNING;
			message = "初始化完成，正在运行";
			logger.info(message);
			// 任务状态日志入库
			addTaskLog(jobId, triggerName, groupName, jobBizStatusEnum, message);
			// 发送MQ
			SendMsg2AMQ.updateStatusAndSend(jobId, jobBizStatusEnum, jmsClusterMgr);
			// 返回调度中心日志
			saveJobBizLog(" INFO " + message, triggerName, groupName, jobBizStatusEnum);

			// 开始执行查询
			logger.info("开始跟新新的job");
		    //////////////////////////////////////////////////////////////////////////////////////////////
			new CheckJobsUtils().updateSuccJobs();
			jobBizStatusEnum = JobBizStatusEnum.FINISHED;
			message = "更新完成";
			logger.info(message);
			addTaskLog(jobId, triggerName, groupName, jobBizStatusEnum, message);
			// 发送MQ
			SendMsg2AMQ.updateStatusAndSend(jobId, jobBizStatusEnum, jmsClusterMgr);
			// 返回调度中心日志
			saveJobBizLog(" INFO " + message, triggerName, groupName, jobBizStatusEnum);
			//删除缓存
			RinseStatusAndLogCache.removeTaskByJobId(jobId);
		} catch (Exception e) {
			jobBizStatusEnum = JobBizStatusEnum.INTERRUPTED;
			message = "执行更新异常";
			logger.error(message ,e);
			// 任务状态日志入库
			addTaskLog(jobId, triggerName, groupName, jobBizStatusEnum, message);
			// 发送MQ
			SendMsg2AMQ.updateStatusAndSend(jobId, jobBizStatusEnum, jmsClusterMgr);
			// 返回调度中心日志
			saveJobBizLog(" ERROR " + message, triggerName, groupName, jobBizStatusEnum);
		}
	}

	/**
	 * 任务状态日志入库
	 * 
	 * @param taskId
	 * @param triggerName
	 * @param groupName
	 * @param taskDesc
	 */
	private void addTaskLog(String taskId, String triggerName, String groupName, JobBizStatusEnum jobBizStatus, String taskDesc, Date... endTime) {
		ClientTaskStatusLog clientTaskStatusLog = new ClientTaskStatusLog();
		clientTaskStatusLog.setTaskId(taskId);
		clientTaskStatusLog.setSchedulerName(triggerName);
		clientTaskStatusLog.setGroupName(groupName);
		clientTaskStatusLog.setTaskStatus(jobBizStatus.name());
		clientTaskStatusLog.setTaskStatusDesc(taskDesc);
		clientTaskStatusLog.setCreateTime(new Date());
		if (endTime.length > 0) {
			clientTaskStatusLog.setSyncEndTime(endTime[0]);
		}
		clientTaskStatusLogService.addClientTaskStatusLog(clientTaskStatusLog);
	}

	@Override
	public JobBizStatusEnum getJobStatus(String jobId, String triggerName, String groupName) {
		Map<String, Object> params = new HashMap<String, Object>();
		params.put("taskId", jobId);
		params.put("schedulerName", triggerName);
		params.put("groupName", groupName);
		ClientTaskStatusLog clientTaskStatusLog = clientTaskStatusLogService.findLastestStatus(params);
		JobBizStatusEnum latestStatus = null;
		if (clientTaskStatusLog != null) {
			for (JobBizStatusEnum status : JobBizStatusEnum.values()) {
				if (status.name().equals(clientTaskStatusLog.getTaskStatus())) {
					latestStatus = status;
				}
			}
		}
		return latestStatus;
	}

	@Override
	public String getLogs(String jobId, String triggerName, String groupName, long lastDate) {
		// TODO Auto-generated method stub
		return null;
	}

	private void saveJobBizLog(String errorMsg, String triggerName, String groupName, JobBizStatusEnum jobBizStatus) {
		JobBizLog jobBizLog = new JobBizLog();
		jobBizLog.setTriggerName(triggerName);
		jobBizLog.setGroupName(groupName);
		jobBizLog.setType(jobBizStatus.name());
		jobBizLog.setGmtDate(System.currentTimeMillis());
		jobBizLog.setRemark(DateUtils.formatDatetime() + errorMsg);
		JOB_BIZ_LOG.add(jobBizLog);
	}

	@Override
	public void initializeJob(String jobId, String triggerName, String groupName) {
		// TODO Auto-generated method stub

	}

	@Override
	public void pauseJob(String jobId, String triggerName, String groupName) {
		// TODO Auto-generated method stub

	}

	@Override
	public void restartJob(String jobId, String triggerName, String groupName) {
		// TODO Auto-generated method stub

	}

	@Override
	public void resumeJob(String jobId, String triggerName, String groupName) {
		// TODO Auto-generated method stub

	}

	@Override
	public void stopJob(String jobId, String triggerName, String groupName) {
		// TODO Auto-generated method stub

	}

	public void setClientTaskStatusLogService(ClientTaskStatusLogService clientTaskStatusLogService) {
		this.clientTaskStatusLogService = clientTaskStatusLogService;
	}



	public EmailSenderService getEmailSenderService() {
		return emailSenderService;
	}

	public void setEmailSenderService(EmailSenderService emailSenderService) {
		this.emailSenderService = emailSenderService;
	}

	@ManagedOperation(description = "job simulator")
	@ManagedOperationParameters({ @ManagedOperationParameter(description = "trigger name", name = "triggerName"),
			@ManagedOperationParameter(description = "group name", name = "groupName") })
	public void simulateJob(String triggerName, String groupName) {
		logger.info("Test the job trigger from simulator.");
		executeJobWithParams(GernerateUuidUtils.getUUID(), triggerName, groupName, null);
	}
   public static void main(String args[]) throws SQLException, ManagerException{
	   try {
			ParseXMLFileUtil.initTask();
		} catch (ManagerException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
      try {
		new CheckJobsUtils().updateSuccJobs();
	} catch (SQLException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
   }
}
