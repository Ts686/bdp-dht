package cn.wonhigh.dc.client.manager.task;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.annotation.Resource;

import org.apache.log4j.Logger;

import com.yougou.logistics.base.common.enums.JobBizStatusEnum;

import cn.wonhigh.dc.client.common.model.ClientTaskStatusLog;
import cn.wonhigh.dc.client.common.model.DelDataHandler;
import cn.wonhigh.dc.client.common.util.DbUtils;
import cn.wonhigh.dc.client.common.util.HiveUtils;
import cn.wonhigh.dc.client.service.ClientTaskStatusLogService;

/**
 * TODO: 增加描述
 * 
 * @author wangl
 * @date 2015年12月19日 下午6:32:44
 * @version 0.9.9
 * @copyright wonhigh.cn 
 */
public class DelDataHandleThreadTask extends Thread {
	private static final Logger logger = Logger.getLogger(DelDataHandleThreadTask.class);
	private static List<Thread> runningThreads = new ArrayList<Thread>();
	
	private DelDataHandler delDataHandler;
	private ClientTaskStatusLogService clientTaskStatusLogService;
	private String handleId;

	public DelDataHandleThreadTask(DelDataHandler delDataHandler, ClientTaskStatusLogService clientTaskStatusLogService, String handleId) {
		this.delDataHandler=delDataHandler;
		this.clientTaskStatusLogService=clientTaskStatusLogService;
		this.handleId=handleId;
	}

	@Override
	public void run() {
		regist(this);// 线程开始时注册
		// 打印开始标记
		String groupName="dc_common";
		JobBizStatusEnum jobBizStatusEnumInt = JobBizStatusEnum.INTERRUPTED;
		logger.info(Thread.currentThread().getName() + "开始...");
		logger.info("执行【"+delDataHandler.getTaskConfig().getTargetTable()+"】修复开始,首先清除hive中删除的数据-----");
		try {
			HiveUtils.delDataHandler(delDataHandler);
		} catch (Exception e) {
			String messageInt = "删除修复任务运行失败:"+e.getMessage().substring(0, Math.min(200, e.getMessage().length()));
			logger.error(messageInt);
			unRegist(this);// 线程结束时取消注册
			addTaskLog(handleId, delDataHandler.getTaskConfig().getTargetTable(),groupName,  jobBizStatusEnumInt, messageInt);
			logger.info("退出当前线程");
			throw new RuntimeException(e.getMessage()); 
		}
		logger.info("执行hive中删除数据修复结束，开始执行pg中删除数据修复");
		try {
			DbUtils.delDataHandler(delDataHandler);
		} catch (Exception e) {
			String messageInt = "删除修复任务运行失败:"+e.getMessage().substring(0, Math.min(200, e.getMessage().length()));
			unRegist(this);// 线程结束时取消注册
			addTaskLog(handleId, delDataHandler.getTaskConfig().getTargetTable(),groupName, jobBizStatusEnumInt, messageInt);
			logger.info("退出当前线程");
			throw new RuntimeException(e.getMessage()); 	
		}
		logger.info("执行pg中删除数据修复结束，开始更新删除数据表已处理的数据状态");
		try {
			DbUtils.updateProcessStatus(delDataHandler);
		} catch (Exception e) {
			String messageInt = "删除修复任务运行失败:"+e.getMessage().substring(0, Math.min(200, e.getMessage().length()));
			unRegist(this);// 线程结束时取消注册
			addTaskLog(handleId, delDataHandler.getTaskConfig().getTargetTable(),groupName, jobBizStatusEnumInt, messageInt);
			logger.info("退出当前线程");
			throw new RuntimeException(e.getMessage()); 
		}
		logger.info("更新删除数据表已处理的数据状态结束");
		// 任务状态日志入库
		JobBizStatusEnum jobBizStatusEnumFin = JobBizStatusEnum.FINISHED;
		String messageInt = "删除修复任务运行成功:";
		addTaskLog(handleId, delDataHandler.getTaskConfig().getTargetTable(),groupName, jobBizStatusEnumFin, messageInt,delDataHandler.getNotProcessMinTime(),delDataHandler.getNotProcessMaxTime());
		unRegist(this);// 线程结束时取消注册
		// 打印结束标记
		logger.info(Thread.currentThread().getName() + "结束.");
	}

	public void regist(Thread t) {
		synchronized (runningThreads) {
			runningThreads.add(t);
		}
	}

	public void unRegist(Thread t) {
		synchronized (runningThreads) {
			runningThreads.remove(t);
		}
	}

	public static boolean hasThreadRunning() {
		// 通过判断runningThreads是否为空就能知道是否还有线程未执行完
		return (runningThreads.size() > 0);
	}
	
	/**
	 * 任务状态日志入库
	 * 
	 * @param taskId
	 * @param triggerName
	 * @param groupName
	 * @param taskDesc
	 */
	private void addTaskLog(String taskId, String triggerName, String groupName, JobBizStatusEnum jobBizStatus,
			String taskDesc, Date... endTime) {
		ClientTaskStatusLog clientTaskStatusLog = new ClientTaskStatusLog();
		clientTaskStatusLog.setTaskId(taskId);
		clientTaskStatusLog.setSchedulerName(triggerName+"_del_repair");
		clientTaskStatusLog.setGroupName(groupName);
		clientTaskStatusLog.setTaskStatus(jobBizStatus.name());
		clientTaskStatusLog.setTaskStatusDesc(taskDesc);
		clientTaskStatusLog.setCreateTime(new Date());
		if (endTime.length > 0) {
			clientTaskStatusLog.setSyncEndTime(endTime[0]);
		}
		clientTaskStatusLogService.addClientTaskStatusLog(clientTaskStatusLog);
	}

}
