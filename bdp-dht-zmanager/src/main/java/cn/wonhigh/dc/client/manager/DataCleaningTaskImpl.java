package cn.wonhigh.dc.client.manager;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.annotation.Resource;

import cn.wonhigh.dc.client.common.util.ExceptionUtil;
import cn.wonhigh.dc.client.service.ApplicationInfoService;
import cn.wonhigh.dc.client.service.ResumeHiveTaskInfoService;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.jmx.export.annotation.ManagedOperationParameter;
import org.springframework.jmx.export.annotation.ManagedOperationParameters;
import org.springframework.jmx.export.annotation.ManagedResource;
import org.springframework.stereotype.Service;

import cn.wonhigh.dc.client.common.constans.CommonEnumCollection;
import cn.wonhigh.dc.client.common.constans.CommonEnumCollection.HiveDefinePartNameEnum;
import cn.wonhigh.dc.client.common.constans.CommonEnumCollection.ParamNameEnum;
import cn.wonhigh.dc.client.common.constans.CommonEnumCollection.SyncTypeEnum;
import cn.wonhigh.dc.client.common.model.ClientTaskStatusLog;
import cn.wonhigh.dc.client.common.model.PhysicDelRecord;
import cn.wonhigh.dc.client.common.model.TaskPropertiesConfig;
import cn.wonhigh.dc.client.common.util.GernerateUuidUtils;
import cn.wonhigh.dc.client.common.util.HiveUtils;
import cn.wonhigh.dc.client.manager.cache.RinseStatusAndLogCache;
import cn.wonhigh.dc.client.manager.jms.SendMsg2AMQ;
import cn.wonhigh.dc.client.service.ClientTaskStatusLogService;
import cn.wonhigh.dc.client.service.PhysicDelRecordService;
import cn.wonhigh.dc.client.service.jms.JmsClusterMgr;

import com.yougou.logistics.base.common.enums.JobBizStatusEnum;
import com.yougou.logistics.base.common.exception.ManagerException;
import com.yougou.logistics.base.common.interfaces.RemoteJobServiceExtWithParams;
import com.yougou.logistics.base.common.vo.scheduler.RemoteJobInvokeParamsDto;

/**
 * 数据清洗程序
 *
 * @author wang.w
 * @version 1.0.0
 * @date 2016-4-13 下午12:10:50
 * @copyright wonhigh.cn
 */
@Service
@ManagedResource(objectName = DataCleaningTaskImpl.MBEAN_NAME, description = "Hive数据清洗过程")
public class DataCleaningTaskImpl implements RemoteJobServiceExtWithParams {

    ThreadPoolExecutor pools = new ThreadPoolExecutor(
            60, 90, 60,
            TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(2000), new RejectedExecutionHandler() {
        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {

        }
    });

    private static final Logger logger = Logger.getLogger(DataCleaningTaskImpl.class);

    public static final String MBEAN_NAME = "dc:client=DataCleaningTaskImpl";
    @Autowired
    private ResumeHiveTaskInfoService resumeHiveTaskInfoService;
    @Autowired
    private ApplicationInfoService applicationInfoService;
    @Value("${dc.date.format.default}")
    private String dateFormat = "yyyy-MM-dd HH:mm:ss";

    @Value("${dc.hadoop.clnd.params}")
    private String hadoopClndParams = "";
    @Resource
    private ClientTaskStatusLogService clientTaskStatusLogService;

    @Resource
    private PhysicDelRecordService physicDelRecordService;

    @Autowired
    private JmsClusterMgr jmsClusterMgr;

    @Value("${jdbc.hive.timeout}")
    private String jdbcTimeout = "30";

    @Value("${clnd.pre.month.num}")
    private String clndPreMonthNum = "12";

    @Value("${clnd.cast.thl.open}")
    private String ifcast = "true";

    @SuppressWarnings("unused")
    private List<Object> checkParamValue(RemoteJobInvokeParamsDto remoteJobInvokeParamsDto, String taskId)
            throws ManagerException {
        List<Object> returnList = new ArrayList<Object>();
        if (remoteJobInvokeParamsDto != null) {
            String startTimeStr = remoteJobInvokeParamsDto.getParam(CommonEnumCollection.ParamNameEnum.START_TIME
                    .getValue());
            returnList.add(startTimeStr);
            String endTimeStr = remoteJobInvokeParamsDto.getParam(CommonEnumCollection.ParamNameEnum.END_TIME
                    .getValue());
            returnList.add(endTimeStr);
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            if (startTimeStr != null && endTimeStr != null) {
                logger.info(String.format("【subInstanceId为：%s】的任务被调用,开始时间:%s ;结束时间:%s .", taskId, startTimeStr, endTimeStr));
                try {
                    Date syncBeginTime = sdf.parse(startTimeStr);
                    Date syncEndTime = sdf.parse(endTimeStr);
                    returnList.add(syncBeginTime);
                    returnList.add(syncEndTime);
                } catch (ParseException e) {
                    RuntimeException runtimeException = new RuntimeException(String.format("【subInstanceId为：%s】的任务被调用,开始时间:%s ;结束时间:%s 转换出现异常", taskId,
                            startTimeStr, endTimeStr));
                    JobBizStatusEnum jobBizStatusEnum = JobBizStatusEnum.STOPED;
                    SendMsg2AMQ.updateStatusAndSendMsg(taskId, jobBizStatusEnum, jmsClusterMgr,
                            ExceptionUtil.getStackTrace(runtimeException));
                    throw runtimeException;
                }
            } else {
                ManagerException managerException = new ManagerException(String.format("【subInstanceId为：%s】的任务被调用,传入的开始和结束时间为空.", taskId));
                JobBizStatusEnum jobBizStatusEnum = JobBizStatusEnum.STOPED;
                SendMsg2AMQ.updateStatusAndSendMsg(taskId, jobBizStatusEnum, jmsClusterMgr,
                        ExceptionUtil.getStackTrace(managerException));
                throw managerException;
            }

        } else {
            ManagerException managerException = new ManagerException(String.format("【subInstanceId为：%s】的任务被调用,传入的参数为空.", taskId));
            JobBizStatusEnum jobBizStatusEnum = JobBizStatusEnum.STOPED;
            SendMsg2AMQ.updateStatusAndSendMsg(taskId, jobBizStatusEnum, jmsClusterMgr,
                    ExceptionUtil.getStackTrace(managerException));
            throw managerException;
        }
        return returnList;
    }

    @Override
    public void executeJobWithParams(String subInstanceId, String triggerName, String groupName,
                                     RemoteJobInvokeParamsDto remoteJobInvokeParamsDto) {
        DataCleaningTaskImplThread dataLoadingTaskImplThread = new DataCleaningTaskImplThread(subInstanceId, triggerName, groupName, remoteJobInvokeParamsDto);
        logger.info(String.format("数据清洗任务开始执行，线程池信息:当前线程池中线程数量【%d】,核心线程数【%d】,活跃线程数【%d】,缓存到队列的任务数量【%d】"
                , pools.getPoolSize(), pools.getCorePoolSize(), pools.getActiveCount(), pools.getQueue().size()));
        pools.submit(dataLoadingTaskImplThread);
        logger.info(String.format("数据清洗提交线程任务--->线程池信息:当前线程池中线程数量【%d】,核心线程数【%d】,活跃线程数【%d】,缓存到队列的任务数量【%d】"
                , pools.getPoolSize(), pools.getCorePoolSize(), pools.getActiveCount(), pools.getQueue().size()));
        logger.info("DataCleaningTaskImplThread started...");
    }

    /**
     * 增量表清洗，去重
     *
     * @param jobName
     * @param taskConfig
     * @param subInstanceId
     * @param syncBeginTime
     * @param syncEndTime
     * @throws Throwable
     */
    private boolean increamentCleaningData(String jobName, TaskPropertiesConfig taskConfig, String subInstanceId, Date syncBeginTime,
                                           Date syncEndTime) throws Throwable {
        String groupName = taskConfig.getGroupName();
        String taskName = taskConfig.getTriggerName();
        String message = "初始化完成，正在运行";
        PhysicDelRecord record = new PhysicDelRecord();
        Map<String, Object> paramsMap = new HashMap<String, Object>();
        String sGroupName = taskConfig.getGroupName();
        String sSourceTable = taskConfig.getTriggerName().replace(
                HiveDefinePartNameEnum.CLEANED_TABLE_NAME_SUBFIX.getValue(), "");
        paramsMap.put(ParamNameEnum.SYS_NAME.getValue(), sGroupName);
        paramsMap.put(ParamNameEnum.TABLE_NAME.getValue(), sSourceTable);

        message = "开始！查询表【t_physic_del_record】";
        logger.info(String.format("***********>%s：【groupName：%s】【triggerName：%s】", message, groupName, taskName));
        List<PhysicDelRecord> recordlist = physicDelRecordService.findByBiz(record, paramsMap);

        logger.info(String.format("结束！查询表 t_physic_del_record】符合条件的记录个数为【%d 】：【groupName：%s】" + "【triggerName：%s】",
                recordlist.size(), groupName, taskName));

        // 调用Hive工具类去重

        Long seqNo = 0L;
        Long seqId = 0L;
        if (recordlist.size() > 0) {
            seqNo = recordlist.get(0).getSeqNo() > 0 ? recordlist.get(0).getSeqNo() : 0L;
            seqId = recordlist.get(0).getId();
        }

        logger.info(String.format("开始调用去重工具,配置的去重周期是:【 %s 】, 从t_physic_del_record表中，"
                + "从t_physic_del_record中获得seqNo的值【 %d 】！", taskConfig.getSelectPreMonth(), seqNo));

        Long maxSeqNo = HiveUtils.cleaningData(jobName,
                taskConfig,
                hadoopClndParams,
                Integer.valueOf(jdbcTimeout),
                seqNo,
                new Timestamp(syncBeginTime.getTime()),
                new Timestamp(syncEndTime.getTime()),
                taskConfig.getSelectPreMonth() != null ? taskConfig.getSelectPreMonth() : Integer
                        .parseInt(clndPreMonthNum), ifcast);


        //将sequence的max值记录到mysql表
        record.setId(seqId);
        record.setSeqNo(maxSeqNo);
        Date updateTime = new Date();
        record.setUpdateTime(updateTime);
        physicDelRecordService.modifyById(record);

        message = "数据去重完成!已经更新表t_physic_del_record!";
        logger.info(String.format("***********>%s 【id：%d maxSeqNo：%d updateTime:%s】【groupName：%s】【triggerName：%s】",
                message, seqId, maxSeqNo, updateTime, groupName, taskName));
        return true;
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
        clientTaskStatusLog.setSchedulerName(triggerName);
        clientTaskStatusLog.setGroupName(groupName);
        clientTaskStatusLog.setTaskStatus(jobBizStatus.name());
        clientTaskStatusLog.setTaskStatusDesc(taskDesc);
        clientTaskStatusLog.setCreateTime(new Date());
        if (endTime.length == 1) {
            clientTaskStatusLog.setSyncEndTime(endTime[0]);
        } else if (endTime.length == 2) {
            clientTaskStatusLog.setSyncBeginTime(endTime[0]);
            clientTaskStatusLog.setSyncEndTime(endTime[1]);
        }
        clientTaskStatusLogService.addClientTaskStatusLog(clientTaskStatusLog);
    }

    @Override
    public JobBizStatusEnum getJobStatus(String subInstanceId, String triggerName, String groupName) {
        Map<String, Object> params = new HashMap<String, Object>();
        params.put("taskId", subInstanceId);
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
    public String getLogs(String subInstanceId, String triggerName, String groupName, long lastDate) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void initializeJob(String subInstanceId, String triggerName, String groupName) {
        // TODO Auto-generated method stub

    }

    @Override
    public void pauseJob(String subInstanceId, String triggerName, String groupName) {
        // TODO Auto-generated method stub

    }

    @Override
    public void restartJob(String subInstanceId, String triggerName, String groupName) {
        // TODO Auto-generated method stub

    }

    @Override
    public void resumeJob(String subInstanceId, String triggerName, String groupName) {
        // TODO Auto-generated method stub

    }

    @Override
    public void stopJob(String subInstanceId, String triggerName, String groupName) {
        // TODO Auto-generated method stub

    }

    public void setClientTaskStatusLogService(ClientTaskStatusLogService clientTaskStatusLogService) {
        this.clientTaskStatusLogService = clientTaskStatusLogService;
    }

    @ManagedOperation(description = "job simulator")
    @ManagedOperationParameters({@ManagedOperationParameter(description = "trigger name", name = "triggerName"),
            @ManagedOperationParameter(description = "group name", name = "groupName")})
    public void simulateJob(String triggerName, String groupName) {
        logger.info("Test the job trigger from simulator.");
        RemoteJobInvokeParamsDto rD = new RemoteJobInvokeParamsDto();
        rD.addParam("startTime", "2018-06-28 00:00:00");
        rD.addParam("endTime", "2018-06-29 00:00:00");
        executeJobWithParams(GernerateUuidUtils.getUUID(), triggerName, groupName, rD);
    }

    private void saveAppInfo(RemoteJobInvokeParamsDto remoteJobInvokeParamsDto, String jobName, String subInstanceId) {
        String instanceId = remoteJobInvokeParamsDto.getParam("instanceId");
        String jobId = remoteJobInvokeParamsDto.getParam("jobId");
        Map<String, String> res = new HashMap<String, String>();
        res.put("scheduleId", jobId);
        res.put("taskParentId", instanceId);
        res.put("taskSonId", subInstanceId);
        res.put("mapred.job.name", jobName);
        try {
            applicationInfoService.insertOrUpdateApplicationInfo(res);
        } catch (Exception e) {
            String msg = "保存app信息异常...";
            logger.error(msg, e);
            return;
        }
    }

    class DataCleaningTaskImplThread implements Runnable {

        private final Logger logger = Logger.getLogger(DataCleaningTaskImplThread.class);
        private String subInstanceId;
        private String triggerName;
        private String groupName;
        private RemoteJobInvokeParamsDto remoteJobInvokeParamsDto;

        public DataCleaningTaskImplThread(String subInstanceId, String triggerName, String groupName,
                                          RemoteJobInvokeParamsDto remoteJobInvokeParamsDto) {
            this.subInstanceId = subInstanceId;
            this.triggerName = triggerName;
            this.groupName = groupName;
            this.remoteJobInvokeParamsDto = remoteJobInvokeParamsDto;
        }

        @Override
        public void run() {
            //保存任务信息
            try {
                boolean insertJobInfo = CommUtil.saveJobInfo(resumeHiveTaskInfoService, subInstanceId, triggerName, groupName,
                        remoteJobInvokeParamsDto, DataCleaningTaskImpl.class);
            } catch (Exception e) {
                String message = "保存任务信息失败";
                logger.error(message, e);
            }
            //如果重复调用，则忽略本次请求
            JobBizStatusEnum jobStauts = RinseStatusAndLogCache.getTaskStatusByJobId(subInstanceId);
            if (null != jobStauts && !jobStauts.name().equals(JobBizStatusEnum.INTERRUPTED.name())) {
                logger.info(String.format("【subInstanceId为：%s】的任务被重复调用", subInstanceId));
                return;
            }
            String startTimeStr = null;
            String endTimeStr = null;
            //获取并验证开始结束时间
            Date syncEndTime = null;
            Date syncBeginTime = null;
            String message = null;
            JobBizStatusEnum jobBizStatusEnum;

            //taskName = taskName.trim();

            jobStauts = RinseStatusAndLogCache.getTaskStatusByJobId(subInstanceId);
            if (jobStauts != null && !jobStauts.name().equals(JobBizStatusEnum.INTERRUPTED.name())) {
                logger.info(String.format("【subInstanceId为：%s】的任务被重复调用", subInstanceId));
                return;
            }
            if (remoteJobInvokeParamsDto != null) {

                startTimeStr = remoteJobInvokeParamsDto.getParam("startTime");
                endTimeStr = remoteJobInvokeParamsDto.getParam("endTime");

                SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
                if (startTimeStr != null && endTimeStr != null) {
                    logger.info(String.format("【subInstanceId为：%s】的任务被调用,", subInstanceId) + "开始时间:" + startTimeStr + ";" + "结束时间:"
                            + endTimeStr + ".");
                    try {
                        syncBeginTime = sdf.parse(startTimeStr);
                        syncEndTime = sdf.parse(endTimeStr);
                    } catch (ParseException e) {
                        jobBizStatusEnum = JobBizStatusEnum.STOPED;
                        SendMsg2AMQ.updateStatusAndSendMsg(subInstanceId, jobBizStatusEnum,
                                jmsClusterMgr, ExceptionUtil.getStackTrace(e));
                        logger.error(e.getMessage(), e);
                        return;
                    }

                } else {
                    message = String.format("【subInstanceId为：%s】的任务被调用,", subInstanceId) + "传入的开始和结束时间为空.";
                    logger.error(message);
                    jobBizStatusEnum = JobBizStatusEnum.INTERRUPTED;
                    // 任务状态日志入库
                    addTaskLog(subInstanceId, triggerName, groupName, jobBizStatusEnum, message);
                    SendMsg2AMQ.updateStatusAndSendMsg(subInstanceId, jobBizStatusEnum, jmsClusterMgr, message);
                    RinseStatusAndLogCache.removeTaskByJobId(subInstanceId);
                    return;
                }

            } else {
                message = String.format("【subInstanceId为：%s】的任务被调用,", subInstanceId) + "传入的参数为空.";
                logger.error(message);
                jobBizStatusEnum = JobBizStatusEnum.INTERRUPTED;
                // 任务状态日志入库
                addTaskLog(subInstanceId, triggerName, groupName, jobBizStatusEnum, message);
                SendMsg2AMQ.updateStatusAndSendMsg(subInstanceId, jobBizStatusEnum, jmsClusterMgr, message);
                RinseStatusAndLogCache.removeTaskByJobId(subInstanceId);
                return;
            }
            try {
                jobBizStatusEnum = JobBizStatusEnum.INITIALIZING;
                message = "正在初始化";
                logger.info(String.format("%s: 【subInstanceId：%s】【groupName：%s】【triggerName：%s】", message, subInstanceId, groupName,
                        triggerName));
                // 任务状态日志入库
                addTaskLog(subInstanceId, triggerName, groupName, jobBizStatusEnum, message);
                // 发送MQ
                SendMsg2AMQ.updateStatusAndSendMsg(subInstanceId, jobBizStatusEnum, jmsClusterMgr, message);

			/*
             * 初始化变量
			 */
                message = "开始读取任务配置信息";
                logger.info(String.format("%s: 【subInstanceId：%s】【groupName：%s】【triggerName：%s】", message, subInstanceId, groupName,
                        triggerName));

                TaskPropertiesConfig taskConfig = CommUtil.checkTaskExecCondition(groupName, triggerName, subInstanceId);
                if (taskConfig == null) {
                    jobBizStatusEnum = JobBizStatusEnum.INTERRUPTED;
                    message = "获取任务配置信息失败";
                    logger.error(String.format("%s：【groupName：%s】【triggerName：%s】", message, groupName, triggerName));
                    // 任务状态日志入库
                    addTaskLog(subInstanceId, triggerName, groupName, jobBizStatusEnum, message);
                    // 发送MQ
                    SendMsg2AMQ.updateStatusAndSendMsg(subInstanceId, jobBizStatusEnum, jmsClusterMgr, message);
                    RinseStatusAndLogCache.removeTaskByJobId(subInstanceId);
                    return;
                }

                //增量清洗
                message = "[**************进入增量表清洗****************]";
                logger.info(String.format("%s: 【subInstanceId：%s】【groupName：%s】【triggerName：%s】", message, subInstanceId, groupName,
                        triggerName));
                jobBizStatusEnum = JobBizStatusEnum.RUNNING;
                message = "初始化完成，正在运行";
                logger.info(String.format("***********>%s：【groupName：%s】【triggerName：%s】", message, groupName, taskConfig.getTriggerName()));
                // 任务状态日志入库
                addTaskLog(subInstanceId, taskConfig.getTriggerName(), groupName, jobBizStatusEnum, message);
                // 发送MQ
                SendMsg2AMQ.updateStatusAndSendMsg(subInstanceId, jobBizStatusEnum, jmsClusterMgr, message);
                String jobName = String.format("%s-%s-%s_%s", "hive", taskConfig.getGroupName(),
                        taskConfig.getTriggerName(), System.currentTimeMillis());
                logger.info(String.format("!!!数据清洗任务开始执行,groupName【%s】,taskName【%s】",
                        groupName, triggerName));
                saveAppInfo(remoteJobInvokeParamsDto, jobName, subInstanceId);
                if (taskConfig.getIsOverwrite() == 0) {

                    if (taskConfig.getSyncType() != null
                            && taskConfig.getSyncType().equals(SyncTypeEnum.SYNC_TYPE_1.getValue())) {
                        HiveUtils.removeDuplicate(jobName,
                                taskConfig,
                                hadoopClndParams,
                                Integer.valueOf(jdbcTimeout),
                                taskConfig.getSelectPreMonth() == null ? Integer.valueOf(clndPreMonthNum) : taskConfig
                                        .getSelectPreMonth(), new Date[]{syncBeginTime, syncEndTime});
                    } else if (taskConfig.getSyncType() != null
                            && taskConfig.getSyncType().equals(SyncTypeEnum.SYNC_TYPE_2.getValue())) {
                        HiveUtils.removeDuplicate3(jobName,
                                taskConfig,
                                hadoopClndParams,
                                Integer.valueOf(jdbcTimeout),
                                taskConfig.getSelectPreMonth() == null ? Integer.valueOf(clndPreMonthNum) : taskConfig
                                        .getSelectPreMonth(), new Date[]{syncBeginTime, syncEndTime});
                    } else if (taskConfig.getSyncType() != null
                            && taskConfig.getSyncType().equals(SyncTypeEnum.SYNC_TYPE_0.getValue())) {
                        increamentCleaningData(jobName, taskConfig, subInstanceId, syncBeginTime, syncEndTime);
                    }

                } else {
                    message = "[**************进入全量表清洗****************]";
                    logger.info(String.format("%s: 【subInstanceId：%s】【groupName：%s】【triggerName：%s】", message, subInstanceId, groupName,
                            triggerName));
                    HiveUtils.fullTableCleaningDate(remoteJobInvokeParamsDto, subInstanceId, taskConfig,
                            hadoopClndParams, 30, jobName);
                }
                logger.info(String.format("数据清洗任务执行完成!!!,groupName【%s】,taskName【%s】", groupName, triggerName));

                jobBizStatusEnum = JobBizStatusEnum.FINISHED;
                message = "数据去重成功";
                logger.info(String.format("%s: 【subInstanceId：%s】【groupName：%s】【triggerName：%s】", message, subInstanceId, groupName,
                        triggerName));
                // 任务状态日志入库
                addTaskLog(subInstanceId, triggerName, groupName, jobBizStatusEnum, message);
                // 发送MQ
                SendMsg2AMQ.updateStatusAndSendMsg(subInstanceId, jobBizStatusEnum, jmsClusterMgr, message);
                try {
                    CommUtil.delJobInfo(resumeHiveTaskInfoService, subInstanceId);
                } catch (Exception e) {
                    logger.error("删除任务信息失败", e);
                }
            } catch (Throwable e) {
                jobBizStatusEnum = JobBizStatusEnum.INTERRUPTED;
                message = "数据去重失败: { " + e.getMessage() + " }";
                logger.error(message + ": " + e.getMessage());
                // 任务状态日志入库
                addTaskLog(subInstanceId, triggerName, groupName, jobBizStatusEnum, message);
                // 发送MQ
                SendMsg2AMQ.updateStatusAndSendMsg(subInstanceId, jobBizStatusEnum,
                        jmsClusterMgr, message + "\n" + ExceptionUtil.getStackTrace(e));
            }
        }
    }
}
