package cn.wonhigh.dc.client.manager;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.annotation.Resource;

import cn.wonhigh.dc.client.common.model.SqoopParams;
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
import cn.wonhigh.dc.client.common.constans.CommonEnumCollection.CDCTableColumnEnum;
import cn.wonhigh.dc.client.common.constans.CommonEnumCollection.HiveDefinePartNameEnum;
import cn.wonhigh.dc.client.common.model.ClientTaskStatusLog;
import cn.wonhigh.dc.client.common.model.TaskPropertiesConfig;
import cn.wonhigh.dc.client.common.util.GernerateUuidUtils;
import cn.wonhigh.dc.client.common.util.HiveUtils;
import cn.wonhigh.dc.client.manager.cache.RinseStatusAndLogCache;
import cn.wonhigh.dc.client.manager.jms.SendMsg2AMQ;
import cn.wonhigh.dc.client.service.ClientTaskStatusLogService;
import cn.wonhigh.dc.client.service.jms.JmsClusterMgr;

import com.yougou.logistics.base.common.enums.JobBizStatusEnum;
import com.yougou.logistics.base.common.interfaces.RemoteJobServiceExtWithParams;
import com.yougou.logistics.base.common.vo.scheduler.RemoteJobInvokeParamsDto;

/**
 * 导入任务服务
 *
 * @author wang.w
 */
@Service("cdcDataMoveTaskImpl")
@ManagedResource(objectName = CDCDataMoveTaskImpl.MBEAN_NAME, description = "cdc数据转移任务")
public class CDCDataMoveTaskImpl implements RemoteJobServiceExtWithParams {

    ThreadPoolExecutor pools = new ThreadPoolExecutor(
            15, 30, 60,
            TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(500), new RejectedExecutionHandler() {
        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {

        }
    });

    String columnSchema = null;

    @Autowired
    private ResumeHiveTaskInfoService resumeHiveTaskInfoService;

    @Autowired
    private ApplicationInfoService applicationInfoService;
    public static final String MBEAN_NAME = "dc:client=CDCDataMoveTaskImpl";

    private static final Logger logger = Logger.getLogger(CDCDataMoveTaskImpl.class);

    @Value("${dc.date.format.default}")
    private String dateFormat = "yyyy-MM-dd HH:mm:ss";

    @Value("${dc.hadoop.thl.params}")
    private String hadoopThlParams = "";

    @Resource
    private ClientTaskStatusLogService clientTaskStatusLogService;

    @Autowired
    private JmsClusterMgr jmsClusterMgr;

    public void setclientTaskStatusLogService(ClientTaskStatusLogService clientTaskStatusLogService) {
        this.clientTaskStatusLogService = clientTaskStatusLogService;
    }

    @Override
    public void initializeJob(String jobId, String triggerName, String groupName) {

    }

    @Override
    public void pauseJob(String jobId, String triggerName, String groupName) {

    }

    @Override
    public void resumeJob(String jobId, String triggerName, String groupName) {

    }

    @Override
    public void stopJob(String jobId, String triggerName, String groupName) {

    }

    @Override
    public void restartJob(String jobId, String triggerName, String groupName) {

    }

    @Override
    public JobBizStatusEnum getJobStatus(String taskId, String taskName, String groupName) {
        Map<String, Object> params = new HashMap<String, Object>();
        params.put(CommonEnumCollection.ParamNameEnum.TASK_ID.getValue(), taskId);
        params.put(CommonEnumCollection.ParamNameEnum.TASK_NAME.getValue(), taskName);
        params.put(CommonEnumCollection.ParamNameEnum.GROUP_NAME.getValue(), groupName);
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

    /**
     * 暂时无用
     */
    @Override
    public String getLogs(String jobId, String triggerName, String groupName, long lastDate) {
        return null;
    }

    /**
     * 调度中心导入jmx方法调用
     */
    @Override
    public void executeJobWithParams(String jobId, String taskName, String groupName,
                                     RemoteJobInvokeParamsDto remoteJobInvokeParamsDto) {
        CDCDataMoveTaskImplThread dataLoadingTaskImplThread = new CDCDataMoveTaskImplThread(jobId, taskName, groupName, remoteJobInvokeParamsDto);
        pools.submit(dataLoadingTaskImplThread);
        logger.info("CDCDataMoveTaskImplThread started...");
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

    //根据结束时间创建分区字段
    private int getHivePartitionValue(Date syncEndTime) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        Date hivePartitionTime = new Date(syncEndTime.getTime() - 1000);
        String hivePartitionValueStr = sdf.format(hivePartitionTime);
        int hivePartitionValue = Integer.parseInt(hivePartitionValueStr);
        return hivePartitionValue;

    }

    @ManagedOperation(description = "job simulator")
    @ManagedOperationParameters({@ManagedOperationParameter(description = "trigger name", name = "triggerName"),
            @ManagedOperationParameter(description = "group name", name = "groupName")})
    public void simulateJob(String triggerName, String groupName) {
        logger.info("Test the job trigger from simulator.");
        RemoteJobInvokeParamsDto rD = new RemoteJobInvokeParamsDto();
        rD.addParam("startTime", "2015-08-20 00:00:00");
        rD.addParam("endTime", "2015-08-21 00:00:00");
        executeJobWithParams(GernerateUuidUtils.getUUID(), triggerName, groupName, rD);
    }

    private void saveAppInfo(RemoteJobInvokeParamsDto remoteJobInvokeParamsDto, String jobName, String jobId) {
        String instanceId = remoteJobInvokeParamsDto.getParam("instanceId");
        String subInstanceId = remoteJobInvokeParamsDto.getParam("subInstanceId");
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

    class CDCDataMoveTaskImplThread implements Runnable {
        private final Logger logger = Logger.getLogger(CDCDataMoveTaskImplThread.class);
        private String jobId;
        private String taskName;
        private String groupName;
        private RemoteJobInvokeParamsDto remoteJobInvokeParamsDto;

        public CDCDataMoveTaskImplThread(String jobId, String taskName, String groupName,
                                         RemoteJobInvokeParamsDto remoteJobInvokeParamsDto) {
            this.jobId = jobId;
            this.taskName = taskName;
            this.groupName = groupName;
            this.remoteJobInvokeParamsDto = remoteJobInvokeParamsDto;
        }

        @Override
        public void run() {
            //保存任务信息
            try {
                CommUtil.saveJobInfo(resumeHiveTaskInfoService, jobId, taskName, groupName,
                        remoteJobInvokeParamsDto, CDCDataMoveTaskImpl.class);
            } catch (Exception e) {
                String message = "保存任务信息失败";
                logger.error(message, e);
            }
            String startTimeStr = null;
            String endTimeStr = null;

//		Date syncEndTime = null;
            Date syncBeginTime = null;

            JobBizStatusEnum jobBizStatusEnum = JobBizStatusEnum.INTERRUPTED;

            //如果重复调用，则忽略本次请求
            jobId = jobId.trim();
            JobBizStatusEnum jobStauts = RinseStatusAndLogCache.getTaskStatusByJobId(jobId);
            if (jobStauts != null && !jobStauts.name().equals(JobBizStatusEnum.INTERRUPTED.name())) {
                logger.info(String.format("【jobId为：%s】的任务被重复调用", jobId));
                return;
            }
            if (remoteJobInvokeParamsDto != null) {

                startTimeStr = remoteJobInvokeParamsDto.getParam("startTime");
                endTimeStr = remoteJobInvokeParamsDto.getParam("endTime");

                SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
                if (startTimeStr != null && endTimeStr != null) {
                    logger.info(String.format("【jobId为：%s】的任务被调用,", jobId) + "开始时间:" + startTimeStr + ";" + "结束时间:"
                            + endTimeStr + ".");
                    try {
                        syncBeginTime = sdf.parse(startTimeStr);
//					syncEndTime = sdf.parse(endTimeStr);
                    } catch (ParseException e) {
                        jobBizStatusEnum = JobBizStatusEnum.STOPED;
                        SendMsg2AMQ.updateStatusAndSendMsg(jobId, jobBizStatusEnum,
                                jmsClusterMgr, ExceptionUtil.getStackTrace(e));
                        logger.error(e.getMessage(), e);
                        return;
                    }
                } else {
                    String message = String.format("【jobId为：%s】的任务被调用,", jobId) + "传入的开始和结束时间为空.";
                    logger.error(message);
                    jobBizStatusEnum = JobBizStatusEnum.INTERRUPTED;
                    // 任务状态日志入库
                    addTaskLog(jobId, taskName, groupName, jobBizStatusEnum, message);
                    SendMsg2AMQ.updateStatusAndSendMsg(jobId, jobBizStatusEnum, jmsClusterMgr, message);
                    RinseStatusAndLogCache.removeTaskByJobId(jobId);
                    return;
                }

            } else {
                String message = String.format("【jobId为：%s】的任务被调用,", jobId) + "传入的参数为空.";
                logger.error(message);
                jobBizStatusEnum = JobBizStatusEnum.INTERRUPTED;
                // 任务状态日志入库
                addTaskLog(jobId, taskName, groupName, jobBizStatusEnum, message);
                SendMsg2AMQ.updateStatusAndSendMsg(jobId, jobBizStatusEnum, jmsClusterMgr, message);
                RinseStatusAndLogCache.removeTaskByJobId(jobId);
                return;
            }
            try {
                taskName = taskName.trim();

//                int partitionValue = getHivePartitionValue(syncBeginTime);

                // 0.更新初始化状态
                jobBizStatusEnum = JobBizStatusEnum.INITIALIZING;
                String message = "初始化中。。。。。。";
                addTaskLog(jobId, taskName, groupName, jobBizStatusEnum, message);
                logger.info(message);
                SendMsg2AMQ.updateStatusAndSend(jobId, jobBizStatusEnum, jmsClusterMgr);

                // 1.加载任务(因为此源表只有新增操作，所以暂时先不对每个分区进行truncate后，再insert操作)
                message = "开始读取任务配置信息,";
                logger.info(String.format("***********>%s：【groupName：%s】【triggerName：%s】", message, groupName, taskName));

                TaskPropertiesConfig taskConfig = CommUtil.checkTaskExecCondition(groupName, taskName, jobId);
                if (taskConfig == null) {
                    jobBizStatusEnum = JobBizStatusEnum.INTERRUPTED;
                    message = "获取任务配置信息失败";
                    logger.error(String.format("%s：【groupName：%s】【triggerName：%s】", message, groupName, taskName));
                    // 任务状态日志入库
                    addTaskLog(jobId, taskName, groupName, jobBizStatusEnum, message);
                    // 发送MQ
                    SendMsg2AMQ.updateStatusAndSendMsg(jobId, jobBizStatusEnum, jmsClusterMgr, message);
                    RinseStatusAndLogCache.removeTaskByJobId(jobId);
                    return;
                }

//                String sourceTable = groupName + "_" + HiveDefinePartNameEnum.CDC_TABLE_SUBFIX.getValue();
//                String selectColumns = taskConfig.getSelectColumnsStr();
                //生成增全量任务对应的sql
                String executeSQL = generateThlSQL(taskConfig,
                        syncBeginTime, startTimeStr, endTimeStr);
                jobBizStatusEnum = JobBizStatusEnum.RUNNING;
                addTaskLog(jobId, taskName, groupName, jobBizStatusEnum, "运行中");
                SendMsg2AMQ.updateStatusAndSend(jobId, jobBizStatusEnum, jmsClusterMgr);

                String msgCdc = String.format("开始执行【groupName：%s】【triggerName：%s】导入到THL对应的表：%s",
                        groupName, taskName, executeSQL);
                logger.info(msgCdc);
                String jobName = String.format("%s-%s-%s_%s", "hive", taskConfig.getGroupName(),
                        taskConfig.getTriggerName(), System.currentTimeMillis());
                saveAppInfo(remoteJobInvokeParamsDto, jobName, jobId);
                HiveUtils.execUpdate(taskConfig.getTargetDbEntity(), taskConfig, hadoopThlParams,
                        executeSQL, 30, jobName);
                //同步元数据到Impala Catalog
                HiveUtils.syncMetaData4Impala(executeSQL
                        , taskConfig.getTargetDbEntity().getDbName(),
                        taskConfig.getTargetTable());
//            HiveUtils.execUpdate(taskConfig.getTargetDbEntity(), taskConfig,
//                    hadoopThlParams, sbBuilder.toString(), 30, remoteJobInvokeParamsDto, jobId);
                //HiveUtils.execUpdate(taskConfig, sbBuilder.toString(), 30);

                msgCdc = String.format("执行完成:导入到THL对应的表,【groupName：%s】【triggerName：%s】", groupName, taskName);
                logger.info(msgCdc);

                jobBizStatusEnum = JobBizStatusEnum.FINISHED;
                addTaskLog(jobId, taskName, groupName, jobBizStatusEnum, "执行完成...");
                SendMsg2AMQ.updateStatusAndSend(jobId, jobBizStatusEnum, jmsClusterMgr);
                try {
                    CommUtil.delJobInfo(resumeHiveTaskInfoService, jobId);
                } catch (Exception e) {
                    logger.error("删除任务信息失败", e);
                }
            } catch (Exception e) {
                // 异常处理
                jobBizStatusEnum = JobBizStatusEnum.INTERRUPTED;
                String errMsg = String.format("【groupName：%s】【triggerName：%s】导入出现异常：%s", groupName, taskName,
                        e.getMessage());
                logger.error(errMsg, e);
                addTaskLog(jobId, taskName, groupName, jobBizStatusEnum,
                        "执行sqoop命令:" + "中断:" + errMsg.substring(0, Math.min(errMsg.length(), 800)));
                RinseStatusAndLogCache.removeTaskByJobId(jobId);
                SendMsg2AMQ.updateStatusAndSendMsg(jobId, jobBizStatusEnum,
                        jmsClusterMgr, errMsg + "\n" + ExceptionUtil.getStackTrace(e));
                return;
            }
        }

        private String generateThlSQL(TaskPropertiesConfig taskConfig,
                                      Date syncBeginTime, String startTimeStr, String endTimeStr) {
            int partitionValue = getHivePartitionValue(syncBeginTime);
            String sourceTable = groupName + "_" + HiveDefinePartNameEnum.CDC_TABLE_SUBFIX.getValue();
            String selectColumns = taskConfig.getSelectColumnsStr();
            StringBuilder sbBuilder = new StringBuilder();
            sbBuilder.append(" insert overwrite table ");
            sbBuilder.append(taskConfig.getTargetTable());
            sbBuilder.append(" partition(");
            sbBuilder.append(HiveDefinePartNameEnum.PARTITION_DATE_NAME.getValue());
            sbBuilder.append(") select ");
            sbBuilder.append(selectColumns);
            sbBuilder.append(" from ");
            sbBuilder.append(sourceTable);
            sbBuilder.append(" src_t where ");
            if (taskConfig.getIsOverwrite() == 0) {
                sbBuilder.append(HiveDefinePartNameEnum.PARTITION_DATE_NAME.getValue());
                sbBuilder.append("  >= '");
                sbBuilder.append(partitionValue);
                sbBuilder.append("' and src_t.");
                sbBuilder.append(taskConfig.getSyncTimeColumnStr());
                sbBuilder.append(" >= '");
                sbBuilder.append(startTimeStr);
                sbBuilder.append("' and src_t.");
                sbBuilder.append(taskConfig.getSyncTimeColumnStr());
                sbBuilder.append(" < '");
                sbBuilder.append(endTimeStr);
                sbBuilder.append("' and ");
            }
            sbBuilder.append(CDCTableColumnEnum.CDC_COLUMN_DB_NAME.getValue());
            sbBuilder.append(" ='");
            sbBuilder.append(groupName);
            sbBuilder.append("' and ");
            sbBuilder.append(CDCTableColumnEnum.CDC_COLUMN_TABLE_NAME.getValue());
            sbBuilder.append(" = '");
//            if (groupName.equalsIgnoreCase("gyl_wms_city")) {
            sbBuilder.append(taskName.replaceAll("_yg_thl", "")
                    .replaceAll("_thl", ""));
//            } else {
//                sbBuilder.append(taskName.substring(0,
//                        taskName.indexOf(HiveDefinePartNameEnum.THL_TABLE_NAME_SUBFIX.getValue())));
//            }
            sbBuilder.append("'");
            return sbBuilder.toString();
        }
    }
}
