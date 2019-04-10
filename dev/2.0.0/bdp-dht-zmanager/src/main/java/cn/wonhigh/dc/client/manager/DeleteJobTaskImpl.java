package cn.wonhigh.dc.client.manager;

import cn.wonhigh.dc.client.common.model.ClientTaskStatusLog;
import cn.wonhigh.dc.client.common.util.ExceptionUtil;
import cn.wonhigh.dc.client.common.util.GernerateUuidUtils;
import cn.wonhigh.dc.client.manager.cache.RinseStatusAndLogCache;
import cn.wonhigh.dc.client.manager.jms.SendMsg2AMQ;
import cn.wonhigh.dc.client.service.ApplicationInfoService;
import cn.wonhigh.dc.client.service.ClientTaskStatusLogService;
import cn.wonhigh.dc.client.service.jms.JmsClusterMgr;
import com.yougou.logistics.base.common.enums.JobBizStatusEnum;
import com.yougou.logistics.base.common.interfaces.RemoteJobServiceExtWithParams;
import com.yougou.logistics.base.common.vo.scheduler.RemoteJobInvokeParamsDto;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.impl.YarnClientImpl;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.jmx.export.annotation.ManagedOperationParameter;
import org.springframework.jmx.export.annotation.ManagedOperationParameters;
import org.springframework.jmx.export.annotation.ManagedResource;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Service
@ManagedResource(objectName = DeleteJobTaskImpl.MBEAN_NAME, description = "导入任务")
public class DeleteJobTaskImpl implements RemoteJobServiceExtWithParams {
    ThreadPoolExecutor pools = new ThreadPoolExecutor(
            6, 20, 60,
            TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(1000), new RejectedExecutionHandler() {
        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {

        }
    });
    public static final String MBEAN_NAME = "dc:client=DeleteJobTaskImpl";

    @Resource
    private ClientTaskStatusLogService clientTaskStatusLogService;

    @Autowired
    private JmsClusterMgr jmsClusterMgr;

    private static final Logger logger = Logger.getLogger(DeleteJobTaskImpl.class);
    @Resource
    private ApplicationInfoService applicationInfoService;

    @ManagedOperation(description = "job simulator")
    @ManagedOperationParameters({
            @ManagedOperationParameter(description = "trigger name", name = "triggerName"),
            @ManagedOperationParameter(description = "group name", name = "groupName"),
            @ManagedOperationParameter(description = "jobId", name = "jobId"),
    })
    public void simulateJob(String triggerName, String groupName, String jobId) {
        logger.info("Test the job trigger from simulator.");
        RemoteJobInvokeParamsDto rD = new RemoteJobInvokeParamsDto();
        executeJobWithParams(jobId, triggerName, groupName, rD);
    }

//    @Override
//    public void executeJobWithParams(String jobId, String taskName, String groupName,
//                                     RemoteJobInvokeParamsDto remoteJobInvokeParamsDto) {
//        String instanceId = remoteJobInvokeParamsDto.getParam("instanceId");
//        try {
//            List<String> killAppIds = applicationInfoService.selectByInstandId(Arrays.asList("222", "333"));
//            RemoteShellExecutor executor = new RemoteShellExecutor("10.9.251.34", "hive", "hive");
//            // 执行myTest.sh 参数为java Know dummy
//            for (String appId : killAppIds) {
//                logger.info(executor.exec("/var/lib/hadoop-hdfs/killJob.sh " + appId));
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }

    //    @Override
    public void executeJobWithParams(String jobId, String taskName, String groupName,
                                     RemoteJobInvokeParamsDto remoteJobInvokeParamsDto) {
        DeleteJobTaskImplThread dataLoadingTaskImplThread = new DeleteJobTaskImplThread(jobId, taskName, groupName);
        pools.submit(dataLoadingTaskImplThread);
        logger.info("DeleteJobTaskImplThread started...");
    }

    @Override
    public void initializeJob(String s, String s1, String s2) {

    }

    @Override
    public void pauseJob(String s, String s1, String s2) {

    }

    @Override
    public void resumeJob(String s, String s1, String s2) {

    }

    @Override
    public void stopJob(String s, String s1, String s2) {

    }

    @Override
    public void restartJob(String s, String s1, String s2) {

    }

    @Override
    public JobBizStatusEnum getJobStatus(String s, String s1, String s2) {
        return null;
    }

    @Override
    public String getLogs(String s, String s1, String s2, long l) {
        return null;
    }

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

    class DeleteJobTaskImplThread implements Runnable {
        private final Logger logger = Logger.getLogger(DeleteJobTaskImplThread.class);
        private String jobId;
        private String taskName;
        private String groupName;

        public DeleteJobTaskImplThread(String jobId, String taskName, String groupName) {
            this.jobId = jobId;
            this.taskName = taskName;
            this.groupName = groupName;
        }

        @Override
        public void run() {
            JobBizStatusEnum jobBizStatusEnum = JobBizStatusEnum.INITIALIZING;
            String message = String.format("撤销正在运行的任务  【groupName：%s】【triggerName：%s】", groupName, taskName);
            logger.info(message);
            addTaskLog(jobId, taskName, groupName, jobBizStatusEnum, message);
            SendMsg2AMQ.updateStatusAndSendMsg(jobId, jobBizStatusEnum, jmsClusterMgr, message);
            try {
                logger.info("Yarn client start initialize");
                YarnClient client = new YarnClientImpl();
                client.init(new Configuration());
                client.start();
                logger.info("Yarn client initialized");
                //需要注意，调度传过来的jobId实际为子实例id，所以后端需要使用subInstanceId进行过滤
                List<String> killAppIds = applicationInfoService.selectByInstandId(
                        Arrays.asList(jobId.split(",")));
                // 执行myTest.sh 参数为java Know dummy
                if (!killAppIds.isEmpty()) {
                    for (String appId : killAppIds) {
                        logger.info("正在撤销的jobId=【" + jobId + "】,appId=【" + appId + "】");
                        if (StringUtils.isNotEmpty(appId)) {
                            String[] split = appId.split("_");
                            ApplicationId id = ApplicationId.newInstance(Long.parseLong(split[1]), Integer.parseInt(split[2]));
                            client.killApplication(id);
                            logger.info("撤销任务成功,jobId=【" + jobId + "】" + "appId=【" + appId + "】");
                        } else {
                            logger.info("撤销任务失败，正在撤销的任务暂未提交到YARN");
                        }
                    }
                }

                jobBizStatusEnum = JobBizStatusEnum.FINISHED;
                addTaskLog(jobId, taskName, groupName, jobBizStatusEnum, message);
                SendMsg2AMQ.updateStatusAndSendMsg(jobId, jobBizStatusEnum, jmsClusterMgr, message);
                RinseStatusAndLogCache.removeTaskByJobId(jobId);
                logger.info("DeleteJobTaskImplThread succcessful...");
            } catch (Exception e) {
                message = String.format("撤销任务失败【groupName：%s】【triggerName：%s】", groupName, taskName);
                logger.error(message);
                jobBizStatusEnum = JobBizStatusEnum.STOPED;
                addTaskLog(jobId, taskName, groupName, jobBizStatusEnum, message);
                SendMsg2AMQ.updateStatusAndSendMsg(jobId, jobBizStatusEnum, jmsClusterMgr, message);
                RinseStatusAndLogCache.removeTaskByJobId(jobId);
                logger.error(ExceptionUtil.getStackTrace(e), e);
            }
        }
    }
}
