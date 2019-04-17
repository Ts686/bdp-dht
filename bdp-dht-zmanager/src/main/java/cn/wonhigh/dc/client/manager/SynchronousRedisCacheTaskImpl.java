package cn.wonhigh.dc.client.manager;

import cn.wonhigh.dc.client.common.model.ClientTaskStatusLog;
import cn.wonhigh.dc.client.common.util.GernerateUuidUtils;
import cn.wonhigh.dc.client.common.util.ParseXMLFileUtil;
import cn.wonhigh.dc.client.manager.cache.RinseStatusAndLogCache;
import cn.wonhigh.dc.client.manager.jms.SendMsg2AMQ;
import cn.wonhigh.dc.client.service.ClientTaskStatusLogService;
import cn.wonhigh.dc.client.service.jms.JmsClusterMgr;
import com.yougou.logistics.base.common.enums.JobBizStatusEnum;
import com.yougou.logistics.base.common.exception.ManagerException;
import com.yougou.logistics.base.common.interfaces.RemoteJobServiceExtWithParams;
import com.yougou.logistics.base.common.vo.scheduler.RemoteJobInvokeParamsDto;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.jmx.export.annotation.ManagedOperationParameter;
import org.springframework.jmx.export.annotation.ManagedOperationParameters;
import org.springframework.jmx.export.annotation.ManagedResource;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


@Service
@ManagedResource(objectName = SynchronousRedisCacheTaskImpl.MBEAN_NAME, description = "同步缓存任务")
public class SynchronousRedisCacheTaskImpl implements RemoteJobServiceExtWithParams {

    ThreadPoolExecutor pools = new ThreadPoolExecutor(
            6, 20, 60,
            TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(1000), new RejectedExecutionHandler() {
        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {

        }
    });

    public static final String MBEAN_NAME = "dc:client=SynchronousRedisCacheTaskImpl";

    private static final Logger logger = Logger.getLogger(SynchronousRedisCacheTaskImpl.class);

    @Resource
    private ClientTaskStatusLogService clientTaskStatusLogService;

    @Autowired
    private JmsClusterMgr jmsClusterMgr;

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

    @Override
    public void executeJobWithParams(String jobId, String taskName, String groupName,
                                     RemoteJobInvokeParamsDto remoteJobInvokeParamsDto) {
        SynchronousRedisCacheTaskImplThread dataLoadingTaskImplThread = new SynchronousRedisCacheTaskImplThread(jobId, taskName, groupName);
        pools.submit(dataLoadingTaskImplThread);
        logger.info("SynchronousRedisCacheTaskImplThread started...");
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

    @ManagedOperation(description = "job simulator")
    @ManagedOperationParameters({@ManagedOperationParameter(description = "trigger name", name = "triggerName"),
            @ManagedOperationParameter(description = "group name", name = "groupName")})
    public void simulateJob(String triggerName, String groupName) {
        logger.info("Test the job trigger from simulator.");
        RemoteJobInvokeParamsDto rD = new RemoteJobInvokeParamsDto();
        rD.addParam("startTime", "2018-06-20 00:00:00");
        rD.addParam("endTime", "2018-06-21 00:00:00");
        rD.addParam("isAutoDocking", "true");
        executeJobWithParams(GernerateUuidUtils.getUUID(), triggerName, groupName, rD);
    }

    class SynchronousRedisCacheTaskImplThread implements Runnable {
        private final Logger logger = Logger.getLogger(SynchronousRedisCacheTaskImplThread.class);
        private String jobId;
        private String taskName;
        private String groupName;

        public SynchronousRedisCacheTaskImplThread(String jobId, String taskName, String groupName) {
            this.jobId = jobId;
            this.taskName = taskName;
            this.groupName = groupName;
        }

        @Override
        public void run() {
            JobBizStatusEnum jobBizStatusEnum;
            try {
                logger.info("---------------------更新redis中任务及数据库信息---------------------");
                logger.info(String.format("【当前任务个数%s个,数据库信息%s个】", ParseXMLFileUtil.getCacheTaskEntitiesKeys().size(), ParseXMLFileUtil.getCacheDbEntities().size()));
                jobBizStatusEnum = JobBizStatusEnum.RUNNING;
                SendMsg2AMQ.updateStatusAndSend(jobId, jobBizStatusEnum, jmsClusterMgr);
                ParseXMLFileUtil.initTaskByRedis(new Properties());
                logger.info("---------------------更新完成---------------------");
                logger.info(String.format("【任务个数%s个,数据库信息%s个】", ParseXMLFileUtil.getCacheTaskEntitiesKeys().size(), ParseXMLFileUtil.getCacheDbEntities().size()));
                jobBizStatusEnum = JobBizStatusEnum.FINISHED;
                addTaskLog(jobId, taskName, groupName, jobBizStatusEnum, "同步redis任务及数据库信息完成",
                        new Date(), new Date());
                SendMsg2AMQ.updateStatusAndSend(jobId, jobBizStatusEnum, jmsClusterMgr);
                RinseStatusAndLogCache.removeTaskByJobId(jobId);
            } catch (Exception e) {
                String message = String.format("【同步redis任务xml异常】", taskName);
                logger.error(message, e);
                jobBizStatusEnum = JobBizStatusEnum.INTERRUPTED;
                // 任务状态日志入库
                addTaskLog(jobId, taskName, groupName, jobBizStatusEnum, message);
                SendMsg2AMQ.updateStatusAndSend(jobId, jobBizStatusEnum, jmsClusterMgr);
                RinseStatusAndLogCache.removeTaskByJobId(jobId);
                return;
            }
        }
    }
}
