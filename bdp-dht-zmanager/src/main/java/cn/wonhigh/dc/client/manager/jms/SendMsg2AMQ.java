package cn.wonhigh.dc.client.manager.jms;

import cn.wonhigh.dc.client.common.constans.MessageConstant;
import cn.wonhigh.dc.client.manager.cache.RinseStatusAndLogCache;
import cn.wonhigh.dc.client.service.jms.JmsClusterMgr;
import cn.wonhigh.dc.scheduler.common.api.dto.bizMsg.JobExecStateMsgDto;

import com.yougou.logistics.base.common.enums.JobBizStatusEnum;

public class SendMsg2AMQ {

    /**
     * 保存任务的状态至缓存、发送任务的状态至mq
     *
     * @param jobId
     * @param jobBizStatusEnum
     * @param jmsClusterMgr
     */
    public static void updateStatusAndSend(String jobId,
                                           JobBizStatusEnum jobBizStatusEnum, JmsClusterMgr jmsClusterMgr) {
        JobExecStateMsgDto jobStatusDto = new JobExecStateMsgDto();
        jobStatusDto.setCheckinTime(System.currentTimeMillis());
        jobStatusDto.setJobId(jobId);
        jobStatusDto.setExecStatus(jobBizStatusEnum);
        // 做完一个步骤就改变一次状态
        RinseStatusAndLogCache.putTaskStatus(jobId, jobBizStatusEnum);
        // 发送状态、心跳给mq
        jmsClusterMgr.sendQueueMsg(MessageConstant.DC_SCHEDULER_JOB_QUEUE,
                jobStatusDto);
    }

    /**
     * 保存任务的状态至缓存、发送任务的状态至mq
     *
     * @param jobId
     * @param jobBizStatusEnum
     * @param jmsClusterMgr
     */
    public static void updateStatusAndSendMsg(String jobId, JobBizStatusEnum jobBizStatusEnum,
                                              JmsClusterMgr jmsClusterMgr, String errMsg) {
        JobExecStateMsgDto jobStatusDto = new JobExecStateMsgDto();
        jobStatusDto.setCheckinTime(System.currentTimeMillis());
        jobStatusDto.setJobId(jobId);
        jobStatusDto.setExecStatus(jobBizStatusEnum);
        jobStatusDto.setErrorMsg(errMsg);
        // 做完一个步骤就改变一次状态
        RinseStatusAndLogCache.putTaskStatus(jobId, jobBizStatusEnum);
        // 发送状态、心跳给mq
        jmsClusterMgr.sendQueueMsg(MessageConstant.DC_SCHEDULER_JOB_QUEUE,
                jobStatusDto);
    }
}
