package cn.wonhigh.dc.client.manager.task;

import cn.wonhigh.dc.client.common.constans.MessageConstant;
import cn.wonhigh.dc.client.common.model.TaskPropertiesConfig;
import cn.wonhigh.dc.client.common.util.ParseXMLFileUtil;
import cn.wonhigh.dc.client.manager.cache.RinseStatusAndLogCache;
import cn.wonhigh.dc.client.service.jms.JmsClusterMgr;
import cn.wonhigh.dc.scheduler.common.api.dto.bizMsg.JobExecStateMsgDto;
import cn.wonhigh.dc.scheduler.common.api.dto.bizMsg.TriggersMsgDto;
import com.yougou.logistics.base.common.enums.JobBizStatusEnum;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentMap;

/**
 * 上传状态、心跳时间
 *
 * @author wang.w
 */
@Service
public class UploadStatusTask {

    private static final Logger logger = Logger.getLogger(UploadStatusTask.class);

    @Autowired
    private JmsClusterMgr jmsClusterMgr;

    @Scheduled(cron = "*/20 * * * * ?")
    public void heartBeat() {
        logger.debug("==========>开始上传状态和心跳时间");
        List<JobExecStateMsgDto> jobDtoList = RinseStatusAndLogCache.getTaskStatusList();
        if (jobDtoList != null && jobDtoList.size() > 0) {
            for (JobExecStateMsgDto dto : jobDtoList) {
                JobBizStatusEnum jobStatus = null;
                for (JobBizStatusEnum status : JobBizStatusEnum.values()) {
                    if (status.name().equals(dto.getExecStatus())) {
                        jobStatus = status;
                    }
                }
                //状态不为空才传mq
                if (jobStatus != null) {
                    sendMsg2Queue(dto.getJobId(), jobStatus);
                }
            }
        }
        logger.debug("==========>结束上传状态和心跳时间");
    }

    @Scheduled(fixedRate = 600000)
    public void updateTaskConfigBeat() {
        logger.debug("==========>开始上传任务状态更新信息");
        // begin
        ConcurrentMap<String, TaskPropertiesConfig> taskmap = ParseXMLFileUtil
                .getTaskList();
        // Set taskset=taskmap.entrySet();
        for (Entry<String, TaskPropertiesConfig> task : taskmap.entrySet()) {
            TriggersMsgDto tmd = new TriggersMsgDto();
            tmd.setIsFull(task.getValue().getIsOverwrite());
            tmd.setTriggerGroup(task.getValue().getGroupName());
            tmd.setTriggerName(task.getValue().getTriggerName());
//			Integer tableConcurrent=1;
//			String tableConcurrentStr=task.getValue().getTableConcurrent();
//			if(tableConcurrentStr!=null&&tableConcurrentStr.trim().length()>0){
//			tableConcurrent=Integer.getInteger(tableConcurrentStr);
//			}
//			tmd.setTableConcurrent(tableConcurrent);
//			String importDurationPara = "0";
//			String importDurationParaStr=task.getValue().getImportDurationPara();
//			if(importDurationParaStr!=null&&importDurationParaStr.trim().length()>0){
//			tableConcurrent=Integer.getInteger(importDurationParaStr.trim());
//			}
//			tmd.setImportDurationPara(importDurationPara);
            //状态不为空才传mq
            if (tmd != null) {
                jmsClusterMgr.sendQueueMsg(MessageConstant.DC_SCHEDULER_TRIGGERMSG_QUEUE, tmd);
            }
        }
        logger.debug("==========>结束上传任务状态更新信息");
    }

    private void sendMsg2Queue(String jobId, JobBizStatusEnum jobBizStatusEnum) {
        JobExecStateMsgDto jobStatusDto = new JobExecStateMsgDto();
        jobStatusDto.setCheckinTime(System.currentTimeMillis());
        jobStatusDto.setJobId(jobId);
        jobStatusDto.setExecStatus(jobBizStatusEnum);
        jmsClusterMgr.sendQueueMsg(MessageConstant.DC_SCHEDULER_JOB_QUEUE,
                jobStatusDto);
    }
}
