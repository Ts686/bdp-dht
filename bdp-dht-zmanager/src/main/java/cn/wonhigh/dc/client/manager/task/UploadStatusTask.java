package cn.wonhigh.dc.client.manager.task;

import cn.wonhigh.dc.client.common.constans.MessageConstant;
import cn.wonhigh.dc.client.common.model.TaskPropertiesConfig;
import cn.wonhigh.dc.client.common.util.ParseXMLFileUtil;
import cn.wonhigh.dc.client.common.util.PropertyFile;
import cn.wonhigh.dc.client.common.util.redis.JedisUtils;
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
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
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

    @Scheduled(fixedRate = 300000)
    public void updateTaskConfigBeat() {
        logger.info("==========>开始上传任务状态更新信息");
//        // begin
//        ConcurrentMap<String, TaskPropertiesConfig> taskmap = ParseXMLFileUtil
//                .getTaskList();
//        // Set taskset=taskmap.entrySet();
//        for (Entry<String, TaskPropertiesConfig> task : taskmap.entrySet()) {
//            TriggersMsgDto tmd = new TriggersMsgDto();
//            tmd.setIsFull(task.getValue().getIsOverwrite());
//            tmd.setTriggerGroup(task.getValue().getGroupName());
//            tmd.setTriggerName(task.getValue().getTriggerName());
////			Integer tableConcurrent=1;
////			String tableConcurrentStr=task.getValue().getTableConcurrent();
////			if(tableConcurrentStr!=null&&tableConcurrentStr.trim().length()>0){
////			tableConcurrent=Integer.getInteger(tableConcurrentStr);
////			}
////			tmd.setTableConcurrent(tableConcurrent);
////			String importDurationPara = "0";
////			String importDurationParaStr=task.getValue().getImportDurationPara();
////			if(importDurationParaStr!=null&&importDurationParaStr.trim().length()>0){
////			tableConcurrent=Integer.getInteger(importDurationParaStr.trim());
////			}
////			tmd.setImportDurationPara(importDurationPara);
//            //状态不为空才传mq
//            if (tmd != null) {
//                jmsClusterMgr.sendQueueMsg(MessageConstant.DC_SCHEDULER_TRIGGERMSG_QUEUE, tmd);
//            }
//        }
//        logger.debug("==========>结束上传任务状态更新信息");
        Properties proprties = PropertyFile.getProps("");
        String isStart = proprties.getProperty(MessageConstant.OVERWIRTE_HEARTBEAT_FIRST);
        ConcurrentMap<String, String> overwriteInfos = ParseXMLFileUtil.getOverwriteInfos();
        logger.info(isStart);
        if ("true".equalsIgnoreCase(isStart) && null != overwriteInfos && !overwriteInfos.isEmpty()) {//启动后首次心跳
            logger.info("启动后【首次】心跳同步任务增全量信息...");
            syncOverwriteInfo(overwriteInfos);
            proprties.setProperty(MessageConstant.OVERWIRTE_HEARTBEAT_FIRST, "false");
            logger.info(String.format("【首次】心跳同步任务增全量信息结束,同步数量：【%s】 个,队列名称:【%s】",
                    overwriteInfos.size(), MessageConstant.DC_SCHEDULER_TRIGGERMSG_QUEUE));
        } else { //如果是运行时心跳,直接发送
            logger.info("心跳同步任务增全量信息...");
            ConcurrentMap<String, String> syncOverwriteInfo = ParseXMLFileUtil.getSyncOverwriteInfo();
            if (null == syncOverwriteInfo || syncOverwriteInfo.isEmpty()) {
                logger.info("暂无任务更改增全量信息");
            } else {
                syncOverwriteInfo(syncOverwriteInfo);
                logger.info(String.format("心跳同步任务增全量信息结束,同步数量：【%s】 个:【%s】，队列名称:【%s】",
                        syncOverwriteInfo.size(), syncOverwriteInfo.keySet(), MessageConstant.DC_SCHEDULER_TRIGGERMSG_QUEUE));
                syncOverwriteInfo.clear();
                JedisUtils.setMap("TASK_OVERWRITE_INFO", overwriteInfos, 0);
            }
        }
    }

    public void syncOverwriteInfo(ConcurrentMap<String, String> infos) {
        for (Map.Entry<String, String> entry : infos.entrySet()) {
            String key = entry.getKey();
            String overwriteInfo = entry.getValue();
            TriggersMsgDto tmd = new TriggersMsgDto();
            if (null != key && !key.isEmpty()) {
                String[] split = key.split("@");
                tmd.setTriggerGroup(split[0].trim());
                tmd.setTriggerName(split[1].trim());
                tmd.setIsFull(Integer.valueOf(overwriteInfo));
                if (tmd != null) {
                    jmsClusterMgr.sendQueueMsg(MessageConstant.DC_SCHEDULER_TRIGGERMSG_QUEUE, tmd);
                }
            }
        }
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
