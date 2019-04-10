package cn.wonhigh.dc.client.manager.cache;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import cn.wonhigh.dc.client.common.model.ClientTaskStatusLog;
import cn.wonhigh.dc.client.service.ClientTaskStatusLogService;
import cn.wonhigh.dc.scheduler.common.api.dto.bizMsg.JobExecStateMsgDto;

import com.yougou.logistics.base.common.enums.JobBizStatusEnum;

/**
 * 装载状态和日志的缓存
 *
 * @author wang.w
 */
@Service
public class RinseStatusAndLogCache implements Serializable, InitializingBean {

    /**
     *
     */
    private static final long serialVersionUID = -1374256281398077737L;

    private static ConcurrentHashMap<String, JobBizStatusEnum> statusCache;
    private static ConcurrentHashMap<String, String> statusMsg;

    @Autowired
    private ClientTaskStatusLogService clientTaskStatusLogService;

    @Value("${select.status.days}")
    private String selectTaskStatusDays;

    public void setClientTaskStatusLogService(
            ClientTaskStatusLogService clientTaskStatusLogService) {
        this.clientTaskStatusLogService = clientTaskStatusLogService;
    }

    static {
        init();
    }

    private static void init() {
        statusCache = new ConcurrentHashMap<String, JobBizStatusEnum>();
        statusMsg = new ConcurrentHashMap<String, String>();
    }

    /**
     * 存入缓存
     *
     * @param jobId
     * @param jobStatus
     */
    public static synchronized void putTaskStatus(String jobId, JobBizStatusEnum jobStatus) {
        if (statusCache == null) {
            init();
        }
        statusCache.put(jobId, jobStatus);
    }

    /**
     * 根据jobId移除任务
     *
     * @param jobId
     */
    public static synchronized void removeTaskByJobId(String jobId) {
        if (statusCache != null && statusCache.size() > 0) {
            statusCache.remove(jobId);
        }
    }

    /**
     * 获得所有的任务
     *
     * @return
     */
    public static List<JobExecStateMsgDto> getTaskStatusList() {
        List<JobExecStateMsgDto> jobList = new ArrayList<JobExecStateMsgDto>();
        if (statusCache != null && statusCache.size() > 0) {
            for (Entry<String, JobBizStatusEnum> entry : statusCache.entrySet()) {
                JobExecStateMsgDto jobDto = new JobExecStateMsgDto();
                jobDto.setJobId(entry.getKey());
                jobDto.setExecStatus(entry.getValue());
                jobDto.setCheckinTime(System.currentTimeMillis());
                jobDto.setErrorMsg(statusMsg.get(entry.getKey()));
                jobList.add(jobDto);
            }
        }
        return jobList;
    }

    /**
     * 根据jobId来获取任务状态
     *
     * @param jobId
     * @return
     */
    public static JobBizStatusEnum getTaskStatusByJobId(String jobId) {

        return statusCache.get(jobId);
    }

    /**
     * 从数据库中加载任务
     */
    private void loadTaskStatus() {
        Map<String, Object> params = new HashMap<String, Object>();
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.DATE, StringUtils.isNotBlank(selectTaskStatusDays) ? -Integer.parseInt(selectTaskStatusDays) : -3);
        params.put("startSelectDate", new Date(calendar.getTimeInMillis()));
        //去掉FINISHED状态记录
        params.put("taskStatus", JobBizStatusEnum.FINISHED.name());
        List<ClientTaskStatusLog> taskStatusList = clientTaskStatusLogService.findLastestStatusList(params);
        if (taskStatusList != null) {
            for (ClientTaskStatusLog entity : taskStatusList) {
                for (JobBizStatusEnum jobEnum : JobBizStatusEnum.values()) {
                    if (jobEnum.name().equals(entity.getTaskStatus())) {
                        statusCache.put(entity.getTaskId(), jobEnum);
                        statusMsg.put(entity.getTaskId(), entity.getTaskStatusDesc());
                    }
                }

            }
        }
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        loadTaskStatus();
    }
}
