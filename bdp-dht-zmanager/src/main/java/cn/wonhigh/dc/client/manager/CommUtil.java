package cn.wonhigh.dc.client.manager;

import cn.wonhigh.dc.client.common.model.ResumeHiveTaskInfo;
import cn.wonhigh.dc.client.common.model.ResumeHiveTaskInfoCondition;
import cn.wonhigh.dc.client.common.model.TaskPropertiesConfig;
import cn.wonhigh.dc.client.common.util.ParseXMLFileUtil;
import cn.wonhigh.dc.client.service.ResumeHiveTaskInfoService;
import com.google.gson.Gson;
import com.yougou.logistics.base.common.vo.scheduler.RemoteJobInvokeParamsDto;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.util.Date;
import java.util.List;

public class CommUtil {
    private static final Logger logger = Logger.getLogger(CommUtil.class);

    /**
     * 检查任务执行所需的必要条件
     *
     * @param groupName
     * @param triggerName
     * @param taskId
     * @return
     */
    public static TaskPropertiesConfig checkTaskExecCondition(String groupName, String triggerName, String taskId) {
        TaskPropertiesConfig taskConfig = ParseXMLFileUtil.getTaskConfig(groupName, triggerName);
        if (taskConfig == null) {
            return null;
        }
        //获取依赖任务
        TaskPropertiesConfig parentTaskConfig = taskConfig.getDependencyTaskList() != null ? taskConfig
                .getDependencyTaskList().get(0) : null;

        //去重一定依赖于导入，所以当导入任务不存在是，说明配置文件加载失败
        if (parentTaskConfig == null) {
            return null;
        }

        // 判断是否有关联键
        if (taskConfig.getRelationColumns() == null || taskConfig.getRelationColumns().size() == 0) {
            return null;
        }
        return taskConfig;
    }

    /**
     * 更新或新增hive执行失败记录数据
     *
     * @return
     */
    public static int insertOrUpdateinstance(ResumeHiveTaskInfoService resumeHiveTaskInfoService,
                                             ResumeHiveTaskInfo resumeHiveTaskInfo) {

        int count = 0;
        try {
            count = resumeHiveTaskInfoService.insertOrUpdateInstance(resumeHiveTaskInfo);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return count;
    }

    /**
     * 删除再次成功执行的hive数据
     *
     * @return
     */
    public static int deleteByConditon(ResumeHiveTaskInfoService resumeHiveTaskInfoService, ResumeHiveTaskInfo resumeHiveTaskInfo) {
        int count = 0;
        try {
            ResumeHiveTaskInfoCondition condition = new ResumeHiveTaskInfoCondition();
            if (null != resumeHiveTaskInfo.getId()) {

                count = resumeHiveTaskInfoService.deleteByPrimaryKey(resumeHiveTaskInfo.getId());
                logger.info("根据主键id=" + resumeHiveTaskInfo.getId() + "删除数据成功");
            } else if (StringUtils.isNotBlank(resumeHiveTaskInfo.getJobId())) {

                condition.createCriteria().andJobIdEqualTo(resumeHiveTaskInfo.getJobId());
                count = resumeHiveTaskInfoService.deleteByCondition(condition);

                logger.info("根据jobId=" + resumeHiveTaskInfo.getJobId() + "删除数据成功");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return count;
    }

    /**
     * 查询当前系统存在的执行失败的hive任务，待重新执行
     *
     * @return
     */
    public static List<ResumeHiveTaskInfo> selectByCondition(ResumeHiveTaskInfoService resumeHiveTaskInfoService, ResumeHiveTaskInfo resumeHiveTaskInfo) throws Exception {

        ResumeHiveTaskInfoCondition condition = new ResumeHiveTaskInfoCondition();
        logger.info("查询当前系统所有未执行成功的hive任务");
        List<ResumeHiveTaskInfo> resumeHiveTaskInfos = null;
        try {
            resumeHiveTaskInfos = resumeHiveTaskInfoService.selectByCondition(condition);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return resumeHiveTaskInfos;
    }

    public static boolean saveJobInfo(ResumeHiveTaskInfoService resumeHiveTaskInfoService,
                                      String jobId, String taskName, String groupName,
                                      RemoteJobInvokeParamsDto remoteJobInvokeParamsDto, Class executeClass) throws Exception {
        Gson gson = new Gson();
        String jobInfo = gson.toJson(remoteJobInvokeParamsDto);
        ResumeHiveTaskInfo resumeHiveTaskInfo = new ResumeHiveTaskInfo();
        resumeHiveTaskInfo.setJobId(jobId);
        resumeHiveTaskInfo.setGroupName(groupName);
        resumeHiveTaskInfo.setTriggerName(taskName);
        resumeHiveTaskInfo.setRemark(executeClass.getSimpleName());
        resumeHiveTaskInfo.setRemoteJobInvokeParams(jobInfo);
        return resumeHiveTaskInfoService.insertOrUpdateInstance(resumeHiveTaskInfo) > 0;
    }

    public static boolean delJobInfo(ResumeHiveTaskInfoService resumeHiveTaskInfoService, String jobId) throws Exception {
        ResumeHiveTaskInfoCondition condition = new ResumeHiveTaskInfoCondition();
//            condition.createCriteria().andJobIdIsNotNull();
        condition.createCriteria().andJobIdEqualTo(jobId);
        logger.info("根据jobId=[" + jobId + "]删除resume_hive中数据记录。");
        boolean flag = false;
        return resumeHiveTaskInfoService.deleteByCondition(condition) > 0;
    }

    /**
     * Created by user on 2017/3/8.
     */
    public static class TestHive {
    }
}
