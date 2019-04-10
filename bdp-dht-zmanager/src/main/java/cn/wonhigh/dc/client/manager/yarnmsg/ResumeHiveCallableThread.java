package cn.wonhigh.dc.client.manager.yarnmsg;

import cn.wonhigh.dc.client.common.model.ResumeHiveTaskInfo;
import cn.wonhigh.dc.client.common.model.ResumeHiveTaskInfoCondition;
import cn.wonhigh.dc.client.manager.*;
import cn.wonhigh.dc.client.service.ApplicationInfoService;
import cn.wonhigh.dc.client.service.ResumeHiveTaskInfoService;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.yougou.logistics.base.common.vo.scheduler.RemoteJobInvokeParamsDto;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.util.List;
import java.util.concurrent.Callable;

/**
 * 重启hive任务线程
 * 有返回值
 */
public class ResumeHiveCallableThread implements Callable<String> {

    private static final Logger logger = LoggerFactory.getLogger("yarnInfo");
    private String jobId = "";
    ClassPathXmlApplicationContext ctx;
    private ApplicationInfoService applicationInfoService;
    ResumeHiveTaskInfoService resumeHiveTaskInfoService;
    DataCleaningTaskImpl dataCleaningTask;
    CDCDataMoveTaskImpl cDCDataMoveTask;
    DataLoadingTaskImpl dataLoadingTask;
    DataOutputTaskImpl dataOutputTask;
    DataParquetDistCpTaskImpl dataParquetDistCpTask;
    DataSynVerificationTaskImpl dataSynVerificationTask;
    TabDupPrimaryConfigTaskImpl tabDupPrimaryConfigTask;


    public ResumeHiveCallableThread(String jobId,
                                    ClassPathXmlApplicationContext ctx) {
        this.jobId = jobId;
        this.resumeHiveTaskInfoService = (ResumeHiveTaskInfoService) ctx.getBean("resumeHiveTaskInfoService");
        this.dataCleaningTask = (DataCleaningTaskImpl) ctx.getBean("dataCleaningTaskImpl");
        this.cDCDataMoveTask = (CDCDataMoveTaskImpl) ctx.getBean("cdcDataMoveTaskImpl");
        this.dataLoadingTask = (DataLoadingTaskImpl) ctx.getBean("dataLoadingTaskImpl");
        this.dataOutputTask = (DataOutputTaskImpl) ctx.getBean("dataOutputTaskImpl");
        this.dataParquetDistCpTask = (DataParquetDistCpTaskImpl) ctx.getBean("dataParquetDistCpTaskImpl");
        this.dataSynVerificationTask = (DataSynVerificationTaskImpl) ctx.getBean("dataSynVerificationTaskImpl");
        this.tabDupPrimaryConfigTask = (TabDupPrimaryConfigTaskImpl) ctx.getBean("tabDupPrimaryConfigTaskImpl");
        logger.info("ResumeHiveCallableThread init finished....");

    }


    @Override
    public String call() {
        logger.info("start to call() ResumeHiveCallableThread....");
        String result = "error";

            ResumeHiveTaskInfoCondition condition = new ResumeHiveTaskInfoCondition();

            if(StringUtils.isNotBlank(jobId)){
                logger.info("根据jobId=【"+jobId+"】过滤重启任务信息表");
                condition.createCriteria().andJobIdEqualTo(jobId);
            }
            //如果没有传入jobId,则扫描全表加载
            List<ResumeHiveTaskInfo> resumeHiveTaskInfos = null;
        try {
            resumeHiveTaskInfos = resumeHiveTaskInfoService.selectByCondition(condition);
        } catch (Exception e) {
            e.printStackTrace();
        }

        if (null == resumeHiveTaskInfos || 0 == resumeHiveTaskInfos.size()) {
                logger.info("当前jobId=" + jobId + "无记录，不需要执行");
                result = "error";
                return result;
            }
            logger.info("总共有"+resumeHiveTaskInfos.size()+"个任务需要重新启动...");
            //一个jobId只能存在一条记录
            for(ResumeHiveTaskInfo resumeHiveTaskInfo : resumeHiveTaskInfos){
                result = resumeTask(resumeHiveTaskInfo);
            }


        return result;
    }

    private String resumeTask(ResumeHiveTaskInfo resumeHiveTaskInfo){
        String result="error";
        try {

            //业务参数json转bean
            String parmsDto = resumeHiveTaskInfo.getRemoteJobInvokeParams();
            if (StringUtils.isBlank(parmsDto)) {
                logger.info("记录中参数丢失，无法重新执行hive任务");
                resumeHiveTaskInfoService.deleteByPrimaryKey(resumeHiveTaskInfo.getId());
                result = "error";
                return result;
            }

            String jobId = resumeHiveTaskInfo.getJobId();
            String triggerName = resumeHiveTaskInfo.getTriggerName();
            String groupName = resumeHiveTaskInfo.getGroupName();


            RemoteJobInvokeParamsDto remoteJobInvokeParamsDto = new Gson().fromJson(parmsDto, RemoteJobInvokeParamsDto.class);

            //根据不同类别的hive任务，分别启动不同的任务实现类拉起任务重新执行
            if (resumeHiveTaskInfo.getRemark().contains(cDCDataMoveTask.getClass().getSimpleName())) {
                //
                logger.info("重新执行任务："+cDCDataMoveTask.getClass().getSimpleName());
                cDCDataMoveTask.executeJobWithParams(jobId, triggerName, groupName, remoteJobInvokeParamsDto);
                result = "success";

            } else if (resumeHiveTaskInfo.getRemark().contains(dataCleaningTask.getClass().getSimpleName())) {
                //调用数据清洗
                logger.info("重新执行任务："+dataCleaningTask.getClass().getSimpleName());

                dataCleaningTask.executeJobWithParams(jobId, triggerName, groupName, remoteJobInvokeParamsDto);
                result = "success";

            } else if (resumeHiveTaskInfo.getRemark().contains(dataLoadingTask.getClass().getSimpleName())) {
                logger.info("重新执行任务："+dataLoadingTask.getClass().getSimpleName());

                dataLoadingTask.executeJobWithParams(jobId, triggerName, groupName, remoteJobInvokeParamsDto);
                //调用数据加载
                result = "success";
            } else if (resumeHiveTaskInfo.getRemark().contains(dataOutputTask.getClass().getSimpleName())) {
                //调用数据导出
                logger.info("重新执行任务："+dataOutputTask.getClass().getSimpleName());

                dataOutputTask.executeJobWithParams(jobId, triggerName, groupName, remoteJobInvokeParamsDto);
                result = "success";
            } else if (resumeHiveTaskInfo.getRemark().contains(dataParquetDistCpTask.getClass().getSimpleName())) {
                logger.info("重新执行任务："+dataParquetDistCpTask.getClass().getSimpleName());

                dataParquetDistCpTask.executeJobWithParams(jobId, triggerName, groupName, remoteJobInvokeParamsDto);
                result = "success";
            } else if (resumeHiveTaskInfo.getRemark().contains(dataSynVerificationTask.getClass().getSimpleName())) {
                logger.info("重新执行任务："+dataSynVerificationTask.getClass().getSimpleName());

                dataSynVerificationTask.executeJobWithParams(jobId, triggerName, groupName, remoteJobInvokeParamsDto);
                result = "success";
            } else if (resumeHiveTaskInfo.getRemark().contains(tabDupPrimaryConfigTask.getClass().getSimpleName())) {
                logger.info("重新执行任务："+tabDupPrimaryConfigTask.getClass().getSimpleName());

                tabDupPrimaryConfigTask.executeJobWithParams(jobId, triggerName, groupName, remoteJobInvokeParamsDto);
                result = "success";
            }

            //执行完毕则删除当前记录
            resumeHiveTaskInfoService.deleteByPrimaryKey(resumeHiveTaskInfo.getId());
            logger.info("id=" + resumeHiveTaskInfo.getId() + "任务启动成功，删除数据库记录");

        } catch (JsonSyntaxException e) {
            result = "error";
            logger.error("再次执行hive任务失败，jobId=" + jobId);
            e.printStackTrace();
        }catch (Exception e) {
            e.printStackTrace();
        }

        return result;
    }
}
