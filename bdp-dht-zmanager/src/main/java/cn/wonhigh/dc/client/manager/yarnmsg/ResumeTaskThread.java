package cn.wonhigh.dc.client.manager.yarnmsg;

import cn.wonhigh.dc.client.common.model.ResumeHiveTaskInfo;
import cn.wonhigh.dc.client.common.model.ResumeHiveTaskInfoCondition;
import cn.wonhigh.dc.client.common.util.DateUtils;
import cn.wonhigh.dc.client.manager.*;
import cn.wonhigh.dc.client.service.ResumeHiveTaskInfoService;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.yougou.logistics.base.common.vo.scheduler.RemoteJobInvokeParamsDto;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.text.ParseException;
import java.util.Date;
import java.util.List;

/**
 * 重启bdp-dht后拉起未执行完毕任务
 * 1：表中数据只保留当前一个星期数据；
 * 2：拉起时只关注最近两天数据
 */
public class ResumeTaskThread implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger("yarnInfo");
    ResumeHiveTaskInfoService resumeHiveTaskInfoService;
    DataCleaningTaskImpl dataCleaningTask;
    CDCDataMoveTaskImpl cDCDataMoveTask;
    DataLoadingTaskImpl dataLoadingTask;
    DataOutputTaskImpl dataOutputTask;
    DataParquetDistCpTaskImpl dataParquetDistCpTask;
    DataSynVerificationTaskImpl dataSynVerificationTask;
    TabDupPrimaryConfigTaskImpl tabDupPrimaryConfigTask;


    ResumeTaskThread(ClassPathXmlApplicationContext ctx) {
        this.resumeHiveTaskInfoService = (ResumeHiveTaskInfoService) ctx.getBean("resumeHiveTaskInfoService");
        this.dataCleaningTask = (DataCleaningTaskImpl) ctx.getBean("dataCleaningTaskImpl");
        this.cDCDataMoveTask = (CDCDataMoveTaskImpl) ctx.getBean("cdcDataMoveTaskImpl");
        this.dataLoadingTask = (DataLoadingTaskImpl) ctx.getBean("dataLoadingTaskImpl");
        this.dataOutputTask = (DataOutputTaskImpl) ctx.getBean("dataOutputTaskImpl");
        this.dataParquetDistCpTask = (DataParquetDistCpTaskImpl) ctx.getBean("dataParquetDistCpTaskImpl");
        this.dataSynVerificationTask = (DataSynVerificationTaskImpl) ctx.getBean("dataSynVerificationTaskImpl");
        this.tabDupPrimaryConfigTask = (TabDupPrimaryConfigTaskImpl) ctx.getBean("tabDupPrimaryConfigTaskImpl");
        logger.info("ResumeTaskThread init finished....");


    }

    @Override
    public void run() {


        List<ResumeHiveTaskInfo> resumeHiveTaskInfos = null;
        try {
            ResumeHiveTaskInfoCondition condition = new ResumeHiveTaskInfoCondition();

            Date cleaningDate = DateUtils.getHeadDate(new Date(), -168);
//            deleteByCondition
            condition.createCriteria().andCreateTimeLessThanOrEqualTo(cleaningDate);
            resumeHiveTaskInfoService.deleteByCondition(condition);
            logger.info("清除当前时间前一周数据成功:createTime<="+DateUtils.formatDatetime(cleaningDate));

            condition.clear();
            logger.info("start to run() ResumeTaskThread....");
             condition = new ResumeHiveTaskInfoCondition();
            Date createTime = null;

            createTime = DateUtils.getHeadDate(new Date(), -48);
            condition.createCriteria().andCreateTimeGreaterThanOrEqualTo(createTime);
            resumeHiveTaskInfos = resumeHiveTaskInfoService.selectByCondition(condition);
            logger.info("加载当前时间前2天的未启动任务数据成功:createTime>="+DateUtils.formatDatetime(createTime));

        } catch (Exception e) {
            e.printStackTrace();
        }
        if (null == resumeHiveTaskInfos || 0 == resumeHiveTaskInfos.size()) {
            return;
        }
        logger.info("总共有" + resumeHiveTaskInfos.size() + "个任务需要重新启动...");
        //一个jobId只能存在一条记录

        for (ResumeHiveTaskInfo resumeHiveTaskInfo : resumeHiveTaskInfos) {

            try {
                //业务参数json转bean
                String parmsDto = resumeHiveTaskInfo.getRemoteJobInvokeParams();
                if (StringUtils.isBlank(parmsDto)) {
                    logger.info("记录中参数丢失，无法重新执行hive任务");
                    resumeHiveTaskInfoService.deleteByPrimaryKey(resumeHiveTaskInfo.getId());
                    continue;
                }

                String jobId = resumeHiveTaskInfo.getJobId();
                String triggerName = resumeHiveTaskInfo.getTriggerName();
                String groupName = resumeHiveTaskInfo.getGroupName();
                RemoteJobInvokeParamsDto remoteJobInvokeParamsDto = new Gson().fromJson(parmsDto, RemoteJobInvokeParamsDto.class);

                //根据不同类别的hive任务，分别启动不同的任务实现类拉起任务重新执行
                if (resumeHiveTaskInfo.getRemark().contains(cDCDataMoveTask.getClass().getSimpleName())) {
                    //
                    logger.info("重新执行任务：" + cDCDataMoveTask.getClass().getSimpleName());
                    cDCDataMoveTask.executeJobWithParams(jobId, triggerName, groupName, remoteJobInvokeParamsDto);

                } else if (resumeHiveTaskInfo.getRemark().contains(dataCleaningTask.getClass().getSimpleName())) {
                    //调用数据清洗
                    logger.info("重新执行任务：" + dataCleaningTask.getClass().getSimpleName());

                    dataCleaningTask.executeJobWithParams(jobId, triggerName, groupName, remoteJobInvokeParamsDto);

                } else if (resumeHiveTaskInfo.getRemark().contains(dataLoadingTask.getClass().getSimpleName())) {
                    logger.info("重新执行任务：" + dataLoadingTask.getClass().getSimpleName());

                    dataLoadingTask.executeJobWithParams(jobId, triggerName, groupName, remoteJobInvokeParamsDto);
                    //调用数据加载
                } else if (resumeHiveTaskInfo.getRemark().contains(dataOutputTask.getClass().getSimpleName())) {
                    //调用数据导出
                    logger.info("重新执行任务：" + dataOutputTask.getClass().getSimpleName());

                    dataOutputTask.executeJobWithParams(jobId, triggerName, groupName, remoteJobInvokeParamsDto);
                } else if (resumeHiveTaskInfo.getRemark().contains(dataParquetDistCpTask.getClass().getSimpleName())) {
                    logger.info("重新执行任务：" + dataParquetDistCpTask.getClass().getSimpleName());

                    dataParquetDistCpTask.executeJobWithParams(jobId, triggerName, groupName, remoteJobInvokeParamsDto);
                } else if (resumeHiveTaskInfo.getRemark().contains(dataSynVerificationTask.getClass().getSimpleName())) {
                    logger.info("重新执行任务：" + dataSynVerificationTask.getClass().getSimpleName());

                    dataSynVerificationTask.executeJobWithParams(jobId, triggerName, groupName, remoteJobInvokeParamsDto);
                } else if (resumeHiveTaskInfo.getRemark().contains(tabDupPrimaryConfigTask.getClass().getSimpleName())) {
                    logger.info("重新执行任务：" + tabDupPrimaryConfigTask.getClass().getSimpleName());

                    tabDupPrimaryConfigTask.executeJobWithParams(jobId, triggerName, groupName, remoteJobInvokeParamsDto);
                }

                //执行完毕则删除当前记录
                resumeHiveTaskInfoService.deleteByPrimaryKey(resumeHiveTaskInfo.getId());
                logger.info("id=" + resumeHiveTaskInfo.getId() + "任务启动成功，删除数据库记录");

            } catch (JsonSyntaxException e) {
                logger.error("ResumeTaskThread再次执行hive任务失败:" + e.getMessage());
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            }

        }

        logger.info("所有在resume_hive_task_info表的任务重新启动完毕...");

    }
}
