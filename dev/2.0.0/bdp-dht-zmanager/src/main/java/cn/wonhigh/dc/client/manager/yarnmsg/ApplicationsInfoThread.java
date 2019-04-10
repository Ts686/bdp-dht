package cn.wonhigh.dc.client.manager.yarnmsg;

import cn.wonhigh.dc.client.common.constans.MessageConstant;
import cn.wonhigh.dc.client.common.model.ApplicationInfo;
import cn.wonhigh.dc.client.common.util.DateUtils;
import cn.wonhigh.dc.client.common.util.HttpClientUtil;
import cn.wonhigh.dc.client.common.util.StringUtils;
import cn.wonhigh.dc.client.common.util.redis.JedisUtils;
import cn.wonhigh.dc.client.manager.cache.RinseStatusAndLogCache;
import cn.wonhigh.dc.client.service.ApplicationInfoService;
import cn.wonhigh.dc.client.service.ClientTaskStatusLogService;
import cn.wonhigh.dc.client.service.jms.JmsClusterMgr;
import cn.wonhigh.dc.scheduler.common.api.dto.bizMsg.JobExecStateMsgDto;
import com.google.gson.*;
import com.yougou.logistics.base.common.enums.JobBizStatusEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.util.*;
import java.util.concurrent.FutureTask;

/**
 * 异步获取yarn中application的相关信息
 */
public class ApplicationsInfoThread implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger("yarnInfo");
    ApplicationInfoService applicationInfoService;
    ClientTaskStatusLogService clientTaskStatusLogService;
    ClassPathXmlApplicationContext ctx;

    private String url = "";
    private Map<String, Object> parmasMap = null;
    //10s等待时间
    private long waitingTime = 60 * 1000;

    private JmsClusterMgr jmsClusterMgr;
    private Properties properties;

    //ExecutorService service = Executors.newFixedThreadPool(4);

    public ApplicationsInfoThread(ClassPathXmlApplicationContext ctx,
                                  String url,
                                  Map<String, Object> parmasMap,
                                  JmsClusterMgr jmsClusterMgr,
                                  Properties properties) {
        this.ctx = ctx;
        this.applicationInfoService = (ApplicationInfoService) ctx.getBean("applicationInfoService");
        this.clientTaskStatusLogService = (ClientTaskStatusLogService) ctx.getBean("clientTaskStatusLogService");
        this.parmasMap = parmasMap;
        this.url = url;
        this.jmsClusterMgr = jmsClusterMgr;
        this.properties = properties;
    }

    @Override
    public void run() {

        while (true) {
            //无限循环扫面yarn相关状态
            //通过http接口获取yarn中相关app信息
            //GET http://<rm http address:port>/ws/v1/cluster/apps
            Map<String, Object> stringObjectMap = null;

            //1.获取yarn上相关信息
            logger.info("开始获取yarn上相关app信息：url=【{}】,parmasMap=[{}]", url, parmasMap.toString());
            if (null == parmasMap) {
                stringObjectMap = HttpClientUtil.doGet(url);
            } else {
                stringObjectMap = HttpClientUtil.doGet(url, parmasMap);
            }

            if (null == stringObjectMap) {
                logger.error("获取app信息失败，返回结果为null");
            }

            //200表示为请求成功，并获取到信息
            if ("200".equals(stringObjectMap.get("statusCode").toString())) {

                String result = stringObjectMap.get("result").toString();
                //2.根据信息与数据中数据做同步
                try {
                    Gson gson = new Gson();
                    JsonObject jsonObject = gson.fromJson(result, JsonObject.class);
                    JsonObject apps = jsonObject.getAsJsonObject("apps");
                    if (null == apps) {
                        logger.info("无apps信息");

                        //可进行sleep进行频率控制:30s执行一次
                        sleepThread(waitingTime);
                        continue;
                    }

                    //具体app集合
                    JsonArray app = apps.getAsJsonArray("app");
                    if (null == app) {
                        logger.info("无app列表信息");
                        sleepThread(waitingTime);
                        continue;
                    }


                    logger.info("在yarn上获取到从" + DateUtils.formatDatetime(new Date(Long.parseLong(parmasMap.get("startedTimeBegin").toString())))
                            + "到" + DateUtils.formatDatetime(new Date()) + "期间总任务数为：" + app.size());

                    for (int i = 0; i < app.size(); i++) {
                        JsonElement jsonElement = app.get(i);
                        if (jsonElement.isJsonObject()) {
                            String appId = jsonElement.getAsJsonObject().get("id").getAsString();
                            String appName = jsonElement.getAsJsonObject().get("name").getAsString();
                            String state = jsonElement.getAsJsonObject().get("state").getAsString();
                            String finalStatus = jsonElement.getAsJsonObject().get("finalStatus").getAsString();
                            long startedTime = jsonElement.getAsJsonObject().get("startedTime").getAsLong();
                            long finishedTime = jsonElement.getAsJsonObject().get("finishedTime").getAsLong();
                            int elapsedTime = jsonElement.getAsJsonObject().get("elapsedTime").getAsInt();

                            //只处理hive和sqoop开头任务
                            if(!appName.toLowerCase().contains("hive")
                                    && !appName.toLowerCase().contains("sqoop")){
                                continue;
                            }
                            logger.info("app信息：" + jsonElement.toString());
                            logger.info("yarn获取信息：appId=【{}】,appName=【{}】,state=【{}】", appId, appName, state);

                            ApplicationInfo info = new ApplicationInfo();
                            info.setAppId(appId);
                            info.setAppName(appName);
                            info.setState(state);
                            info.setFinalstatus(finalStatus);
                            info.setStartedtime(startedTime);
                            info.setFinishedtime(finishedTime);
                            info.setElapsedtime(elapsedTime);
                            //info.setRemark(jsonElement.toString());

                            Map<String, String> parms = new HashMap<>(2);
                            parms.put("appName", appName);
                            parms.put("appId", appId);


                            ApplicationInfo applicationInfo = applicationInfoService.selectByAppNameAndAppId(parms);
                            if (null != applicationInfo) {
                                //存在
                                logger.info("appName=" + appName + "appId=" + appId + "记录已存在，原状态为" + applicationInfo.getState() + "，更新为" + state);
                                //state状态不一致或者finalStatus不一致则进行更新.finalStatus状态决定了程序本身执行是否成功
                                if (!state.equalsIgnoreCase(applicationInfo.getState())
                                        || !finalStatus.equalsIgnoreCase(applicationInfo.getFinalstatus())) {
                                    applicationInfo.setUpdateTime(new Date());
                                    applicationInfo.setState(state);
                                    applicationInfo.setFinalstatus(finalStatus);
                                    //状态不一致则进行更新
                                    applicationInfoService.updateByAppNameAndAppIdBySelective(applicationInfo);
                                    logger.info("更新id=" + applicationInfo.getId() + "状态成功");
                                }


                            } else {
                                //不存在，查询appName相关的信息
                                List<ApplicationInfo> appLists = applicationInfoService.selectByAppNameList(appName);
                                if (null == appLists || 0 == appLists.size()) {
                                    //此时获取不到相关的schedule_id，instanceId，subInstanceId
                                    logger.info(appName + "在yarn中没有获取到相关信息，直接跳过，扫描其他app信息");
                                    continue;
                                }
                                //如果已经存在，只取其中一个特例即可
                                ApplicationInfo appInfoTemp = appLists.get(0);

                                info.setCreateTime(new Date());
                                info.setUpdateTime(new Date());

                                info.setInstanceid(appInfoTemp.getInstanceid());
                                info.setScheduleId(appInfoTemp.getScheduleId());
                                info.setSubinstanceid(appInfoTemp.getSubinstanceid());

                                applicationInfoService.insertApplicationBySelective(info);
                                logger.info("appName=" + appName + "appId=" + appId + "记录不存在，直接进行插入操作。");

                                //插入数据成功后，将原来没有appid为空的数据进行清除
                                int delCount = applicationInfoService.deleteByAppNameAndAppidIsNull(appName);
                               if(delCount>0){
                                   logger.info("已删除appName="+appName+"且appId为空的记录");
                               }


                            }


                        }
                    }

                } catch (JsonSyntaxException e) {
                    logger.error("转换json出错，" + e.getMessage());
                    e.printStackTrace();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            logger.info("结束获取yarn上相关app信息");

            String isRestart = properties.getProperty("isRestart");
            try {
                //重启后检查，yarn的任务状态，并通知调度系统
                notifySecheduleSystem(isRestart);
            } catch (Exception e) {
                e.printStackTrace();
            }

            //2：扫描还未提交到yarn也发生了失败，重新拉起任务
            if ("true".equalsIgnoreCase(isRestart)) {
                //扫描resume_hive_task_info表，将里面失败的任务都重走一遍
                logger.info("异步全表扫描resume_hive_task_info，将所有失败任务重启动");
                new Thread(new ResumeTaskThread(ctx)).start();
                properties.setProperty("isRestart", "false");
            }

            sleepThread(waitingTime);

        }


    }

    /**
     * 根据最新yarn状态通知调度系统
     */
    private void notifySecheduleSystem(String isRestart) throws Exception {

        logger.info("异步查询调度任务的状态，并和yarn进行匹配后通知调度系统。");
        //查出缓存中的任务信息列表
        //获取重启bdp-dht时候的标志
//        String isRestart = properties.getProperty("isRestart");
        Map<String, Object> param = new HashMap<>();
        param.put("updateTime", new Date());
//        param.put("updateTime", DateUtils.getHeadDate(new Date(), -48));
        //此处不能使用缓存数据，应该获取最新的application_message中的数据
        List<String> jobIdList = applicationInfoService.selectAllJobIdByParamsMap(param);
        if (null == jobIdList || 0 == jobIdList.size()) {
            return;
        }

        Map<String, String> paramsMap = new HashMap<>();

        for (String jobId : jobIdList) {
            JobExecStateMsgDto jobStatusDto = new JobExecStateMsgDto();
            jobStatusDto.setJobId(jobId);

            //取出数据后，从缓存中删除，防止无法重洗拉起信息任务
            RinseStatusAndLogCache.removeTaskByJobId(jobId);

            paramsMap.put("jobId", jobId);
            //查询本地yarn信息状态表
            List<ApplicationInfo> applicationInfosList = applicationInfoService.selectByParamsMap(paramsMap);
            int size = 0;
            if (null != applicationInfosList) {
                size = applicationInfosList.size();
            }
            //根据sechdeuleId进行对比，规则，如果存在FAILED和KILLED则都为失败，如果都是FINISHED则都为成功，否则其他情况都RUNNING

            //重新拉起失败任务分两种情况：1：提交到yarn执行失败；2：还未提交到yarn也发生了失败；
            String temp = applicationInfosList.toString();
            logger.info(temp);
            int finishedCount = StringUtils.countStringContainSubString(temp, "state='FINISHED'");
            int succeededCount = StringUtils.countStringContainSubString(temp, "finalStatus='SUCCEEDED'");

            logger.info("jobId=【{}】在yarn信息表状态总数：【{}】，包含有FINISHED状态总数：【{}】,任务本身SUCCEEDED总数：【{}】", jobId, size, finishedCount,succeededCount);
            if (temp.contains("FAILED") || temp.contains("KILLED")) {
                //1：提交到yarn执行失败
                if ("false".equalsIgnoreCase(isRestart)) {
                    jobStatusDto.setExecStatus(JobBizStatusEnum.STOPED);
                    logger.info("非重启下：jobId = " + jobStatusDto.getJobId() + "任务为失败状态，通知调度成功，并从缓存删除成功");
                } else {
                    //失败的hive任务需要进行重新拉起执行，如果自身执行成功，则通知调度为成功
                    FutureTask<String> futureTask = new FutureTask<>(new ResumeHiveCallableThread(jobId, ctx));
                    //由于此处为重启后执行，故取消线程池
                    new Thread(futureTask).start();
                    String result = futureTask.get();
                    if ("success".equalsIgnoreCase(result)) {

                        jobStatusDto.setExecStatus(JobBizStatusEnum.FINISHED);
                        logger.info("重启后：jobId = " + jobStatusDto.getJobId() + "任务为完成状态，通知调度成功");
                        //properties.setProperty("isRestart", "false");
                    }
                    //此时执行失败，暂时不通知调度；待重新扫描全表拉起任务后，再次执行任务时通知调度；

                }


            } else if (0 != size && size == finishedCount && size==succeededCount) {
                //如果都是FINISHED则都为成功,且finalStatus全为SUCCEEDED
                jobStatusDto.setExecStatus(JobBizStatusEnum.FINISHED);
                logger.info("jobId = " + jobStatusDto.getJobId() + "任务为完成状态，通知调度成功");


            }
//            else {
//                //其他的都可以表示为运行中,2019-4-1屏蔽此分支，从yarn上只将最终状态返回给调度
//                jobStatusDto.setExecStatus(JobBizStatusEnum.RUNNING);
//                logger.info("jobId = " + jobStatusDto.getJobId() + "任务为正在运行状态，通知调度成功");
//
//            }

            //监控到有变化的才通过消息队列通知调度系统
            String val = JedisUtils.get(jobId);
            if (org.apache.commons.lang.StringUtils.isBlank(val) || !val.equalsIgnoreCase(jobStatusDto.getExecStatus().toString())) {
                //为空或则与当前的状态不一致，则通知调度
                jmsClusterMgr.sendQueueMsg(MessageConstant.DC_SCHEDULER_JOB_QUEUE, jobStatusDto);
                logger.info("yarn上的任务状态已发生改变，通知调度系统。由【" + val + "】转变为【" + jobStatusDto.getExecStatus() + "】");
            }

            JedisUtils.set(jobId, jobStatusDto.getExecStatus()==null?"":jobStatusDto.getExecStatus().toString(), 60 * 60 * 1000);

        }

        logger.info("同步yarn信息并通知给调度系统完成");
    }


    private void sleepThread(long tm) {

        //可进行sleep进行频率控制:30s执行一次
        try {
            Thread.sleep(tm);

        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


}
