package cn.wonhigh.dc.client.manager;

import cn.wonhigh.dc.client.common.constans.CommonEnumCollection;
import cn.wonhigh.dc.client.common.constans.CommonEnumCollection.*;
import cn.wonhigh.dc.client.common.constans.MessageConstant;
import cn.wonhigh.dc.client.common.model.*;
import cn.wonhigh.dc.client.common.util.*;
import cn.wonhigh.dc.client.manager.cache.RinseStatusAndLogCache;
import cn.wonhigh.dc.client.manager.jms.SendMsg2AMQ;
import cn.wonhigh.dc.client.service.ApplicationInfoService;
import cn.wonhigh.dc.client.service.ClientTaskStatusLogService;
import cn.wonhigh.dc.client.service.ResumeHiveTaskInfoService;
import cn.wonhigh.dc.client.service.jms.JmsClusterMgr;
import cn.wonhigh.dc.client.sqoop.JobExecutionResult;
import cn.wonhigh.dc.client.sqoop.JobStatus;
import cn.wonhigh.dc.client.sqoop.MaxHadoopJobRequestsException;
import cn.wonhigh.dc.client.sqoop.SqoopApi;
import com.google.gson.Gson;
import com.yougou.logistics.base.common.enums.JobBizStatusEnum;
import com.yougou.logistics.base.common.exception.ManagerException;
import com.yougou.logistics.base.common.interfaces.RemoteJobServiceExtWithParams;
import com.yougou.logistics.base.common.vo.scheduler.RemoteJobInvokeParamsDto;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.jmx.export.annotation.ManagedOperationParameter;
import org.springframework.jmx.export.annotation.ManagedOperationParameters;
import org.springframework.jmx.export.annotation.ManagedResource;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;

/**
 * 导入任务服务
 *
 * @author wang.w
 * @version 1.0.0
 * @date 2016-4-13 下午12:10:50
 * @copyright wonhigh.cn
 */
@Service
@ManagedResource(objectName = DataLoadingTaskImpl.MBEAN_NAME, description = "导入任务")
public class DataLoadingTaskImpl implements RemoteJobServiceExtWithParams {
    ThreadPoolExecutor pools = new ThreadPoolExecutor(
            15, 30, 60,
            TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(500), new RejectedExecutionHandler() {
        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {

        }
    });

    String columnSchema = null;


    class ParentsJobId {
        private String parentsJobId = "";
        private String currentJobId = "";

        public void setPrevJobId(String parentsJobId) {
            this.parentsJobId = parentsJobId;
        }

        public String getPrevJobId() {
            return parentsJobId;
        }

        public void setCrrentJobId(String currentJobId) {
            this.currentJobId = currentJobId;
        }

        public String getCrrentJobId() {
            return currentJobId;
        }

        public Boolean isCanRepair() {
            return (parentsJobId.equals(currentJobId) ? false : true);
        }

        public String toString() {
            return "parentsJobId: " + parentsJobId + " currentJobId: " + currentJobId;
        }

    }

    Map<String, ParentsJobId> RepairByParentsId = new HashMap<String, ParentsJobId>();

    public static final String MBEAN_NAME = "dc:client=DataLoadingTaskImpl";

    private static final Logger logger = Logger.getLogger(DataLoadingTaskImpl.class);

    public static final String SPLITONE = ":";
    public static final String SPLITTWO = ",";
    @Resource
    private ClientTaskStatusLogService clientTaskStatusLogService;
    @Autowired
    private ApplicationInfoService applicationInfoService;

    @Autowired
    private ResumeHiveTaskInfoService resumeHiveTaskInfoService;

    @Autowired
    private JmsClusterMgr jmsClusterMgr;

    @Value("${dc.date.format.default}")
    private String dateFormat = "yyyy-MM-dd HH:mm:ss";

    @Value("${dc.sqoop.fields.terminated}")
    private String fieldsTermBy;

    @Value("${dc.sqoop.lines.terminated}")
    private String linesTermBy;

    @Value("${dc.sqoop.bindir}")
    private String bindir;

    @Value("${dc.sqoop.null.not.string}")
    private String nullNotString;

    @Value("${dc.sqoop.outdir}")
    private String outdir;

    @Value("${dc.sqoop.null.string}")
    private String nullString;

    @Value("${dc.sqoop.import.map.num}")
    private String mapNumIm = "1";

    @Value("${dc.import.params}")
    private String importParams = "";

    @Value("${hive.job.name}")
    private String hiveJobNamePref = "";

    private final String tmpDir = "/tmp/hive/";

    public void setclientTaskStatusLogService(ClientTaskStatusLogService clientTaskStatusLogService) {
        this.clientTaskStatusLogService = clientTaskStatusLogService;
    }

    public void setMapNumIm(String mapNumIm) {
        this.mapNumIm = mapNumIm;
    }

    @Override
    public void initializeJob(String jobId, String triggerName, String groupName) {
        // if(!transactionHistoryLogs.isEmpty()){
        // HiveUtils.transactionHistoryLogSrc =
        // transactionHistoryLogs.split(",");
        // logger.info(String.format("【jobId为：%s】的任务被重复调用", jobId));
        // }else{
        //
        // }
    }

    @Override
    public void pauseJob(String jobId, String triggerName, String groupName) {

    }

    @Override
    public void resumeJob(String jobId, String triggerName, String groupName) {

    }

    @Override
    public void stopJob(String jobId, String triggerName, String groupName) {

    }

    @Override
    public void restartJob(String jobId, String triggerName, String groupName) {

    }

    @Override
    public JobBizStatusEnum getJobStatus(String taskId, String taskName, String groupName) {
        Map<String, Object> params = new HashMap<String, Object>();
        params.put(CommonEnumCollection.ParamNameEnum.TASK_ID.getValue(), taskId);
        params.put(CommonEnumCollection.ParamNameEnum.TASK_NAME.getValue(), taskName);
        params.put(CommonEnumCollection.ParamNameEnum.GROUP_NAME.getValue(), groupName);
        ClientTaskStatusLog clientTaskStatusLog = clientTaskStatusLogService.findLastestStatus(params);
        JobBizStatusEnum latestStatus = null;
        if (clientTaskStatusLog != null) {
            for (JobBizStatusEnum status : JobBizStatusEnum.values()) {
                if (status.name().equals(clientTaskStatusLog.getTaskStatus())) {
                    latestStatus = status;
                }
            }
        }
        return latestStatus;
    }

    /**
     * 暂时无用
     */
    @Override
    public String getLogs(String jobId, String triggerName, String groupName, long lastDate) {
        return null;
    }

    /**
     * 调度中心导入jmx方法调用
     * 注意：jobId在此为调度系统的subInstanceId（子实例id）
     * 真正的jobId需要在RemoteJobInvokeParamsDto对象中获取
     */
    @Override
    public void executeJobWithParams(String jobId, String taskName, String groupName,
                                     RemoteJobInvokeParamsDto remoteJobInvokeParamsDto) {
        DataLoadingTaskImplThread dataLoadingTaskImplThread = new DataLoadingTaskImplThread(jobId, taskName, groupName, remoteJobInvokeParamsDto);
        pools.submit(dataLoadingTaskImplThread);
        logger.info("DataLoadingTaskImplThread started...");
    }


    private List<Object> checkParamValue(RemoteJobInvokeParamsDto remoteJobInvokeParamsDto, String taskId)
            throws ManagerException {
        List<Object> returnList = new ArrayList<Object>();
        if (remoteJobInvokeParamsDto != null) {
            String startTimeStr = remoteJobInvokeParamsDto
                    .getParam(CommonEnumCollection.ParamNameEnum.START_TIME.getValue());
            returnList.add(startTimeStr);
            String endTimeStr = remoteJobInvokeParamsDto
                    .getParam(CommonEnumCollection.ParamNameEnum.END_TIME.getValue());
            logger.info("开始时间：" + startTimeStr + " 结束时间：" + endTimeStr);
            returnList.add(endTimeStr);
            SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
            if (startTimeStr != null && endTimeStr != null) {
                logger.info(String.format("【jobId为：%s】的任务被调用,开始时间:%s ;结束时间:%s .", taskId, startTimeStr, endTimeStr));
                try {
                    Date syncBeginTime = sdf.parse(startTimeStr);
                    Date syncEndTime = sdf.parse(endTimeStr);
                    returnList.add(syncBeginTime);
                    returnList.add(syncEndTime);
                } catch (ParseException e) {
                    RuntimeException runtimeException = new RuntimeException(String.format("【jobId为：%s】的任务被调用,开始时间:%s ;结束时间:%s 转换出现异常", taskId,
                            startTimeStr, endTimeStr));
                    JobBizStatusEnum jobBizStatusEnum = JobBizStatusEnum.STOPED;
                    SendMsg2AMQ.updateStatusAndSendMsg(taskId, jobBizStatusEnum, jmsClusterMgr,
                            ExceptionUtil.getStackTrace(runtimeException));
                    throw runtimeException;
                }
            } else {
                ManagerException managerException = new ManagerException(String.format("【jobId为：%s】的任务被调用,传入的开始和结束时间为空.", taskId));
                JobBizStatusEnum jobBizStatusEnum = JobBizStatusEnum.STOPED;
                SendMsg2AMQ.updateStatusAndSendMsg(taskId, jobBizStatusEnum, jmsClusterMgr,
                        ExceptionUtil.getStackTrace(managerException));
                throw new ManagerException(String.format("【jobId为：%s】的任务被调用,传入的开始和结束时间为空.", taskId));
            }

        } else {
            ManagerException managerException = new ManagerException(String.format("【jobId为：%s】的任务被调用,传入的参数为空.", taskId));
            JobBizStatusEnum jobBizStatusEnum = JobBizStatusEnum.STOPED;
            SendMsg2AMQ.updateStatusAndSendMsg(taskId, jobBizStatusEnum, jmsClusterMgr,
                    ExceptionUtil.getStackTrace(managerException));
            throw managerException;
        }
        return returnList;
    }

    /**
     * 任务初始化
     *
     * @throws Exception
     */
    private SqoopParams taskInitMethod(String jobId, String taskName, String groupName,
                                       ClientTaskStatusLog clientTaskStatusLog, RemoteJobInvokeParamsDto remoteJobInvokeParamsDto,
                                       Date syncBeginTime, Date syncEndTime, Integer isRepairStr, String parentsJobId,
                                       TaskPropertiesConfig taskConfig) throws ManagerException {

        String message = String.format("开始检查参数导入参数是否有效！  【groupName：%s】【triggerName：%s】", groupName, taskName);
        logger.info(message);
        checkParamValue(remoteJobInvokeParamsDto, jobId);
        message = String.format("完成检查参数导入参数有效！  【groupName：%s】【triggerName：%s】", groupName, taskName);
        logger.info(message);
        // 0.更新初始化状态
        JobBizStatusEnum jobBizStatusEnum = JobBizStatusEnum.INITIALIZING;

        addTaskLog(jobId, taskName, groupName, jobBizStatusEnum, message);
        SendMsg2AMQ.updateStatusAndSendMsg(jobId, jobBizStatusEnum, jmsClusterMgr, message);
        message = String.format("初始化中...  【groupName：%s】【triggerName：%s】", groupName, taskName);
        logger.info(message);
        String jobName = String.format("%s-%s-%s_%s", "hive", taskConfig.getGroupName(),
                taskConfig.getTriggerName(), System.currentTimeMillis());
        try {
            // 1.加载数据，并校验任务
            if (null != taskConfig) {
                if (taskConfig.getIsOverwrite() == 1) {
                    if (taskConfig.getIsSlaveTable() != 1) {
                        // 当同步目标表为parquet类型时，采用txt类型表先做临时存储
                        if (taskConfig.getSyncType().equals(SyncTypeEnum.SYNC_TYPE_1.getValue())) {
                            // 当txt表不存在时，则创建
                            StringBuilder sbBuilder = new StringBuilder();
                            sbBuilder.append("create table if not exists ");
                            sbBuilder.append(taskConfig.getTargetTable());
                            sbBuilder.append(HiveDefinePartNameEnum.TMP_TABLE_NAME_SUBFIX.getValue());
                            sbBuilder
                                    .append(new SimpleDateFormat(DateFormatStrEnum.PARTITION_DATE_YYYY_MM_DD.getValue())
                                            .format(syncBeginTime));
                            sbBuilder.append(" like ");
                            sbBuilder.append(taskConfig.getTargetTable());
                            sbBuilder.append(" stored as textfile ");
                            message = String.format("开始尝试创建txt类型的表【%s】【groupName：%s】【triggerName：%s】sql:【%s】",
                                    taskConfig.getTargetTable(), groupName, taskName, sbBuilder.toString());
                            logger.info(message);
                            logger.info(String.format("!!!创建临时表任务开始执行,任务名称【%s】", jobName));
                            boolean execUpdate = HiveUtils.execUpdate(taskConfig.getTargetDbEntity(),
                                    taskConfig, hiveJobNamePref, sbBuilder.toString(),
                                    30, jobName);
                            if (execUpdate) {
                                logger.info(String.format("创建临时表任务【%s】执行完成!!!", jobName));
                            }
                            // 清空txt类型表的数据
                            sbBuilder = new StringBuilder();
                            sbBuilder.append("TRUNCATE TABLE ");
                            sbBuilder.append(taskConfig.getTargetTable());
                            sbBuilder.append(HiveDefinePartNameEnum.TMP_TABLE_NAME_SUBFIX.getValue());
                            sbBuilder
                                    .append(new SimpleDateFormat(DateFormatStrEnum.PARTITION_DATE_YYYY_MM_DD.getValue())
                                            .format(syncBeginTime));
                            message = String.format("txt类型临时表全量表清空【groupName：%s】【triggerName：%s】sql:【%s】", groupName,
                                    taskName, sbBuilder.toString());
                            logger.info(message);
                            jobName = String.format("%s-%s-%s_%s", "hive", taskConfig.getGroupName(),
                                    taskConfig.getTriggerName(), System.currentTimeMillis());
                            logger.info(String.format("清空目标表--->【%s】,任务名称【%s】",
                                    taskConfig.getTargetTable(), jobName));
                            HiveUtils.execUpdate(taskConfig.getTargetDbEntity(), taskConfig, hiveJobNamePref, sbBuilder.toString(),
                                    30, jobName);
                            logger.info(String.format("任务【%s】执行完成!", jobName));
                            HiveUtils.syncMetaData4Impala(sbBuilder.toString()
                                    , taskConfig.getTargetDbEntity().getDbName(), taskConfig.getTargetTable());
                        }

                        String sql = "TRUNCATE TABLE " + taskConfig.getTargetTable();

                        message = String.format("该任务是全量导入任务，首先要清空装载区内的数据【%s】【groupName：%s】【triggerName：%s】", sql,
                                groupName, taskName);
                        logger.info(message);
                        jobName = String.format("%s-%s-%s_%s", "hive", taskConfig.getGroupName(),
                                taskConfig.getTriggerName(), System.currentTimeMillis());
                        logger.info(String.format("清空目标表--->【%s】,任务名称【%s】",
                                taskConfig.getTargetTable(), jobName));
                        Boolean overWriteTruncat = HiveUtils.execUpdate(taskConfig.getTargetDbEntity(), taskConfig, hiveJobNamePref,
                                sql, 30);
                        if (overWriteTruncat) {
                            message = String.format("改任务是全量导入任务，清空装载区内的数据【%s】【groupName：%s】【triggerName：%s】", sql,
                                    groupName, taskName);
                            logger.info(message);
                            //同步元数据到Impala Catalog
                            HiveUtils.syncMetaData4Impala(sql
                                    , taskConfig.getTargetDbEntity().getDbName(),
                                    taskConfig.getTargetTable());
                            logger.info(String.format("任务【%s】执行完成!", jobName));
                        } else {
                            ManagerException managerException = new ManagerException(String.format("【groupName：%s】【triggerName：%s】导入出现异常,清空全量表失败：",
                                    groupName, taskName));
                            jobBizStatusEnum = JobBizStatusEnum.INTERRUPTED;
                            SendMsg2AMQ.updateStatusAndSendMsg(jobId, jobBizStatusEnum, jmsClusterMgr,
                                    ExceptionUtil.getStackTrace(managerException));
//                            throw managerException
                        }
                    }
                    return getFullSqoopParams(taskConfig, syncBeginTime, syncEndTime, jobId, remoteJobInvokeParamsDto);
                } else if (!taskConfig.getSourceTable().endsWith(HiveDefinePartNameEnum.CDC_TABLE_SUBFIX.getValue())) {
                    if (taskConfig.getIsSlaveTable() != 1) {
                        // 2.truncate 分区内的所有数据(*transaction_history_log
                        // 此表除外，因为此表只会新增数据)
                        // 修复数据，则先备份 src 表在执行操作
                        int getXml = taskConfig.getRepairData();
                        if (-1 == getXml) {
                            message = String.format(
                                    "%s-%s_src.xml 文件未配置isRepairData项，因此默认值为【%d】,使用从调度传来的值isRepair为【%d】", groupName,
                                    taskName, getXml, isRepairStr);
                        } else {
                            isRepairStr = getXml;
                            message = String.format("从：%s-%s_src.xml 中获取isRepairData为【%d】,从调度传来的isRepair为【%d】",
                                    groupName, taskName, getXml, isRepairStr);
                        }
                        logger.info(message);
                        String key = groupName + "@" + taskName;
                        logger.info("key: " + key);
                        ParentsJobId job = null;
                        if (RepairByParentsId.keySet().contains(key)) {
                            job = RepairByParentsId.get(key);
                        }
                        logger.info("key: " + key + " isCanRepair:" + job.isCanRepair()
                                + " RepairByParentsId.toString() " + job);
                        if (isRepairStr == 1 && (1 == getXml ? true : (job.isCanRepair() && job != null))) {
                            // 设置
                            job.setCrrentJobId(parentsJobId);

                            message = String.format(
                                    "进入数据修复流程该流程共分为四步：\n" + " 第一步：重命名原表 \n" + " 第二步：重建新表 \n" + " 第三步：向新表中插入原表的数据 \n"
                                            + " 第四步：将业务库的数据插入到新表 \n" + "【groupName：%s】【triggerName：%s】",
                                    groupName, taskName);
                            logger.info(message);

                            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
                            Date currentDate = new Date();
                            String strCurrentDate = sdf.format(currentDate);

                            // 第一步 先将src 原表重命名为 表名称 + 时间戳（yyyyMMddHHmmss）
                            // 新表名称
                            String strNewTableName = taskConfig.getTargetTable() + "_bak" + strCurrentDate;

                            StringBuilder repairData = new StringBuilder();
                            repairData.append(" alter table ");
                            repairData.append(taskConfig.getTargetTable());
                            repairData.append(" rename to ");
                            repairData.append(strNewTableName);
                            message = String.format(
                                    "修复数据 第一步：重命名原表：【sourceTableName：%s】为【newTableName：%s】执行的sql语句：%s 【groupName：%s】【triggerName：%s】",
                                    taskConfig.getSourceTable(), strNewTableName, repairData.toString(), groupName,
                                    taskName);
                            logger.info(message);
                            HiveUtils.execUpdate(taskConfig.getTargetDbEntity(), taskConfig, hiveJobNamePref, repairData.toString(),
                                    30);
                            //同步元数据到Impala Catalog
                            HiveUtils.syncMetaData4Impala(repairData.toString()
                                    , taskConfig.getTargetDbEntity().getDbName(),
                                    taskConfig.getTargetTable());

                            // 第二步 创建新表 create table 表名称 like
                            // 表名称+时间戳（yyyyMMddHHmmss）
                            StringBuilder createTable = new StringBuilder();
                            createTable.append("create table if not exists ");
                            createTable.append(taskConfig.getTargetTable());
                            createTable.append(" like ");
                            createTable.append(strNewTableName);
                            message = String.format(
                                    "修复数据 第二步：创建新表：【sourceTableName：%s】执行的sql语句：%s 【groupName：%s】【triggerName：%s】",
                                    taskConfig.getSourceTable(), createTable.toString(), groupName, taskName);
                            logger.info(message);
                            HiveUtils.execUpdate(taskConfig.getTargetDbEntity(), taskConfig, "", createTable.toString(),
                                    30);
                            // 第三步 向新表中插入数据
                            StringBuilder insertData = new StringBuilder();
                            insertData.append("insert into table ");
                            insertData.append(taskConfig.getTargetTable());
                            insertData.append(" partition (");
                            insertData.append(HiveDefinePartNameEnum.PARTITION_DATE_NAME.getValue());
                            insertData.append(" ) select ");
                            String selectColumnsStr = taskConfig.getSelectColumnsStr()
                                    .replace("now() as src_update_time", " src_update_time");
                            insertData.append(selectColumnsStr);
                            insertData.append(" ,cast(substring(regexp_replace(");
                            insertData.append(taskConfig.getSyncTimeColumnStr());
                            insertData.append(", '-', ''),1,8) as int) as partition_date");
                            insertData.append(" from ");
                            insertData.append(strNewTableName);
                            message = String.format(
                                    "修复数据 第三步：向新表中插入数据：【newTableName：%s】执行的sql语句：%s 【groupName：%s】【triggerName：%s】",
                                    strNewTableName, insertData.toString(), groupName, taskName);
                            logger.info(message);
                            HiveUtils.execUpdate(taskConfig.getTargetDbEntity(), taskConfig, hiveJobNamePref, insertData.toString(),
                                    30);

                            //同步元数据到Impala Catalog
                            HiveUtils.syncMetaData4Impala(insertData.toString()
                                    , taskConfig.getTargetDbEntity().getDbName(),
                                    taskConfig.getTargetTable());


                            message = String.format("修复数据 第四步：将业务库的数据插入到新表：【groupName：%s】【triggerName：%s】", groupName,
                                    taskName);
                            logger.info(message);
                        }

                        int partition = getHivePartitionValue(syncBeginTime);
                        StringBuilder sbBuilder = new StringBuilder();

                        // 当同步目标表为parquet类型时，采用txt类型表先做临时存储
                        if (taskConfig.getSyncType().equals(SyncTypeEnum.SYNC_TYPE_1.getValue())) {
                            // 当txt表不存在时，则创建
                            sbBuilder = new StringBuilder();
                            sbBuilder.append("create table if not exists ");
                            sbBuilder.append(taskConfig.getTargetTable());
                            sbBuilder.append(HiveDefinePartNameEnum.TMP_TABLE_NAME_SUBFIX.getValue());
                            sbBuilder
                                    .append(new SimpleDateFormat(DateFormatStrEnum.PARTITION_DATE_YYYY_MM_DD.getValue())
                                            .format(syncBeginTime));
                            sbBuilder.append(" like ");
                            sbBuilder.append(taskConfig.getTargetTable());
                            sbBuilder.append(" stored as textfile ");
                            message = String.format("开始尝试创建txt类型的表【%s】【groupName：%s】【triggerName：%s】sql:【%s】",
                                    taskConfig.getTargetTable(), groupName, taskName, sbBuilder.toString());
                            logger.info(message);
                            HiveUtils.execUpdate(taskConfig.getTargetDbEntity(), taskConfig, hiveJobNamePref, sbBuilder.toString(),
                                    30);

                            // 清空txt类型表的分区数据
                            sbBuilder = new StringBuilder();
                            sbBuilder.append("ALTER TABLE ");
                            sbBuilder.append(taskConfig.getTargetTable());
                            sbBuilder.append(HiveDefinePartNameEnum.TMP_TABLE_NAME_SUBFIX.getValue());
                            sbBuilder
                                    .append(new SimpleDateFormat(DateFormatStrEnum.PARTITION_DATE_YYYY_MM_DD.getValue())
                                            .format(syncBeginTime));
                            sbBuilder.append(" DROP IF EXISTS partition (");
                            sbBuilder.append(HiveDefinePartNameEnum.PARTITION_DATE_NAME.getValue());
                            sbBuilder.append("=");
                            sbBuilder.append(partition);
                            sbBuilder.append(") purge ");
                            message = String.format("开始格式化txt类型表的Partition【%d】【groupName：%s】【triggerName：%s】sql:【%s】",
                                    partition, groupName, taskName, sbBuilder.toString());
                            logger.info(message);
                            HiveUtils.execUpdate(taskConfig.getTargetDbEntity(), taskConfig, hiveJobNamePref, sbBuilder.toString(),
                                    30);
                            //同步元数据到Impala Catalog
                            HiveUtils.syncMetaData4Impala(sbBuilder.toString()
                                    , taskConfig.getTargetDbEntity().getDbName(),
                                    taskConfig.getTargetTable());

                        }
                        sbBuilder = new StringBuilder();
                        sbBuilder.append("ALTER TABLE ");
                        sbBuilder.append(taskConfig.getTargetTable());
                        sbBuilder.append(" DROP IF EXISTS partition (");
                        sbBuilder.append(HiveDefinePartNameEnum.PARTITION_DATE_NAME.getValue());
                        sbBuilder.append("=");
                        sbBuilder.append(partition);
                        sbBuilder.append(") purge ");
                        message = String.format("开始格式化Partition【%d】【groupName：%s】【triggerName：%s】sql:【%s】", partition,
                                groupName, taskName, sbBuilder.toString());
                        logger.info(message);
                        HiveUtils.execUpdate(taskConfig.getTargetDbEntity(), taskConfig, hiveJobNamePref, sbBuilder.toString(), 30);
                        message = String.format("完成对Partition【%d】的格式化!【groupName：%s】【triggerName：%s】", partition,
                                groupName, taskName);
                        logger.info(message);
                        //同步元数据到Impala Catalog
                        HiveUtils.syncMetaData4Impala(sbBuilder.toString()
                                , taskConfig.getTargetDbEntity().getDbName(),
                                taskConfig.getTargetTable());
                    }
//                    3. 初始化任务，构造sqoop参数
                    return getIncreamentSqoopParams(taskConfig, syncBeginTime, syncEndTime, jobId);

                } else {
                    message = String
                            .format("【groupName：%s】【triggerName：%s】导入出现异常,既不是全量导入，也缺失增量导入所需的条件：", groupName, taskName);
                    ManagerException managerException = new ManagerException(message);
                    jobBizStatusEnum = JobBizStatusEnum.INTERRUPTED;
                    SendMsg2AMQ.updateStatusAndSendMsg(jobId, jobBizStatusEnum, jmsClusterMgr,
                            ExceptionUtil.getStackTrace(managerException));
                    logger.error(managerException.getMessage(), managerException);
                    return null;
                }
            } else {
                message = String.format("【groupName：%s】【triggerName：%s】获取对应的xml 配置文件失败！", groupName, taskName);
                ManagerException managerException = new ManagerException(message);
                jobBizStatusEnum = JobBizStatusEnum.INTERRUPTED;
                SendMsg2AMQ.updateStatusAndSendMsg(jobId, jobBizStatusEnum, jmsClusterMgr,
                        ExceptionUtil.getStackTrace(managerException));
//                throw new ManagerException(message);

                return null;
            }
        } catch (Throwable e) {
            message = String.format("【groupName：%s】【triggerName：%s】获取对应的xml 配置文件失败！", groupName, taskName);
            jobBizStatusEnum = JobBizStatusEnum.INTERRUPTED;
            SendMsg2AMQ.updateStatusAndSendMsg(jobId, jobBizStatusEnum, jmsClusterMgr,
                    message + "\n" + ExceptionUtil.getStackTrace(e));
            logger.error("error in taskInitMethod...: " + e.getMessage());
            return null;
        }
    }

    /**
     * 任务执行过程
     *
     * @param jobId
     * @throws Exception
     */
    private void taskExcuteMethod(String jobId, String taskName, String groupName,
                                  ClientTaskStatusLog clientTaskStatusLog, SqoopParams sqoopParams, Date syncBeginTime, Date syncEndTime,
                                  TaskPropertiesConfig taskConfig) throws ManagerException {

        JobBizStatusEnum jobBizStatusEnum = JobBizStatusEnum.RUNNING;
        String runMsg = "运行中";
        addTaskLog(jobId, taskName, groupName, jobBizStatusEnum, runMsg);
        SendMsg2AMQ.updateStatusAndSendMsg(jobId, jobBizStatusEnum, jmsClusterMgr, runMsg);
        JobExecutionResult jobExecutionResult = null;

        // 4.执行sqoop命令
        logger.info("begin to prepared sqoop params.............");
        String command = sqoopParams.getCommand();
        Map<String, String> paras = sqoopParams.getParas();
        List<String> options = sqoopParams.getOptions();
        Map<String, String> sqoopProperties = sqoopParams.getProperties();
        logger.info("get sqoop params successful.............");

        try {
            logger.info("------开始调用sqoopApi接口-----");
            SqoopApi sqoopApi = SqoopApi.getSqoopApi();
//            Map<String, String> sqoopProperties = HiveUtils.generateSqoopProperties(importParams);
            logger.info("--------------------------------任务名称为..." + sqoopProperties);
            logger.info(String.format("!!!导入任务开始执行，任务名称【%s】"
                    , sqoopParams.getProperties().get("mapred.job.name")));
//            jobExecutionResult = sqoopApi.execute(jobId, command, paras, options);
            jobExecutionResult = sqoopApi.execute(jobId, command, paras, options, sqoopProperties);
            logger.info("------完成调用sqoopApi接口-----");
        } catch (MaxHadoopJobRequestsException e) {
            ManagerException managerException = new ManagerException(
                    String.format("import job runs into max job request error for sqoop job '%s'", jobId), e);
            jobBizStatusEnum = JobBizStatusEnum.INTERRUPTED;
            SendMsg2AMQ.updateStatusAndSendMsg(jobId, jobBizStatusEnum, jmsClusterMgr, ExceptionUtil.getStackTrace(managerException));
            logger.error(managerException.getMessage(), managerException);
//            throw new ManagerException(
//                    String.format("import job runs into max job request error for sqoop job '%s'", jobId), e);
        } catch (Throwable t) {
            String msg = String.format("import job runs into unknown error for sqoop job '%s'", jobId);
            jobBizStatusEnum = JobBizStatusEnum.INTERRUPTED;
            SendMsg2AMQ.updateStatusAndSendMsg(jobId, jobBizStatusEnum, jmsClusterMgr,
                    msg + "\n" + ExceptionUtil.getStackTrace(t));
//            throw new ManagerException(msg, t);
            logger.error(t.getMessage(), t);
        }

        if (jobExecutionResult == null) {
            ManagerException managerException = new ManagerException("sqoop没有反馈执行结果.....");
            jobBizStatusEnum = JobBizStatusEnum.INTERRUPTED;
            SendMsg2AMQ.updateStatusAndSendMsg(jobId, jobBizStatusEnum, jmsClusterMgr,
                    ExceptionUtil.getStackTrace(managerException
                    ));
            logger.error(managerException.getMessage(), managerException);
        } else if (jobExecutionResult.isSucceed()) {

            if (taskConfig.getSyncType().equals(SyncTypeEnum.SYNC_TYPE_1.getValue())) {
                StringBuffer sbuBuffer = new StringBuffer();
                try {
                    // 转移txt表的数据至parquet中
                    sbuBuffer.append("insert into table ");
                    sbuBuffer.append(taskConfig.getTargetTable());
                    sbuBuffer.append(" partition(");
                    sbuBuffer.append(HiveDefinePartNameEnum.PARTITION_DATE_NAME.getValue());
                    sbuBuffer.append(") select * from ");
                    sbuBuffer.append(taskConfig.getTargetTable());
                    sbuBuffer.append(HiveDefinePartNameEnum.TMP_TABLE_NAME_SUBFIX.getValue());
                    sbuBuffer.append(new SimpleDateFormat(DateFormatStrEnum.PARTITION_DATE_YYYY_MM_DD.getValue())
                            .format(syncBeginTime));
                    logger.info(String.format("转移txt表的数据至parquet中的sql【%s】", sbuBuffer.toString()));
                    HiveUtils.execUpdate(taskConfig.getTargetDbEntity(), taskConfig, "", sbuBuffer.toString(), 30);

                    //同步元数据到Impala Catalog
                    HiveUtils.syncMetaData4Impala(sbuBuffer.toString()
                            , taskConfig.getTargetDbEntity().getDbName(),
                            taskConfig.getTargetTable());

                } catch (Exception e) {
                    ManagerException managerException = new ManagerException("数据从txt类型的表转移至parquet失败.....");
                    jobBizStatusEnum = JobBizStatusEnum.INTERRUPTED;
                    SendMsg2AMQ.updateStatusAndSendMsg(jobId, jobBizStatusEnum, jmsClusterMgr,
                            ExceptionUtil.getStackTrace(managerException));
                } finally {
                    try {
                        // 删除临时的txt表
                        sbuBuffer = new StringBuffer();
                        sbuBuffer.append("drop table if exists ");
                        sbuBuffer.append(taskConfig.getTargetTable());
                        sbuBuffer.append(HiveDefinePartNameEnum.TMP_TABLE_NAME_SUBFIX.getValue());
                        sbuBuffer.append(new SimpleDateFormat(DateFormatStrEnum.PARTITION_DATE_YYYY_MM_DD.getValue())
                                .format(syncBeginTime));
                        HiveUtils.execUpdate(taskConfig.getTargetDbEntity(), taskConfig, "", sbuBuffer.toString(), 30);
                    } catch (Exception e) {
                        ManagerException managerException = new ManagerException("删除临时表失败.....");
                        jobBizStatusEnum = JobBizStatusEnum.INTERRUPTED;
                        SendMsg2AMQ.updateStatusAndSendMsg(jobId, jobBizStatusEnum, jmsClusterMgr,
                                ExceptionUtil.getStackTrace(managerException));
                    }
                }
            }
            logger.info(String.format("导入任务【%s】执行完成!!!"
                    , sqoopParams.getProperties().get("mapred.job.name")));
            jobBizStatusEnum = JobBizStatusEnum.FINISHED;
            String sucMsg = "执行sqoop导入命令成功:" + String.format("【groupName=%s】【schedulerName=%s】！", groupName, taskName);
            logger.info(sucMsg);
            addTaskLog(jobId, taskName, groupName, jobBizStatusEnum, sucMsg, syncBeginTime, syncEndTime);
        } else if (JobStatus.Repeat == jobExecutionResult.getJobStatus()) {
            // 如果导入命令出现jobId重复
            ManagerException managerException = new ManagerException(jobExecutionResult.getErrorMessage());
            jobBizStatusEnum = JobBizStatusEnum.STOPED;
            SendMsg2AMQ.updateStatusAndSendMsg(jobId, jobBizStatusEnum, jmsClusterMgr,
                    jobExecutionResult.getErrorMessage() + "\n" + ExceptionUtil.getStackTrace(managerException));
            logger.error(managerException.getMessage(), managerException);
        } else {
            ManagerException managerException = new ManagerException(jobExecutionResult.getErrorMessage());
            // 如果导入命令执行失败
            jobBizStatusEnum = JobBizStatusEnum.STOPED;
            SendMsg2AMQ.updateStatusAndSendMsg(jobId, jobBizStatusEnum, jmsClusterMgr,
                    jobExecutionResult.getErrorMessage() + "\n" + ExceptionUtil.getStackTrace(managerException));
            logger.error(managerException.getMessage(), managerException);
        }

        // 成功后，需要传mq并删除缓存
        SendMsg2AMQ.updateStatusAndSendMsg(jobId, jobBizStatusEnum, jmsClusterMgr, "");
        RinseStatusAndLogCache.removeTaskByJobId(jobId);
//        cleanAppInfo(sqoopParams);
    }

    private void cleanAppInfo(SqoopParams sqoopParams) {
        ApplicationInfo info = new ApplicationInfo();
        info.setAppName(sqoopParams.getProperties().get("mapred.job.name"));
        //任务成功后，删除数据库记录
        try {
            applicationInfoService.delByAppName(info);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // 根据结束时间创建分区字段
    private int getHivePartitionValue(Date syncEndTime) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        // Date hivePartitionTime = new Date(syncEndTime.getTime() - 1000);
        // String hivePartitionValueStr = sdf.format(hivePartitionTime);
        String hivePartitionValueStr = sdf.format(syncEndTime);
        int hivePartitionValue = Integer.parseInt(hivePartitionValueStr);
        return hivePartitionValue;

    }

    /**
     * 全量导入的sqoop参数赋予
     *
     * @param taskConfig
     * @param syncBeginTime
     * @param syncEndTime
     * @param jobId
     * @param remoteJobInvokeParamsDto
     * @return
     * @throws ManagerException
     */
    private SqoopParams getFullSqoopParams(TaskPropertiesConfig taskConfig, Date syncBeginTime, Date syncEndTime,
                                           String jobId, RemoteJobInvokeParamsDto remoteJobInvokeParamsDto) throws ManagerException {

        TaskDatabaseConfig sourceDbEntity = taskConfig.getSourceDbEntity();
        if (sourceDbEntity == null) {
            IllegalArgumentException iilegalError = new IllegalArgumentException("请检查调度任务数据库配置信息");
            JobBizStatusEnum jobBizStatusEnum = JobBizStatusEnum.STOPED;
            SendMsg2AMQ.updateStatusAndSendMsg(jobId, jobBizStatusEnum, jmsClusterMgr
                    , ExceptionUtil.getStackTrace(iilegalError));
            throw iilegalError;
        }

        Integer dbType = sourceDbEntity.getDbType();
        String schemaName = "";
        // 值拼接oracle
        if (dbType == DbTypeCollecEnum.ORACLE.getValue() || dbType == DbTypeCollecEnum.DB2.getValue()) {
            schemaName = sourceDbEntity.getSchemaName();
            if (StringUtils.isBlank(schemaName)) {
                schemaName = "";
            } else {
                schemaName += ".";
            }

        }

        SqoopParams sqoopParams = new SqoopParams();
        Map<String, String> params = new LinkedHashMap<String, String>();
        String filterConditions = " ";
        if (taskConfig.getFilterConditions() != null) {
            StringBuffer filters = new StringBuffer();
            for (String filter : taskConfig.getFilterConditions()) {
                filters.append(" ");
                filters.append(filter);
                filters.append(" ");
            }
            filterConditions = filters.toString();
        }

        // 拼接查询sql
        StringBuilder sbBuilder = new StringBuilder();
        sbBuilder.append("select ");
        sbBuilder.append(taskConfig.getSelectColumnsStr());
        sbBuilder.append(" from ");
        sbBuilder.append(schemaName + taskConfig.getSourceTable());
        sbBuilder.append(" t where 1=1 ");
        sbBuilder.append(filterConditions);
        sbBuilder.append(" and $CONDITIONS ");

        sqoopParams.setCommand("import");
        params.put("--connect", sourceDbEntity.getConnectionUrl());
        params.put("--username", taskConfig.getSourceDbEntity().getUserName());
        params.put("--password", taskConfig.getSourceDbEntity().getPassword());
        params.put("--query", sbBuilder.toString());
        // 全量导入的map数只能是 1
        String mapNum = "1";

        params.put("-m", mapNum);
        String message = "全量表导入的Map数：";
        logger.info(String.format("【%s】 组名【%s】调度名【%s】 全量导入map数【%s】", message, taskConfig.getGroupName(),
                taskConfig.getTriggerName(), mapNum));
        // 特殊转换字段处理
        List<String> specialColumnTypeList = taskConfig.getSpecialColumnTypeList();
        if (specialColumnTypeList != null && specialColumnTypeList.size() > 0) {
            String mapColumnHive = this.createMapColumnHive(taskConfig);
            params.put("--map-column-hive", mapColumnHive);
        }

        int hivePartitionValue = this.getHivePartitionValue(syncEndTime);
        logger.info("		importParams:" + importParams);
        HiveUtils.setSqoopParamProperties(taskConfig, params, importParams, sqoopParams);
        params.put("--class-name", "import_" + taskConfig.getGroupName() + "_" + taskConfig.getSourceTable() + "_"
                + String.valueOf(hivePartitionValue));
        params.put("--fields-terminated-by", fieldsTermBy);
        params.put("--lines-terminated-by", linesTermBy);
        params.put("--hive-database", taskConfig.getTargetDbEntity().getDbName());
        // 如果是parquet类型，则需要先导入到txt后，再转移到parquet表
        if (taskConfig.getSyncType().equals(SyncTypeEnum.SYNC_TYPE_1.getValue())) {
            params.put("--hive-table",
                    taskConfig.getTargetTable() + HiveDefinePartNameEnum.TMP_TABLE_NAME_SUBFIX.getValue()
                            + new SimpleDateFormat(DateFormatStrEnum.PARTITION_DATE_YYYY_MM_DD.getValue())
                            .format(syncBeginTime));
        } else {
            // 默认为txt类型
            params.put("--hive-table", taskConfig.getTargetTable());
        }
        params.put("--hive-partition-key", HiveDefinePartNameEnum.PARTITION_DATE_NAME.getValue());
        params.put("--hive-partition-value", String.valueOf(hivePartitionValue));
        params.put("--null-string", nullString);
        params.put("--null-non-string", nullNotString);
        params.put("--hive-delims-replacement", " ");
        params.put("--target-dir", tmpDir + taskConfig.getSourceDbEntity().getDbName() + "/"
                + taskConfig.getSourceTable() + GernerateUuidUtils.getUUID());

        List<String> options = new ArrayList<String>();
        params.put("--bindir", bindir);
        params.put("--outdir", outdir);
        // options.add("--hive-overwrite");
        options.add("--hive-import");
        options.add("--delete-target-dir");

        sqoopParams.setParas(params);
        sqoopParams.setOptions(options);
        sqoopParams.setSyncBeginTime(syncBeginTime);
        sqoopParams.setSyncEndTime(syncEndTime);
        return sqoopParams;
    }

    /**
     * 单表增量导入情形
     */
    private SqoopParams getIncreamentSqoopParams(TaskPropertiesConfig taskConfig, Date syncBeginTime, Date syncEndTime,
                                                 String jobId) throws ManagerException {
        String message = "";
        TaskDatabaseConfig sourceDbEntity = taskConfig.getSourceDbEntity();
        int hivePartitionValue = this.getHivePartitionValue(syncBeginTime);
        if (sourceDbEntity == null) {
            message = "请检查调度任务数据库配置信息";
            IllegalArgumentException illegalArgumentException = new IllegalArgumentException(message);
            JobBizStatusEnum jobBizStatusEnum = JobBizStatusEnum.INTERRUPTED;
            SendMsg2AMQ.updateStatusAndSendMsg(jobId, jobBizStatusEnum, jmsClusterMgr,
                    ExceptionUtil.getStackTrace(illegalArgumentException));
            throw illegalArgumentException;
        }
        String sourceDbUrl = sourceDbEntity.getConnectionUrl();
        Integer dbType = sourceDbEntity.getDbType();
        String sourceDbUser = sourceDbEntity.getUserName();
        String sourceDbPass = sourceDbEntity.getPassword();
        String schemaName = "";
        // 值拼接oracle
        if (dbType == DbTypeCollecEnum.ORACLE.getValue() || dbType == DbTypeCollecEnum.DB2.getValue()) {
            schemaName = sourceDbEntity.getSchemaName();
            if (StringUtils.isBlank(schemaName)) {
                schemaName = "";
            } else {
                schemaName += ".";
            }

        }

        SqoopParams sqoopParams = new SqoopParams();
        String sqlStr = "";
        StringBuilder whereField = new StringBuilder();
        List<String> syncTimeColumn = taskConfig.getSyncTimeColumn();
        List<String> selectColumns = taskConfig.getSelectColumns();
        logger.info("begin hisSelectColumns1");
        List<String> hisSelectColumns = taskConfig.getHisSelectColumns();
        logger.info("end hisSelectColumns1" + hisSelectColumns.toString());
        String syncBeginTimeStr = DateUtils.formatDatetime(syncBeginTime, dateFormat);
        String syncEndTimeStr = DateUtils.formatDatetime(syncEndTime, dateFormat);
        String syncBeginDateStr = DateUtils.formatDatetime(syncBeginTime,
                DateFormatStrEnum.PARTITION_DATE_YYYY_MM_DD.getValue());
        String syncEndDateStr = DateUtils.formatDatetime(syncEndTime,
                DateFormatStrEnum.PARTITION_DATE_YYYY_MM_DD.getValue());
        // 如果调度的区间值都在同一天内，则换算成调度一天的数据
        if (syncEndDateStr.equals(syncBeginDateStr)) {
            syncEndDateStr = DateUtils.formatDatetime(DateUtils.getNextDay(syncEndTime),
                    DateFormatStrEnum.PARTITION_DATE_YYYY_MM_DD.getValue());
        }

        // 构造where条件子句
        StringBuffer whereFieldBuffer = new StringBuffer();
        // 构造增量条件
        StringBuffer incrementFieldBuffer = new StringBuffer();
        String syncTimeColumnName = null;
        if (syncTimeColumn.size() >= 1) {
            // 同步时间字段
            syncTimeColumnName = taskConfig.getSyncTimeColumnStr();
            if (dbType == DbTypeCollecEnum.ORACLE.getValue() || dbType == DbTypeCollecEnum.DB2.getValue()) {
                // 除时间字段外的日期格式的数据类型
                if (syncTimeColumnName.contains(SplitCharEnum.SPLIT_CHAR_1.getValue())) {
                    String[] columnNameStrs = syncTimeColumnName.split(SplitCharEnum.SPLIT_CHAR_1.getValue());
                    syncTimeColumnName = columnNameStrs[0];
                    String columnType = columnNameStrs[1];
                    if (columnType.toUpperCase().equals("STR")) {
                        incrementFieldBuffer.append(" where ").append(syncTimeColumnName).append(" >= '")
                                .append(syncBeginDateStr).append("' and ").append(syncTimeColumnName).append(" < '")
                                .append(syncEndDateStr).append("' ");
                    } else {
                        IllegalArgumentException illegalArgumentException = new IllegalArgumentException(message);
                        message = "导入不支持这种字段类型.......";
                        JobBizStatusEnum jobBizStatusEnum = JobBizStatusEnum.INTERRUPTED;
                        SendMsg2AMQ.updateStatusAndSendMsg(jobId, jobBizStatusEnum,
                                jmsClusterMgr, ExceptionUtil.getStackTrace(illegalArgumentException));
                        throw illegalArgumentException;
                    }
                } else {
                    incrementFieldBuffer.append(" where ").append(syncTimeColumnName).append(" >= to_date('")
                            .append(syncBeginTimeStr).append("','yyyy-mm-dd hh24:mi:ss') and ")
                            .append(syncTimeColumnName).append(" < to_date('").append(syncEndTimeStr)
                            .append("','yyyy-mm-dd hh24:mi:ss') ");
                }

            } else if (dbType == DbTypeCollecEnum.MYSQL.getValue()) {
                // 除时间字段外的日期格式的数据类型
                if (syncTimeColumnName.contains(SplitCharEnum.SPLIT_CHAR_1.getValue())) {
                    String[] columnNameStrs = syncTimeColumnName.split(SplitCharEnum.SPLIT_CHAR_1.getValue());
                    syncTimeColumnName = columnNameStrs[0];
                    String columnType = columnNameStrs[1];
                    if (columnType.toUpperCase().equals("STR")) {
                        incrementFieldBuffer.append(" where ").append(syncTimeColumnName).append(" >= '")
                                .append(syncBeginDateStr).append("' and ").append(syncTimeColumnName).append(" < '")
                                .append(syncEndDateStr).append("' ");
                    } else {
                        message = "导入不支持这种字段类型.......";
                        IllegalArgumentException illegalArgumentException = new IllegalArgumentException(message);
                        JobBizStatusEnum jobBizStatusEnum = JobBizStatusEnum.INTERRUPTED;
                        SendMsg2AMQ.updateStatusAndSendMsg(jobId, jobBizStatusEnum, jmsClusterMgr
                                , ExceptionUtil.getStackTrace(illegalArgumentException));
                        throw illegalArgumentException;
                    }
                } else {
                    incrementFieldBuffer.append(" where ").append(syncTimeColumnName).append(" >= str_to_date('")
                            .append(syncBeginTimeStr).append("','%Y-%m-%d %H:%i:%s') and ").append(syncTimeColumnName)
                            .append(" < str_to_date('").append(syncEndTimeStr).append("','%Y-%m-%d %H:%i:%s') ");
                }
            } else {
                message = "导入不支持这种数据库类型.......";
                IllegalArgumentException illegalArgumentException = new IllegalArgumentException(message);
                JobBizStatusEnum jobBizStatusEnum = JobBizStatusEnum.INTERRUPTED;
                SendMsg2AMQ.updateStatusAndSendMsg(jobId, jobBizStatusEnum, jmsClusterMgr,
                        ExceptionUtil.getStackTrace(illegalArgumentException));
                throw illegalArgumentException;
            }
        } else {
            IllegalArgumentException illegalArgumentException = new IllegalArgumentException(message);
            message = "时间戳字段个数存在问题.......";
            JobBizStatusEnum jobBizStatusEnum = JobBizStatusEnum.INTERRUPTED;
            SendMsg2AMQ.updateStatusAndSendMsg(jobId, jobBizStatusEnum, jmsClusterMgr,
                    ExceptionUtil.getStackTrace(illegalArgumentException));
            throw illegalArgumentException;
        }
        whereFieldBuffer.append(incrementFieldBuffer);

        // 拼接查询字段（不允许使用 * 来查询所有字段）
        String selectField = "";
        if (selectColumns == null || selectColumns.size() <= 0) {
            IllegalArgumentException illegalArgumentException = new IllegalArgumentException("查询列表不能为空.....");
            JobBizStatusEnum jobBizStatusEnum = JobBizStatusEnum.INTERRUPTED;
            SendMsg2AMQ.updateStatusAndSendMsg(jobId, jobBizStatusEnum, jmsClusterMgr,
                    ExceptionUtil.getStackTrace(illegalArgumentException));
            throw illegalArgumentException;
        } else {
            StringBuffer selectFildBuffer = new StringBuffer();
            for (int i = 0; i < selectColumns.size(); i++) {
                if (i == selectColumns.size() - 1) {
                    selectFildBuffer.append(selectColumns.get(i));
                } else {
                    selectFildBuffer.append(selectColumns.get(i));
                    selectFildBuffer.append(",");
                }
            }
            selectField = selectFildBuffer.toString();
        }

        String hisSelectField = "";
        if (hisSelectColumns != null || hisSelectColumns.size() > 0) {
            StringBuffer selectFildBuffer = new StringBuffer();
            for (int i = 0; i < hisSelectColumns.size(); i++) {
                if (i == hisSelectColumns.size() - 1) {
                    selectFildBuffer.append(hisSelectColumns.get(i));
                } else {
                    selectFildBuffer.append(hisSelectColumns.get(i));
                    selectFildBuffer.append(",");
                }
            }
            hisSelectField = selectFildBuffer.toString();
        }
        // 增加过滤条件（非特殊情况，导入环节不进行任何过滤）
        String filterConditions = " ";
        if (taskConfig.getFilterConditions() != null) {
            StringBuffer filters = new StringBuffer();

            for (String filter : taskConfig.getFilterConditions()) {
                if (filter.trim().toLowerCase().startsWith("and")) {
                    filters.append(" ");
                } else {
                    filters.append(" and ");
                }
                filters.append(filter);
                filters.append(" ");
            }
            filterConditions = filters.toString();
        }
        whereFieldBuffer.append(filterConditions);

        StringBuffer hisSourceTable = new StringBuffer("");

        logger.info("=====thie partiton is:" + hivePartitionValue);
        if (taskConfig.hasPrimaryKey(hivePartitionValue) && taskConfig.getHisSourceTable() != null
                && taskConfig.getHisSourceTable().length() > 0) {
            hisSourceTable.append(" union all ");
            hisSourceTable.append("select ");
            if (hisSelectField == null || hisSelectField.length() == 0 || hisSelectField.isEmpty()) {
                hisSourceTable.append(selectField);
            } else {
                hisSourceTable.append(hisSelectField);
            }
            hisSourceTable.append(" from ");
            hisSourceTable.append(schemaName);
            hisSourceTable.append(taskConfig.getHisSourceTable());
            hisSourceTable.append(incrementFieldBuffer);
        }
        whereFieldBuffer.append(hisSourceTable);
        whereFieldBuffer.append(" and $CONDITIONS");
        whereField.append(whereFieldBuffer);

        // 拼接查询Sql字符串

        sqlStr = new StringBuilder().append("select ").append(selectField).append(" from ")
                .append(schemaName + taskConfig.getSourceTable()).append(" t").append(whereField).toString();

        // 构造sqoop命令
        Map<String, String> params = new LinkedHashMap<String, String>();
        params.put("--connect", sourceDbUrl);
        params.put("--username", sourceDbUser);
        params.put("--password", sourceDbPass);
        params.put("--query", sqlStr);
        String mapNumIm = this.mapNumIm;
        logger.info("		importParams:" + importParams);
        HiveUtils.setSqoopParamProperties(taskConfig, params, importParams, sqoopParams);
        message = "导入增量表设置map 数量：";
        if (taskConfig.getImportMapSize() != null) {
            mapNumIm = taskConfig.getImportMapSize();
            logger.info(String.format("从对应%s-%s.xml文件中读取信息：%s 【%s】", taskConfig.getGroupName(),
                    taskConfig.getTriggerName(), message, mapNumIm));
        } else {
            logger.info(String.format("从对应dc-client.properties文件中读取信息：%s 导入map数【%s】", message, mapNumIm));
        }
        // 简单的分页机制（一条sql被分成多条来执行，缺陷：当时间非常集中时，难以拆分）
        String boundaryQuerySql = "select " + "min(" + syncTimeColumnName + ")," + "max(" + syncTimeColumnName + ")"
                + " from " + schemaName + taskConfig.getSourceTable()
                + whereField.toString().replace("and $CONDITIONS", "").replace(hisSourceTable, "");
        if (taskConfig.hasPrimaryKey(hivePartitionValue) && taskConfig.getHisSourceTable() != null) {
            mapNumIm = String.valueOf("1");
            logger.info(String.format("该%s-%s.xml文件中配有历史表【%s】，因此不应该含有--boundary-query 并且map 设置为【%s】",
                    taskConfig.getGroupName(), taskConfig.getTriggerName(), taskConfig.getHisSourceTable(), mapNumIm));
        } else {
            // 当m>1时，才需要执行boundary-query和split-by
            if (Integer.valueOf(mapNumIm) > 1) {
                params.put("--boundary-query", boundaryQuerySql);
                params.put("--split-by", syncTimeColumnName);
            }
        }

        params.put("-m", mapNumIm);

        // 特殊转换字段处理（RDBMS的sql类型转换为hive的sql类型）
        List<String> specialColumnTypeList = taskConfig.getSpecialColumnTypeList();
        if (specialColumnTypeList != null && specialColumnTypeList.size() > 0) {
            String mapColumnHive = this.createMapColumnHive(taskConfig);
            params.put("--map-column-hive", mapColumnHive);
        }

        params.put("--class-name", "import_" + taskConfig.getGroupName() + "_" + taskConfig.getSourceTable() + "_"
                + String.valueOf(hivePartitionValue));
        params.put("--fields-terminated-by", fieldsTermBy);
        params.put("--lines-terminated-by", linesTermBy);
        params.put("--hive-database", taskConfig.getTargetDbEntity().getDbName());
        // 如果是parquet类型，则需要先导入到txt后，再转移到parquet表
        if (taskConfig.getSyncType().equals(SyncTypeEnum.SYNC_TYPE_1.getValue())) {
            params.put("--hive-table",
                    taskConfig.getTargetTable() + HiveDefinePartNameEnum.TMP_TABLE_NAME_SUBFIX.getValue()
                            + new SimpleDateFormat(DateFormatStrEnum.PARTITION_DATE_YYYY_MM_DD.getValue())
                            .format(syncBeginTime));
        } else {
            // 默认为txt类型
            params.put("--hive-table", taskConfig.getTargetTable());
        }

        params.put("--hive-partition-key", HiveDefinePartNameEnum.PARTITION_DATE_NAME.getValue());
        params.put("--hive-partition-value", String.valueOf(hivePartitionValue));
        params.put("--null-string", nullString);
        params.put("--null-non-string", nullNotString);
        params.put("--hive-delims-replacement", " ");
        params.put("--target-dir", tmpDir + taskConfig.getSourceDbEntity().getDbName() + "/"
                + taskConfig.getSourceTable() + GernerateUuidUtils.getUUID());
        // modify by wang.w 生成的java和class文件统一放到一个目录下，方便管理
        params.put("--bindir", bindir);
        params.put("--outdir", outdir);

        List<String> optionsList = new ArrayList<String>();
        optionsList.add("--hive-import");
        optionsList.add("--delete-target-dir");
        sqoopParams.setCommand("import");
        sqoopParams.setParas(params);
        sqoopParams.setOptions(optionsList);
        sqoopParams.setSyncBeginTime(syncBeginTime);
        sqoopParams.setSyncEndTime(syncEndTime);
        logger.info(String.format("sqoop组装的参数如下：key-->【%s】-values-->【%s】", params.keySet(), params.values()));
        return sqoopParams;

    }

    /**
     * 构造特殊字段映射配置信息
     */
    private String createMapColumnHive(TaskPropertiesConfig taskConfig) {
        List<String> specialColumnTypeList = taskConfig.getSpecialColumnTypeList();
        StringBuffer mapColumnHive = new StringBuffer();
        for (String sct : specialColumnTypeList) {
            mapColumnHive.append(sct);
            mapColumnHive.append(",");
        }
        mapColumnHive.deleteCharAt(mapColumnHive.lastIndexOf(","));
        return mapColumnHive.toString();
    }

    /**
     * 结束时间要与当前时间进行判断 当结束时间超过了当前时间或者小于当前时间减去一个周期的时间，则使用当前时间作为结束时间
     */
    public Date makeSysEndTime(Date taskEndTime, Date now, int taskFreq) {
        if (taskEndTime.after(now) || taskEndTime.before(new Date(now.getTime() - (taskFreq) * 1000 / 2))) {
            return now;
        } else {
            return taskEndTime;
        }
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
        rD.addParam("startTime", "2018-06-28 00:00:00");
        rD.addParam("endTime", "2018-06-29 00:00:00");
        rD.addParam("instanceId", "1111111");
        rD.addParam("subInstanceId", "11122");
        executeJobWithParams(GernerateUuidUtils.getUUID(), triggerName, groupName, rD);
    }

    public String executeJobWithParamsForTest(String jobId, String triggerName, String groupName,
                                              RemoteJobInvokeParamsDto remoteJobInvokeParamsDto) throws Exception {
        /**
         * 调度执行状态
         */
        String cmdStr = null;

        return cmdStr;
    }

    private void saveAppInfo(RemoteJobInvokeParamsDto remoteJobInvokeParamsDto,
                             SqoopParams sqoopParams, String jobId) {
        String instanceId = remoteJobInvokeParamsDto.getParam("instanceId");
        String subInstanceId = remoteJobInvokeParamsDto.getParam("subInstanceId");
        Map<String, String> res = new HashMap<String, String>();
        res.put("scheduleId", jobId);
        res.put("taskParentId", instanceId);
        res.put("taskSonId", subInstanceId);
        res.put("mapred.job.name", sqoopParams.getProperties().get("mapred.job.name"));
        try {
            applicationInfoService.insertOrUpdateApplicationInfo(res);
        } catch (Exception e) {
            String msg = "保存app信息异常...";
            logger.error(msg, e);
            return;
        }
    }

    class DataLoadingTaskImplThread implements Runnable {
        private final Logger logger = Logger.getLogger(DataLoadingTaskImplThread.class);
        private String jobId;
        private String taskName;
        private String groupName;
        private RemoteJobInvokeParamsDto remoteJobInvokeParamsDto;

        public DataLoadingTaskImplThread(String jobId, String taskName,
                                         String groupName, RemoteJobInvokeParamsDto remoteJobInvokeParamsDto) {
            this.jobId = jobId;
            this.taskName = taskName;
            this.groupName = groupName;
            this.remoteJobInvokeParamsDto = remoteJobInvokeParamsDto;
        }

        @Override
        public void run() {
            logger.info("执行DataLoadingTaskImpl任务，调度参数为jobId=【" + jobId + "】taskName=【" + taskName + "】groupName=【"
                    + groupName + "】remoteJobInvokeParamsDto=【" + new Gson().toJson(remoteJobInvokeParamsDto) + "】");
            //保存任务信息
            try {
                CommUtil.saveJobInfo(resumeHiveTaskInfoService, jobId, taskName, groupName,
                        remoteJobInvokeParamsDto, DataLoadingTaskImpl.class);
            } catch (Exception e) {
                String message = "保存任务信息失败";
                logger.error(message, e);
            }
            JobBizStatusEnum jobBizStatusEnum;
            Date syncEndTime = null;
            Date syncBeginTime = null;
            Integer isRepair = 0;
            String parentJobIdStr = "";
            // 如果重复调用，则忽略本次请求
            jobId = jobId.trim();
            JobBizStatusEnum jobStauts = RinseStatusAndLogCache.getTaskStatusByJobId(jobId);
            if (jobStauts != null && !jobStauts.name().equals(JobBizStatusEnum.INTERRUPTED.name())) {
                logger.info(String.format("【jobId为：%s】的任务被重复调用", jobId));
                return;
            }
            if (remoteJobInvokeParamsDto != null) {
                String startTimeStr = remoteJobInvokeParamsDto.getParam("startTime");
                String endTimeStr = remoteJobInvokeParamsDto.getParam("endTime");
                String isRepairStr = remoteJobInvokeParamsDto.getParam("isRepair");
                String parentJobId = remoteJobInvokeParamsDto.getParam("jobInstanceId");
                logger.info(String.format("isRepairStr：%s jobInstanceId:%s 【groupName：%s】【triggerName：%s】", isRepairStr,
                        parentJobId, groupName, taskName));
                if (isRepairStr == null || isRepairStr.isEmpty() || isRepairStr.length() == 0) {
                    isRepair = 0;
                } else {
                    isRepair = Integer.valueOf(remoteJobInvokeParamsDto.getParam("isRepair"));
                }
                if (parentJobId == null || parentJobId.isEmpty() || parentJobId.length() == 0) {
                    parentJobIdStr = "";
                    logger.info(
                            String.format("parentJobId is null or isEmpty parentJobIdStr:%s 【groupName：%s】【triggerName：%s】",
                                    parentJobIdStr, groupName, taskName));
                } else {
                    parentJobIdStr = parentJobId;
                    logger.info(String.format(
                            "parentJobId is not null or isEmpty parentJobIdStr:%s 【groupName：%s】【triggerName：%s】",
                            parentJobIdStr, groupName, taskName));
                }
                String key = groupName + "@" + taskName;
                if (RepairByParentsId.containsKey(key)) {
                    ParentsJobId paraentParam = RepairByParentsId.get(key);
                    paraentParam.setPrevJobId(parentJobIdStr);
                    logger.info(String.format("has key then RepairByParentsId[%s] = %s", key,
                            RepairByParentsId.get(key).getPrevJobId()));
                } else {
                    ParentsJobId paraentParam = new ParentsJobId();
                    paraentParam.setPrevJobId(parentJobIdStr);
                    RepairByParentsId.put(key, paraentParam);
                    logger.info(String.format("new RepairByParentsId[%s] = %s", key,
                            RepairByParentsId.get(key).getPrevJobId()));
                }
                SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
                if (startTimeStr != null && endTimeStr != null) {
                    logger.info(String.format("【jobId为：%s】的任务被调用,", jobId) + "开始时间:" + startTimeStr + ";" + "结束时间:"
                            + endTimeStr + ".");
                    try {
                        syncBeginTime = sdf.parse(startTimeStr);
                        syncEndTime = sdf.parse(endTimeStr);
                    } catch (ParseException e) {
                        e.printStackTrace();
                        logger.error(e);
                        return;
                    }

                    if (syncEndTime.after(DateUtils.getNextDay(syncBeginTime)) || !syncEndTime.after(syncBeginTime)) {
                        String message = String.format("【jobId为：%s】的任务被调用,", jobId) + "开始时间:" + startTimeStr + "超过"
                                + "结束时间:" + endTimeStr + "一天或者调度结束时间在开始时间之前";
                        logger.error(message);
                        jobBizStatusEnum = JobBizStatusEnum.INTERRUPTED;
                        // 任务状态日志入库
                        addTaskLog(jobId, taskName, groupName, jobBizStatusEnum, message);
                        SendMsg2AMQ.updateStatusAndSendMsg(jobId, jobBizStatusEnum, jmsClusterMgr, message);
                        RinseStatusAndLogCache.removeTaskByJobId(jobId);
                        return;
                    }
                } else {
                    String message = String.format("【jobId为：%s】的任务被调用,", jobId) + "传入的开始和结束时间为空.";
                    logger.error(message);
                    jobBizStatusEnum = JobBizStatusEnum.INTERRUPTED;
                    // 任务状态日志入库
                    addTaskLog(jobId, taskName, groupName, jobBizStatusEnum, message);
                    SendMsg2AMQ.updateStatusAndSendMsg(jobId, jobBizStatusEnum, jmsClusterMgr, message);
                    RinseStatusAndLogCache.removeTaskByJobId(jobId);
                    return;
                }

            } else {
                String message = String.format("【jobId为：%s】的任务被调用,", jobId) + "传入的参数为空.";
                logger.error(message);
                jobBizStatusEnum = JobBizStatusEnum.INTERRUPTED;
                // 任务状态日志入库
                addTaskLog(jobId, taskName, groupName, jobBizStatusEnum, message);
                SendMsg2AMQ.updateStatusAndSendMsg(jobId, jobBizStatusEnum, jmsClusterMgr, message);
                RinseStatusAndLogCache.removeTaskByJobId(jobId);
                return;
            }

            try {
                taskName = taskName.trim();

                ClientTaskStatusLog clientTaskStatusLog = new ClientTaskStatusLog();
                // 初始化
                TaskPropertiesConfig taskConfig = ParseXMLFileUtil.getTaskConfig(groupName, taskName);

                SqoopParams sqoopParams = taskInitMethod(jobId, taskName, groupName, clientTaskStatusLog,
                        remoteJobInvokeParamsDto, syncBeginTime, syncEndTime, isRepair, parentJobIdStr, taskConfig);
                logger.info("==========>enter mq/sqoop sections");
                saveAppInfo(remoteJobInvokeParamsDto, sqoopParams, jobId);
                // 开始执行
                taskExcuteMethod(jobId, taskName, groupName, clientTaskStatusLog, sqoopParams, syncBeginTime, syncEndTime,
                        taskConfig);
                if (PropertyFile.getValue(MessageConstant.TRANSCATION_HISTORY_LOG, "")
                        .contains(groupName + "_" + taskName)) {
                    String message = String.format("导入完成！发现【%s_%s】配置在文件dc-client.properties的cdc.table.list 列表中，"
                            + "开始更新dml_type  != 0 的id_column_value值到缓存中", groupName, taskName);
                    logger.info(message);

                    if (taskConfig == null) {
                        jobBizStatusEnum = JobBizStatusEnum.INTERRUPTED;
                        message = "更新失败 updateTransactionHisLog";
                        logger.error(String.format("%s：【groupName：%s】【triggerName：%s】", message, groupName, taskName));
                        // 任务状态日志入库
                        addTaskLog(jobId, taskName, groupName, jobBizStatusEnum, message);
                        // 发送MQ
                        SendMsg2AMQ.updateStatusAndSendMsg(jobId, jobBizStatusEnum, jmsClusterMgr, message);
                        RinseStatusAndLogCache.removeTaskByJobId(jobId);
                        return;
                    } else {
                        HiveUtils.updateTransactionHisLog(taskConfig, null, syncBeginTime, syncEndTime, null, 6000, false);
                        message = String.format("【groupName：%s】【triggerName：%s】任务更新：%s", groupName, taskName,
                                groupName + "_" + taskName);
                        logger.info(message);
                    }
                }
                try {
                    CommUtil.delJobInfo(resumeHiveTaskInfoService, jobId);
                } catch (Exception e) {
                    String message = "删除任务信息失败";
                    logger.error(message, e);
                }
            } catch (Exception e) {
                // 异常处理
                jobBizStatusEnum = JobBizStatusEnum.INTERRUPTED;
                String errMsg = String.format("【groupName：%s】【triggerName：%s】导入出现异常：%s", groupName, taskName,
                        e.getMessage());
                logger.error(errMsg, e);
                addTaskLog(jobId, taskName, groupName, jobBizStatusEnum,
                        "执行sqoop命令:" + "中断:" + errMsg.substring(0, Math.min(errMsg.length(), 800)));
                RinseStatusAndLogCache.removeTaskByJobId(jobId);
                SendMsg2AMQ.updateStatusAndSendMsg(jobId, jobBizStatusEnum, jmsClusterMgr,
                        errMsg + "\n" + ExceptionUtil.getStackTrace(e));
                return;
            }
        }
    }

}
