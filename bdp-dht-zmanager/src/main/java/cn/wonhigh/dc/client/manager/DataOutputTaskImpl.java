package cn.wonhigh.dc.client.manager;

import cn.wonhigh.dc.client.common.constans.CommonEnumCollection;
import cn.wonhigh.dc.client.common.constans.CommonEnumCollection.DateFormatStrEnum;
import cn.wonhigh.dc.client.common.constans.CommonEnumCollection.HiveDefinePartNameEnum;
import cn.wonhigh.dc.client.common.constans.CommonEnumCollection.ParamNameEnum;
import cn.wonhigh.dc.client.common.constans.CommonEnumCollection.SyncTypeEnum;
import cn.wonhigh.dc.client.common.constans.MessageConstant;
import cn.wonhigh.dc.client.common.model.ClientTaskStatusLog;
import cn.wonhigh.dc.client.common.model.SqoopParams;
import cn.wonhigh.dc.client.common.model.TaskDatabaseConfig;
import cn.wonhigh.dc.client.common.model.TaskPropertiesConfig;
import cn.wonhigh.dc.client.common.util.*;
import cn.wonhigh.dc.client.manager.cache.RinseStatusAndLogCache;
import cn.wonhigh.dc.client.manager.jms.SendMsg2AMQ;
import cn.wonhigh.dc.client.service.ApplicationInfoService;
import cn.wonhigh.dc.client.service.ClientTaskStatusLogService;
import cn.wonhigh.dc.client.service.ResumeHiveTaskInfoService;
import cn.wonhigh.dc.client.service.jms.JmsClusterMgr;
import cn.wonhigh.dc.client.sqoop.JobExecutionResult;
import cn.wonhigh.dc.client.sqoop.SqoopApi;
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
import java.io.File;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 导出任务服务实现类
 *
 * @author wang.w
 * @version 1.0.0
 * @date 2015-3-23 上午11:23:35
 * @copyright wonhigh.cn
 */
@Service
@ManagedResource(objectName = DataOutputTaskImpl.MBEAN_NAME, description = "导出任务")
public class DataOutputTaskImpl implements RemoteJobServiceExtWithParams {

    ThreadPoolExecutor pools = new ThreadPoolExecutor(
            15, 30, 60,
            TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(500), new RejectedExecutionHandler() {
        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {

        }
    });

    @Autowired
    private ApplicationInfoService applicationInfoService;

    public static final String MBEAN_NAME = "dc:client=DataOutputTaskImpl";

    public static final String HIVE_HDFS_PATH = "/hive/warehouse/";

    private static final Logger logger = Logger.getLogger(DataOutputTaskImpl.class);

    @Autowired
    private ResumeHiveTaskInfoService resumeHiveTaskInfoService;
    @Resource
    private ClientTaskStatusLogService clientTaskStatusLogService;

    @Value("${dc.sqoop.export.map.num}")
    private String mapNumEx;

    @Value("${dc.export.pg.span}")
    private String exportDateSpan = "40";

    @Value("${dc.sqoop.export.dir.prefix}")
    private String exportDirPrefix;

    @Value("${dc.date.format.default}")
    private String dateFormat = "yyyy-MM-dd HH:mm:ss";

    @Value("${dc.sqoop.fields.terminated}")
    private String fieldsTermBy;

    @Value("${dc.sqoop.lines.terminated}")
    private String linesTermBy;

    @Value("${dc.sqoop.bindir}")
    private String bindir;

    @Value("${dc.sqoop.outdir}")
    private String outdir;

    @Value("${dc.sqoop.input.null.string}")
    private String inputNullString;

    @Value("${dc.sqoop.input.null.not.string}")
    private String inputNullNotString;

    @Value("${jdbc.hive.timeout}")
    private String jdbcTimeout = "30";

    @Value("${is.open.export.direct}")
    private String isOpenExpDirect = "0";

    @Value("${dc.export.params}")
    private String exportParams = "";

    @Value("${hive.job.name}")
    private String hiveJobNamePref = "";
    @Autowired
    private JmsClusterMgr jmsClusterMgr;

    public void setClientTaskStatusLogService(ClientTaskStatusLogService clientTaskStatusLogService) {
        this.clientTaskStatusLogService = clientTaskStatusLogService;
    }

    /**
     * 如果不是以文件路径分隔符结尾的，则需要加上
     *
     * @param exportDirPrefix
     */
    public void setExportDirPrefix(String exportDirPrefix) {
        if (StringUtils.isNotBlank(exportDirPrefix)) {
            exportDirPrefix = exportDirPrefix.trim();
            if (exportDirPrefix.endsWith(String.valueOf(File.separatorChar))) {
                this.exportDirPrefix = exportDirPrefix;
            } else {
                this.exportDirPrefix = exportDirPrefix + String.valueOf(File.separatorChar);
            }
        } else {
            this.exportDirPrefix = HIVE_HDFS_PATH;
        }
    }

    public void setBindir(String bindir) {
        if (StringUtils.isNotBlank(bindir)) {
            bindir = bindir.trim();
            if (exportDirPrefix.endsWith(String.valueOf(File.separatorChar))) {
                this.bindir = bindir;
            } else {
                this.bindir = bindir + String.valueOf(File.separatorChar);
            }
        } else {
            this.bindir = "/home/hive/";
        }
    }

    public void setOutdir(String outdir) {
        if (StringUtils.isNotBlank(outdir)) {
            outdir = outdir.trim();
            if (outdir.endsWith(String.valueOf(File.separatorChar))) {
                this.outdir = outdir;
            } else {
                this.outdir = outdir + String.valueOf(File.separatorChar);
            }
        } else {
            this.outdir = "/home/hive/";
        }
    }

    @Override
    public void initializeJob(String jobId, String triggerName, String groupName) {

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
    public JobBizStatusEnum getJobStatus(String jobId, String triggerName, String groupName) {
        Map<String, Object> params = new HashMap<String, Object>();
        params.put(ParamNameEnum.TASK_ID.getValue(), jobId);
        params.put(ParamNameEnum.TASK_NAME.getValue(), triggerName);
        params.put(ParamNameEnum.GROUP_NAME.getValue(), groupName);
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

    @Override
    public String getLogs(String jobId, String triggerName, String groupName, long lastDate) {

        return null;
    }

    /**
     * 1、删除30天前的数据、从pg表中清除掉被物理删除记录
     * 2、pg copy方式将增量数据导出到pg
     */
    @Override
    public void executeJobWithParams(String jobId, String triggerName, String groupName,
                                     RemoteJobInvokeParamsDto remoteJobInvokeParamsDto) {
        DataOutputTaskImplThread dataLoadingTaskImplThread = new DataOutputTaskImplThread(jobId, triggerName, groupName, remoteJobInvokeParamsDto);
        pools.submit(dataLoadingTaskImplThread);
        logger.info("DataOutputTaskImplThread started...");

    }

    /**
     * 删除pg库某个表的所有数据
     * 针对于全量表的处理
     *
     * @param taskConfig
     * @throws ManagerException
     */
    private Map<String, String> clearAllData(TaskPropertiesConfig taskConfig) {
        Map<String, String> variablesMap = new HashMap<String, String>();
        variablesMap.put("--connect", taskConfig.getTargetDbEntity().getConnectionUrl());
        variablesMap.put("--username", taskConfig.getTargetDbEntity().getUserName());
        variablesMap.put("--password", taskConfig.getTargetDbEntity().getPassword());
        variablesMap.put("-e", " truncate table " + taskConfig.getTargetTable());
        return variablesMap;
    }

    /**
     * 导出数据公用方法
     *
     * @param taskConfig
     * @param returnList
     * @param jobId
     * @throws Throwable
     */
    private void commonExportData(TaskPropertiesConfig taskConfig,
                                  List<Object> returnList, String jobId,
                                  RemoteJobInvokeParamsDto remoteJobInvokeParamsDto)
            throws Throwable {
        Date syncBeginTime = (Date) returnList.get(2);
        Date syncEndTime = (Date) returnList.get(3);

        String groupName = taskConfig.getGroupName();
        String taskName = taskConfig.getTriggerName();
        String timeSubixStr = "";
        SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmmss");
        timeSubixStr = formatter.format(new Date());
        try {
            // 在hive上将数据转移至临时表
            String expTmpTableSql = getExpTmpTableSql(taskConfig, syncBeginTime, syncEndTime, timeSubixStr);
            logger.info(String.format("========导出产生临时表的sql语句：%s", expTmpTableSql));
            String jobName = String.format("%s-%s-%s_%s", "hive", taskConfig.getGroupName(),
                    taskConfig.getTriggerName(), System.currentTimeMillis());
            saveAppInfo(remoteJobInvokeParamsDto, jobName, jobId);
            if (!HiveUtils.execUpdate(ParseXMLFileUtil.getDbById(taskConfig.getSourceDbId()), taskConfig, hiveJobNamePref,
                    expTmpTableSql, Integer.valueOf(jdbcTimeout))) {
                JobBizStatusEnum jobBizStatusEnum = JobBizStatusEnum.INTERRUPTED;
                String messageLocalTw = String.format("【groupName=%s】【schedulerName=%s】在hive上将数据转移至临时表 出错", groupName,
                        taskName);
                logger.error(messageLocalTw);
                addTaskLog(jobId, taskName, groupName, jobBizStatusEnum, messageLocalTw, syncBeginTime, syncEndTime);
                SendMsg2AMQ.updateStatusAndSendMsg(jobId, jobBizStatusEnum, jmsClusterMgr, messageLocalTw);
                RinseStatusAndLogCache.removeTaskByJobId(jobId);
                return;
            }

            // 5、拼接sqoop参数
            Map<String, String> paras = createExportArgs(taskConfig, timeSubixStr);
            String command = CommonEnumCollection.TaskTypeEnum.getCommand(taskConfig.getTaskType());
            List<String> options = new ArrayList<String>();
            int openExpDirect = Integer.parseInt(isOpenExpDirect);
            if (taskConfig.getOpenExpDirect() != null) {
                openExpDirect = Integer.parseInt(taskConfig.getOpenExpDirect());
                logger.info(String.format("从对应%s-%s.xml文件中读取信息： 导出方式【%s】", taskConfig.getGroupName(),
                        taskConfig.getTriggerName(), openExpDirect));
            } else {
                logger.info(String.format("从对应dc-client.properties文件中读取信息：导出方式【%s】", openExpDirect));
            }

            if (openExpDirect == 1) {
                options.add("--direct");
                if (!paras.keySet().contains("--escaped-by") || !paras.keySet().contains("--enclosed-by"))
                    logger.warn(String.format("请在%s-%s.xml文件中配置： 【 escaped-by 】【enclosed-by】",
                            taskConfig.getGroupName(), taskConfig.getTriggerName()));
            } else {
                // 这个是单独导出xml 文件中设置了escaped-by 和 enclosed-by 但是导出方式设置的是direct = 0，因此强制设置direct=1
                if (!exportParams.contains("--escaped-by") && !exportParams.contains("--enclosed-by")) {
                    if (paras.keySet().contains("--escaped-by") && paras.keySet().contains("--enclosed-by")
                            && !options.contains("--direct")) {
                        //logger.warn(String.format("导出开关为：Direct = 0 组名【 %s 】调度名【 %s 】 强制设置导出模式为：Direct ", taskConfig.getGroupName(),
                        //		taskConfig.getTriggerName()));
                        //options.add("--direct");
                        logger.warn(String
                                .format("组名【 %s 】调度名【 %s 】 导出参数【escaped-by】【enclosed-by】被设置，但是导出开关为：Direct = 0 因此移除 【escaped-by】【enclosed-by】",
                                        taskConfig.getGroupName(), taskConfig.getTriggerName()));
                        paras.remove("--escaped-by");
                        paras.remove("--enclosed-by");
                    }
                } else {
                    if (paras.keySet().contains("--escaped-by") && paras.keySet().contains("--enclosed-by")
                            && !options.contains("--direct")) {
                        logger.warn(String
                                .format(" 导出开关为：Direct = 0 仅在dc-client.properties中配置【escaped-by】【enclosed-by】未在【 %s-%s.xml 】中配置,因此移除【escaped-by】【enclosed-by】",
                                        taskConfig.getGroupName(), taskConfig.getTriggerName()));
                        paras.remove("--escaped-by");
                        paras.remove("--enclosed-by");
                    }
                }
            }

            // 6、调用sqoop命令
            SqoopApi sqoopApi = SqoopApi.getSqoopApi();
            SqoopParams sqoopParams = new SqoopParams();
            HiveUtils.setSqoopParamProperties(taskConfig, new HashMap<String, String>(),
                    "mapred.job.name=sqoop,", sqoopParams);
            saveAppInfo(remoteJobInvokeParamsDto, sqoopParams, jobId);
            JobExecutionResult ret = sqoopApi.execute(jobId, command,
                    paras, options, sqoopParams.getProperties());

            // 7、执行结束
            if (ret.isSucceed()) {
                JobBizStatusEnum jobBizStatusEnum = JobBizStatusEnum.FINISHED;
                String messageLocalTh = "执行sqoop导出命令成功:"
                        + String.format("【groupName=%s】【schedulerName=%s】！", groupName, taskName);
                logger.info(messageLocalTh);

                Timestamp currentTime = new Timestamp(System.currentTimeMillis()); //获得当前时间
                String selectSql = "select count(*) from all_task_latest_status where task_name = ? and group_name = ?";
                String updataSql = "update all_task_latest_status set status = ? ,finshed_time=TIMESTAMP '"
                        + currentTime + "' where task_name = ? and group_name = ? ";
                String insertSql = "insert into all_task_latest_status values(?,?," + "TIMESTAMP '" + currentTime
                        + "',?)";
                String[] selectParam = {taskName, groupName};
                String[] insertPara = {groupName, taskName, jobBizStatusEnum.toString().toUpperCase()};
                String[] updatePara = {jobBizStatusEnum.toString().toUpperCase(), taskName, groupName};
                logger.info(String.format("开始查询表【all_task_latest_status】的数据的状态，sql=【%s】"
                        + "【groupName=%s】【schedulerName=%s】！", selectSql, groupName, taskName));
                ResultSet rs = PgSqlUtils.executeQuery(taskConfig.getTargetDbEntity(), selectSql, selectParam);
                int numb = 0;
                while (rs.next()) {
                    numb = rs.getInt(1);
                }
                if (numb > 0) { //存在就更新
                    logger.info(String.format("开始更新表【all_task_latest_status】的数据的状态，sql=【%s】"
                            + "【groupName=%s】【schedulerName=%s】！", updataSql, groupName, taskName));
                    PgSqlUtils.executeUpdate(taskConfig.getTargetDbEntity(), updataSql, updatePara);
                } else { //不存在就插入
                    logger.info(String.format("开始插入表【all_task_latest_status】的数据的状态，sql=【%s】"
                            + "【groupName=%s】【schedulerName=%s】！", insertSql, groupName, taskName));
                    PgSqlUtils.executeUpdate(taskConfig.getTargetDbEntity(), insertSql, insertPara);
                }
                addTaskLog(jobId, taskName, groupName, jobBizStatusEnum, messageLocalTh, syncBeginTime, syncEndTime);
                // 成功后，需要传mq并删除缓存
                SendMsg2AMQ.updateStatusAndSendMsg(jobId, jobBizStatusEnum, jmsClusterMgr, messageLocalTh);
                RinseStatusAndLogCache.removeTaskByJobId(jobId);
            } else {
                // 如果导入命令执行失败
                JobBizStatusEnum jobBizStatusEnum = JobBizStatusEnum.INTERRUPTED;
                String messageLocalFo = String.format("【groupName=%s】【schedulerName=%s】执行导出失败！,失败原因：%s", groupName,
                        taskName, ret.getErrorMessage());
                logger.error(messageLocalFo);
                addTaskLog(jobId, taskName, groupName, jobBizStatusEnum,
                        messageLocalFo.substring(0, Math.min(messageLocalFo.length(), 800)), syncBeginTime, syncEndTime);
                RinseStatusAndLogCache.removeTaskByJobId(jobId);
                SendMsg2AMQ.updateStatusAndSendMsg(jobId, jobBizStatusEnum, jmsClusterMgr, messageLocalFo);
            }
        } catch (SQLException e) {
            JobBizStatusEnum jobBizStatusEnum = JobBizStatusEnum.STOPED;
            SendMsg2AMQ.updateStatusAndSendMsg(jobId, jobBizStatusEnum,
                    jmsClusterMgr, ExceptionUtil.getStackTrace(e));
            logger.error("导出失败...", e);
            throw e;
        } finally {
            PgSqlUtils.close(PgSqlUtils.getRs(), PgSqlUtils.getPs(), PgSqlUtils.getCt());
        }
    }

    private List<Object> checkParamValue(RemoteJobInvokeParamsDto remoteJobInvokeParamsDto, String taskId)
            throws ManagerException {
        List<Object> returnList = new ArrayList<Object>();
        if (remoteJobInvokeParamsDto != null) {
            String startTimeStr = remoteJobInvokeParamsDto.getParam(CommonEnumCollection.ParamNameEnum.START_TIME
                    .getValue());
            returnList.add(startTimeStr);
            String endTimeStr = remoteJobInvokeParamsDto.getParam(CommonEnumCollection.ParamNameEnum.END_TIME
                    .getValue());
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
                    SendMsg2AMQ.updateStatusAndSendMsg(taskId, jobBizStatusEnum, jmsClusterMgr, ExceptionUtil.getStackTrace(e));
                    throw runtimeException;
                }
            } else {
                throw new ManagerException(String.format("【jobId为：%s】的任务被调用,传入的开始和结束时间为空.", taskId));
            }

        } else {
            throw new ManagerException(String.format("【jobId为：%s】的任务被调用,传入的参数为空.", taskId));
        }
        return returnList;
    }

    /**
     * 获取导出任务的后缀名
     *
     * @param triggerName
     */
    private String getExportPrefix(String triggerName) {
        String exportPrefix = "";

        if (0 == triggerName.length() || null == triggerName) {
            logger.error("导出异常，在获取导出任务名的后缀出错");
        }
        if (triggerName.endsWith(HiveDefinePartNameEnum.EXPORT_RETAIL_PERFIX.getValue())) {
            exportPrefix = HiveDefinePartNameEnum.EXPORT_RETAIL_PERFIX.getValue();
        } else if (triggerName.endsWith(HiveDefinePartNameEnum.EXPORT_SPORT_PERFIX.getValue())) {
            exportPrefix = HiveDefinePartNameEnum.EXPORT_SPORT_PERFIX.getValue();
        } else {
            if (triggerName.length() > 3) {
                exportPrefix = triggerName.substring(triggerName.length() - 3, triggerName.length());
                logger.info("获取导出任务名的后缀为" + exportPrefix);
            }
        }
        return exportPrefix;
    }

    private String getExpTmpTableSql(TaskPropertiesConfig taskConfig, Date startTime, Date endTime, String timeSubixStr)
            throws ManagerException {
        String triggerName = taskConfig.getTriggerName();

        String tableSubix = HiveDefinePartNameEnum.STAGE_TABLE_NAME_SUBFIX.getValue();
        //为了保证导出的字段与pg的字段一致，必须使用create table .. as ..
        StringBuffer sbBuffer = new StringBuffer("create table ");
        sbBuffer.append(taskConfig.getSourceDbEntity().getDbName());
        sbBuffer.append(".");
        sbBuffer.append(taskConfig.getSourceTable());
        sbBuffer.append(getExportPrefix(triggerName));
        sbBuffer.append(tableSubix);
        sbBuffer.append(timeSubixStr);
        sbBuffer.append(" row format delimited fields terminated by '").append(fieldsTermBy)
                .append("' lines terminated by '").append(linesTermBy).append("' stored as textfile as select ");
        if (taskConfig.getSelectColumns() == null || taskConfig.getSelectColumns().size() <= 0) {
            throw new ManagerException("...........导出功能必须要指定查询字段列表.............");
        }
        // 查询字段列表
        sbBuffer.append(taskConfig.getSelectColumnsStr());

        sbBuffer.append(" from ");
        // 针对删除再插入情况的表，需要使用去重后的真实表来进行数据同步
        String tableName = taskConfig.getSourceTable().trim();
        sbBuffer.append(taskConfig.getSourceDbEntity().getDbName());
        sbBuffer.append(".");
        sbBuffer.append(tableName);
        //}
        sbBuffer.append(" t2 where 1=1 ");

        //全量导出
        if (taskConfig.getIsOverwrite() == 0) {
            if (taskConfig.getSyncTimeColumn() == null || taskConfig.getSyncTimeColumn().size() <= 0) {
                throw new ManagerException("...........导出功能必须要指定时间戳.............");
            }
            List<Object> timeStrList = getSyncTime(taskConfig, startTime, endTime);
            String startTimeStr = (String) timeStrList.get(0);
            String endTimeStr = (String) timeStrList.get(1);
            sbBuffer.append(" and t2.");
            sbBuffer.append(HiveDefinePartNameEnum.PARTITION_DATE_NAME.getValue());
            sbBuffer.append(" >= ");
            sbBuffer.append(startTimeStr);
            sbBuffer.append(" and t2.");
            sbBuffer.append(HiveDefinePartNameEnum.PARTITION_DATE_NAME.getValue());
            sbBuffer.append(" <= ");
            sbBuffer.append(endTimeStr);

        }

        String filterConditions = " ";
        if (taskConfig.getFilterConditions() != null) {
            StringBuffer filters = new StringBuffer();
            for (String filter : taskConfig.getFilterConditions()) {
                if (!filter.contains("and")) {
                    filters.append(" and ");
                } else {
                    filters.append(" ");
                }
                filters.append(filter);
                filters.append(" ");
            }
            filterConditions = filters.toString();
        }
        sbBuffer.append(filterConditions);
        logger.info(String.format("导出临时表部分：【groupName=%s】【schedulerName=%s】【sql=%s】", taskConfig.getGroupName(),
                taskConfig.getTriggerName(), sbBuffer.toString()));
        return sbBuffer.toString();
    }

    /**
     * 增量任务的同步时间
     *
     * @param taskConfig
     * @param startTime
     * @param endTime
     */
    private List<Object> getSyncTime(TaskPropertiesConfig taskConfig, Date startTime, Date endTime)
            throws ManagerException {

        Date currentDate = new Date();
        Calendar calendar = Calendar.getInstance();
        if (null == currentDate || null == calendar) {
            String message = "获取导出时间实例失败";
            throw new ManagerException(message);
        }

        calendar.setTime(currentDate);
        endTime = calendar.getTime();

        int preserveDate = Integer.parseInt(exportDateSpan);
        String message = "导出到 PG 表中预保存日期为：";
        if (taskConfig.getExportDateSpan() != null) {
            preserveDate = taskConfig.getExportDateSpan();
            logger.info(String.format("从对应%s-%s.xml文件中读取信息：%s 【%d】天", taskConfig.getGroupName(),
                    taskConfig.getTriggerName(), message, preserveDate));
        } else {
            logger.info(String.format("从对应dc-client.properties文件中读取信息：%s 【%d】天", message, preserveDate));
        }
        // 设置导出的起始日期从当前时间向上追述  preserveDate 天
        calendar.add(Calendar.DATE, -preserveDate);

        Date startTimer = calendar.getTime();

        SimpleDateFormat sdf = null;
        //判断是否为parquet文件类型导出
        if (taskConfig.getSyncType() != null && taskConfig.getSyncType().equals(SyncTypeEnum.SYNC_TYPE_1.getValue())) {
            sdf = new SimpleDateFormat(DateFormatStrEnum.PARTITION_DATE_YYYY_MM.getValue());
        } else {
            sdf = new SimpleDateFormat(DateFormatStrEnum.PARTITION_DATE_YYYY_MM_DD.getValue());
        }

        List<Object> returnList = new ArrayList<Object>();
        returnList.add(sdf.format(startTimer));
        returnList.add(sdf.format(endTime));
        return returnList;
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

    /**
     * 包装sqoop中拥有参数值的参数
     *
     * @param taskConfig
     * @param timeSubixStr
     * @return
     */
    private Map<String, String> createExportArgs(TaskPropertiesConfig taskConfig, String timeSubixStr) {

        TaskDatabaseConfig targetDbEntity = taskConfig.getTargetDbEntity();
        String targetDbUrl = targetDbEntity.getConnectionUrl();
        String targetDbUser = targetDbEntity.getUserName();
        String targetDbPass = targetDbEntity.getPassword();
        String hiveDbName = taskConfig.getSourceDbEntity().getDbName();
        String targetTableName = taskConfig.getTargetTable();
        Map<String, String> paras = new HashMap<String, String>();
        paras.put("--connect", targetDbUrl);
        paras.put("--username", targetDbUser);
        paras.put("--password", targetDbPass);
        paras.put("--table", targetTableName);
        logger.warn("exportParams:" + exportParams);
        if (targetDbUrl.contains("oracle")) {
            exportParams = "";
        }
        HiveUtils.setSqoopParams(taskConfig, paras, exportParams);
        String message = "导出到PG 表设置map 数量：";
        if (taskConfig.getExportMapSize() != null) {
            mapNumEx = taskConfig.getExportMapSize();
            logger.info(String.format("从对应%s-%s.xml文件中读取信息：%s 【%s】", taskConfig.getGroupName(),
                    taskConfig.getTriggerName(), message, mapNumEx));
        } else {
            logger.info(String.format("从对应dc-client.properties文件中读取信息：%s 【%s】", message, mapNumEx));
        }
        paras.put("-m", mapNumEx);
        String exportPrefix = getExportPrefix(taskConfig.getTriggerName());
        SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
        String classNametimeSubixStr = formatter.format(new Date());

        paras.put("--class-name", exportPrefix.replace("_", "") + "_export_" + taskConfig.getGroupName() + "_"
                + taskConfig.getSourceTable() + classNametimeSubixStr);
        paras.put("--export-dir", exportDirPrefix + hiveDbName + ".db/" + taskConfig.getSourceTable() + exportPrefix
                + HiveDefinePartNameEnum.STAGE_TABLE_NAME_SUBFIX.getValue() + timeSubixStr);
        paras.put("--fields-terminated-by", fieldsTermBy);
        paras.put("--input-null-non-string", inputNullNotString);
        paras.put("--input-null-string", inputNullString);
        //使用 insert模式，将增量数据直接插入到pg库
        /*if (taskConfig.getIsOverwrite() == 0) {
            if (taskConfig.getIsSlaveTable() == 0) {
				if (taskConfig.getPrimaryKeys() != null && taskConfig.getPrimaryKeys().size() > 0) {
					paras.put("--update-key", this.keysToString(taskConfig.getPrimaryKeys()));
					paras.put("--update-mode", "deleteinsert");
				}
			} else {
				if (taskConfig.getRelationColumns() != null && taskConfig.getRelationColumns().size() > 0) {
					paras.put("--update-key", this.keysToString(taskConfig.getRelationColumns()));
					paras.put("--update-mode", "deleteinsert");
				}
			}
		}*/

        //modify by wang.w 生成的java和class文件统一放到一个目录下，方便管理
        paras.put("--bindir", bindir);
        paras.put("--outdir", outdir);
        return paras;
    }

    @ManagedOperation(description = "job simulator")
    @ManagedOperationParameters({@ManagedOperationParameter(description = "trigger name", name = "triggerName"),
            @ManagedOperationParameter(description = "group name", name = "groupName")})
    public void simulateJob(String triggerName, String groupName) {
        logger.info("Test the job trigger from simulator.");
        executeJobWithParams(GernerateUuidUtils.getUUID(), triggerName, groupName, null);
    }

    private void saveAppInfo(RemoteJobInvokeParamsDto remoteJobInvokeParamsDto, String jobName, String jobId) {
        String instanceId = remoteJobInvokeParamsDto.getParam("instanceId");
        String subInstanceId = remoteJobInvokeParamsDto.getParam("subInstanceId");
        Map<String, String> res = new HashMap<String, String>();
        res.put("scheduleId", jobId);
        res.put("taskParentId", instanceId);
        res.put("taskSonId", subInstanceId);
        res.put("mapred.job.name", jobName);
        try {
            applicationInfoService.insertOrUpdateApplicationInfo(res);
        } catch (Exception e) {
            String msg = "保存app信息异常...";
            logger.error(msg, e);
            return;
        }
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
        }
    }

    class DataOutputTaskImplThread implements Runnable {
        private final Logger logger = Logger.getLogger(DataOutputTaskImplThread.class);
        private String jobId;
        private String triggerName;
        private String groupName;
        private RemoteJobInvokeParamsDto remoteJobInvokeParamsDto;

        public DataOutputTaskImplThread(String jobId, String triggerName, String groupName,
                                        RemoteJobInvokeParamsDto remoteJobInvokeParamsDto) {
            this.jobId = jobId;
            this.triggerName = triggerName;
            this.groupName = groupName;
            this.remoteJobInvokeParamsDto = remoteJobInvokeParamsDto;
        }

        @Override
        public void run() {
            //保存任务信息
            try {
                CommUtil.saveJobInfo(resumeHiveTaskInfoService, jobId, triggerName, groupName,
                        remoteJobInvokeParamsDto, DataOutputTaskImpl.class);
            } catch (Exception e) {
                String message = "保存任务信息失败";
                logger.error(message, e);
            }
            JobBizStatusEnum jobBizStatusEnum = null;
            //如果重复调用，则忽略本次请求
            JobBizStatusEnum jobStauts = RinseStatusAndLogCache.getTaskStatusByJobId(jobId);
            if (jobStauts != null && !jobStauts.name().equals(JobBizStatusEnum.INTERRUPTED.name())) {
                logger.info(String.format("【jobId为：%s】的任务被重复调用", jobId));
                return;
            }

            Date syncBeginTime = null;
            Date syncEndTime = null;
            try {
                //判断传入参数是否合理
                List<Object> returnList = checkParamValue(remoteJobInvokeParamsDto, jobId);
                @SuppressWarnings("unused")
                String startTimeStr = (String) returnList.get(0);
                @SuppressWarnings("unused")
                String endTimeStr = (String) returnList.get(1);
                syncBeginTime = (Date) returnList.get(2);
                syncEndTime = (Date) returnList.get(3);

                logger.info("--------开始进入调度方法--初始化------------");
                jobBizStatusEnum = JobBizStatusEnum.INITIALIZING;
                String messageLocalZe = "初始化中";
                addTaskLog(jobId, triggerName, groupName, jobBizStatusEnum, messageLocalZe, syncBeginTime, syncEndTime);
                SendMsg2AMQ.updateStatusAndSendMsg(jobId, jobBizStatusEnum, jmsClusterMgr, messageLocalZe);

                logger.info("--------获取任务配置信息------------");
                // 1、获取任务配置信息
                TaskPropertiesConfig taskConfig = ParseXMLFileUtil.getTaskConfig(groupName, triggerName);
                if (taskConfig == null
                        || (taskConfig.getDependencyTaskIds() != null && taskConfig.getDependencyTaskIds().size() > 0 && taskConfig
                        .getDependencyTaskList().size() <= 0)) {
                    String msg = String.format("获取缓存中的xml失败：组名【%s】调度名【%s】", groupName, triggerName);
                    logger.error(msg);
                    SendMsg2AMQ.updateStatusAndSendMsg(jobId, JobBizStatusEnum.INTERRUPTED, jmsClusterMgr, msg);
                    RinseStatusAndLogCache.removeTaskByJobId(jobId);
                    return;
                }

                logger.info(String.format("任务开始时间：%s ===>结束时间：%s！组名【%s】调度名【%s】", syncBeginTime, syncEndTime, groupName,
                        triggerName));
                // 2、更新实例状态--初始化
                jobBizStatusEnum = JobBizStatusEnum.RUNNING;
                String message = "运行中";
                addTaskLog(jobId, triggerName, groupName, jobBizStatusEnum, message, syncBeginTime, syncEndTime);
                SendMsg2AMQ.updateStatusAndSendMsg(jobId, jobBizStatusEnum, jmsClusterMgr, message);
                Map<String, String> clearMap = null;

                clearMap = clearAllData(taskConfig);

                String connectURL = clearMap.get("--connect");
                if (connectURL.contains("oracle")) {
                    logger.info(String.format("开始truncate  ORACLE 表数据！组名【%s】调度名【%s】", groupName, triggerName));
                } else {
                    logger.info(String.format("开始truncate  PG 表数据！组名【%s】调度名【%s】", groupName, triggerName));
                }

                List<String> options1 = new ArrayList<String>();
                SqoopApi sqoopApi = SqoopApi.getSqoopApi();
                //增加jobName
                SqoopParams sqoopParams = new SqoopParams();
                HiveUtils.setSqoopParamProperties(taskConfig, new HashMap<String, String>(), "mapred.job.name=sqoop,", sqoopParams);
                JobExecutionResult ret = sqoopApi.execute(jobId,
                        CommonEnumCollection.TaskTypeEnum.getCommand(MessageConstant.SQOOP_EVAL),
                        clearMap, options1, sqoopParams.getProperties());
                if (!ret.isSucceed()) {
                    logger.error(String.format("truncate失败.组名【%s】调度名【%s】", groupName, triggerName));
                    throw new ManagerException("******** Truncate失败......");
                }
                commonExportData(taskConfig, returnList, jobId, remoteJobInvokeParamsDto);
                try {

                    CommUtil.delJobInfo(resumeHiveTaskInfoService, jobId);
                } catch (Exception e) {
                    logger.error("删除任务信息失败", e);
                }
            } catch (Throwable e) {
                jobBizStatusEnum = JobBizStatusEnum.INTERRUPTED;
                String errMsg = ExceptionHandleUtils.getExceptionMsg(e);
                String message = String.format("【groupName：%s】--【triggerName：%s】导出任务时失败", groupName, triggerName) + errMsg;
                logger.error(message);
                addTaskLog(jobId, triggerName, groupName, jobBizStatusEnum,
                        message.substring(0, Math.min(message.length(), 800)), syncBeginTime, syncEndTime);
                SendMsg2AMQ.updateStatusAndSendMsg(jobId, jobBizStatusEnum, jmsClusterMgr,
                        message + "\n" + ExceptionUtil.getStackTrace(e));
                RinseStatusAndLogCache.removeTaskByJobId(jobId);
            }
        }
    }
}
