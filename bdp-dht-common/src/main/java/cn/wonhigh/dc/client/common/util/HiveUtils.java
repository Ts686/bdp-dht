package cn.wonhigh.dc.client.common.util;

import cn.wonhigh.dc.client.common.constans.CommonEnumCollection.DateFormatStrEnum;
import cn.wonhigh.dc.client.common.constans.CommonEnumCollection.HiveDefinePartNameEnum;
import cn.wonhigh.dc.client.common.constans.CommonEnumCollection.SyncTypeEnum;
import cn.wonhigh.dc.client.common.constans.MessageConstant;
import cn.wonhigh.dc.client.common.model.*;
import com.alibaba.druid.pool.DruidDataSource;
import com.yougou.logistics.base.common.vo.scheduler.RemoteJobInvokeParamsDto;
import org.apache.log4j.Logger;

import java.sql.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.*;

/**
 * Hive工具类
 *
 * @author wang.w
 * @version 1.0.0
 * @since 2016-05-01
 */
public class HiveUtils {

    private static final Logger logger = Logger.getLogger(HiveUtils.class);
    private static DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    // 表前缀
    public static final String PREF_FULL_TABLE = "ful_";// 全量表
    public static final String PREF_TMP_TABLE = "tmp_";// 临时表

    protected static String cdcStartData = "";
    protected static String cdcEndData = "";

    public static Properties properties;
    public static DruidDataSource dataSourceSrc;
    public static DruidDataSource dataSourceOds;



    static {
        //初始化hive的durid线程池
        properties = PropertyFile.getProps("");
        dataSourceSrc = new DruidDataSource();

        dataSourceSrc.setUrl(properties.getProperty("hive.jdbc.src"));
        dataSourceSrc.setDriverClassName(properties.getProperty("hive.driver.class.name"));
        dataSourceSrc.setUsername(properties.getProperty("hive.user"));
        dataSourceSrc.setPassword(properties.getProperty("hive.password"));
        dataSourceSrc.setTestWhileIdle(Boolean.valueOf(properties.getProperty("hive.testWhileIdle")));
        dataSourceSrc.setValidationQuery(properties.getProperty("hive.validationQuery"));
        dataSourceSrc.setMaxActive(Integer.valueOf(properties.getProperty("hive.max.active")));
        dataSourceSrc.setInitialSize(Integer.valueOf(properties.getProperty("hive.initialSize")));
        dataSourceSrc.setRemoveAbandoned(Boolean.valueOf(properties.getProperty("hive.removeAbandoned")));
        dataSourceSrc.setRemoveAbandonedTimeout(Integer.valueOf(properties.getProperty("hive.removeAbandonedTimeout")));

        dataSourceOds = new DruidDataSource();
        dataSourceOds.setUrl(properties.getProperty("hive.jdbc.ods"));
        dataSourceOds.setDriverClassName(properties.getProperty("hive.driver.class.name"));
        dataSourceOds.setUsername(properties.getProperty("hive.user"));
        dataSourceOds.setPassword(properties.getProperty("hive.password"));
        dataSourceOds.setTestWhileIdle(Boolean.valueOf(properties.getProperty("hive.testWhileIdle")));
        dataSourceOds.setValidationQuery(properties.getProperty("hive.validationQuery"));
        dataSourceOds.setMaxActive(Integer.valueOf(properties.getProperty("hive.max.active")));
        dataSourceOds.setInitialSize(Integer.valueOf(properties.getProperty("hive.initialSize")));
        dataSourceOds.setRemoveAbandoned(Boolean.valueOf(properties.getProperty("hive.removeAbandoned")));
        dataSourceOds.setRemoveAbandonedTimeout(Integer.valueOf(properties.getProperty("hive.removeAbandonedTimeout")));
    }



    /**
     * 从druid连接池中获取对应连接
     * @param dataSource
     * @return
     */
    public static Connection getConnectionFromDruid(DruidDataSource dataSource){

        Connection connection = null;

        try {
            connection = dataSource.getConnection();
        } catch (SQLException e) {
            logger.error("从druid连接池中获取hive连接失败。");
            e.printStackTrace();
        }

        return connection;
    }

    /*
     * 从 bdp-dht.properties 文件中读取cdc.table.list 配置的transactionHistoryLog 列表，
     * 如果该列表为空则不启动 历史数据迁移功能
     */
    public static String[] getCdcTableList() {
        String cdcTable_list = PropertyFile.getValue(MessageConstant.TRANSCATION_HISTORY_LOG, " ");

        String[] tmpTransHistoryLogSrc = new String[cdcTable_list.split(",").length];
        tmpTransHistoryLogSrc = cdcTable_list.split(",");

        logger.info(String.format("在【bdp-dht.properties】文件中读取配置项目【cdc.table.list】读取的内容为【 %s 】，" + "个数为：【%s】",
                cdcTable_list, tmpTransHistoryLogSrc.length));
        return tmpTransHistoryLogSrc;
    }


    /**
     * 重载获取hive连接池
     * 可动态获取hive库连接池信息
     * @param dbName
     * @param url
     * @param user
     * @param passwd
     * @param jdbcTimeout
     * @return
     * @throws SQLException
     */
    public static Connection getConn(String dbName,String url, String user, String passwd, Integer jdbcTimeout) throws SQLException {
        logger.info("获取Hive JDBC连接信息: " + url);
        // DriverManager.setLoginTimeout(jdbcTimeout);

        Connection connectionFromDruid = null;
        //根据数据库名称动态选择连接池
        if("dc_src".equalsIgnoreCase(dbName)){
            connectionFromDruid = getConnectionFromDruid(dataSourceSrc);
        }else if("dc_ods".equalsIgnoreCase(dbName)){
            connectionFromDruid = getConnectionFromDruid(dataSourceOds);
        }

        if(null !=  connectionFromDruid){
            logger.info("从druid连接池中获取hive连接成功");
            return connectionFromDruid;
        }

        String jdbcUser = "hive";
        String jdbcPassword = "123456";
        logger.info("druid连接池中获取hive连接失败，直接创建connection");

        return DriverManager.getConnection(url, user == null ? jdbcUser : user, passwd == null ? jdbcPassword : passwd);
    }

    /**
     * 获取当个hive数据库连接
     * @param url
     * @param user
     * @param passwd
     * @param jdbcTimeout
     * @return
     * @throws SQLException
     */
    public static Connection getConn(String url, String user, String passwd, Integer jdbcTimeout) throws SQLException {
        logger.info("获取Hive JDBC连接信息: " + url);
        // DriverManager.setLoginTimeout(jdbcTimeout);

        String jdbcUser = "hive";
        String jdbcPassword = "123456";
        logger.info("druid连接池中获取hive连接失败，直接创建connection");

        return DriverManager.getConnection(url, user == null ? jdbcUser : user, passwd == null ? jdbcPassword : passwd);
    }

    @SuppressWarnings("static-access")
    public static void updateTransactionHisLog(TaskPropertiesConfig taskConfig, String tableName, Date startTime,
                                               Date endTime, String filterConditions, Integer jdbcTimeOut, boolean isFull) throws SQLException {
        Connection conn = null;
        Statement stmt = null;
        logger.debug(String.format("将*transcation_history_log更新数据加载==【groupName：%s】【triggerName：%s】",
                taskConfig.getGroupName(), taskConfig.getTriggerName()));
        String syncBeginTimeStr = "";
        String syncEndTimeStr = "";
        Date currentDate = new Date();
        Calendar calendar = Calendar.getInstance();
        if (null == currentDate || null == calendar) {
            String message = "获取导出时间实例失败";
            logger.error(message);
        }
        calendar.setTime(currentDate);
        if (endTime == null) {
            endTime = calendar.getTime();
        }

        calendar.add(Calendar.YEAR, -1);
        if (startTime == null) {
            startTime = calendar.getTime();
        }
        if (startTime != null) {
            syncBeginTimeStr = DateUtils.formatDatetime(startTime, "yyyyMMdd");
        }
        if (endTime != null) {
            syncEndTimeStr = String.valueOf(getHivePartitionValue(endTime));
        }
        try {
            conn = getConn(taskConfig.getTargetDbEntity().getDbName(),taskConfig.getTargetDbEntity().getConnectionUrl(),
                    taskConfig.getTargetDbEntity().getUserName(), taskConfig.getTargetDbEntity().getPassword(),
                    jdbcTimeOut);

            List<String> cdcTableLists = new ArrayList<String>();

            if (isFull == true) {
                cdcTableLists = Arrays.asList(getCdcTableList());
            } else {
                cdcTableLists = Arrays
                        .asList(taskConfig.getGroupName() + "_" + HiveDefinePartNameEnum.CDC_TABLE_SUBFIX.getValue());
                ;
            }
            List<String> tmp = new ArrayList<String>();
            for (String historyLog : cdcTableLists) {
                logger.debug(String.format("查找%s中的 dml_type != 0 的id_column_value值", historyLog));
                String prefixHisLog = historyLog.replace("_transaction_history_log_src", "");
                String cdcTableName = HiveDefinePartNameEnum.DB_NAME_SRC.getValue() + historyLog;
                String groupName = prefixHisLog + ParseXMLFileUtil.SPLITE_CHAR_4;
                StringBuffer sql = new StringBuffer(" select table_name,id_column_name,dml_event_time from ");
                sql.append(cdcTableName);
                sql.append(" where dml_type != 0 ");
                if (!syncBeginTimeStr.isEmpty() && !syncEndTimeStr.isEmpty()) {
                    sql.append(" and partition_date >= ");
                    sql.append(syncBeginTimeStr);
                    sql.append(" and partition_date <= ");
                    sql.append(syncEndTimeStr);
                }
                stmt = conn.createStatement();
                logger.info(String.format("***执行任务***【%s】", sql.toString()));
                if (stmt == null) {
                    logger.info(String.format("***执行任务***【%s】", "get stmt error,and stmt is null!"));
                    return;
                }
                ResultSet res = stmt.executeQuery(sql.toString());
                while (res.next()) {
                    String table_name = res.getString("table_name");
                    String id_column_name = res.getString("id_column_name");
                    Date id_column_value = res.getTimestamp("dml_event_time");
                    Integer capture_time = getHivePartitionValue(id_column_value);
                    if (table_name.endsWith(HiveDefinePartNameEnum.HIS_TABLE_TABLE_NAME_SUBFIX.getValue())) {
                        table_name = table_name.replace(HiveDefinePartNameEnum.HIS_TABLE_TABLE_NAME_SUBFIX.getValue(),
                                "");
                    }
                    StringBuffer keyParam = new StringBuffer(groupName);
                    keyParam.append(table_name);
                    keyParam.append(HiveDefinePartNameEnum.SRC_TABLE_NAME_SUBFIX.getValue());

                    logger.debug(String.format("将【%s】中的 dml_type != 0 的id_column_value值 【value: %s】更新到缓存【key: %s】",
                            historyLog, capture_time, keyParam.toString()));
                    logger.debug(String.format("table_name：%s id_column_name:%s id_column_value:%s key:%s", table_name,
                            id_column_name, capture_time, keyParam.toString()));
                    // 将transcation_history_log表中的找到的字段进行更新

                    if (ParseXMLFileUtil.setTaskConfigParam(keyParam.toString(), capture_time)) {
                        tmp.add(keyParam.toString());
                        logger.debug(String.format(
                                "将【%s】中的 dml_type != 0 的id_column_value值 【value: %s】更新到缓存【key: %s】成功【tmp:%s】",
                                historyLog, capture_time, keyParam.toString(), tmp.size()));
                    } else {
                        logger.error(String.format(
                                "*****将【%s】中的 dml_type != 0 的id_column_value值 【value: %s】更新到缓存【key: %s】失败！", historyLog,
                                id_column_value, keyParam.toString()));
                    }

                }
                // 将transcation_history_log表中的未找到的字段更新为空值
                Set<String> tmpAllKeys = new HashSet<String>();
                Set<String> tmpAll = ParseXMLFileUtil.getCacheTaskEntitiesKeys();
                Iterator<String> keyIt = tmpAll.iterator();
                while (keyIt.hasNext()) {
                    tmpAllKeys.add(keyIt.next());
                }
                logger.info("tmpAllKeys:" + tmpAllKeys.size() + " tmp:" + tmp.size());
                for (String key : tmp) {
                    if (tmpAllKeys.contains(key)) {
                        logger.debug("remove begin tmpAllKeys contains key:" + key
                                + " then remove the key from tmpAllKeys size:" + tmpAllKeys.size() + " tmp:"
                                + tmp.size());
                        tmpAllKeys.remove(key);
                        logger.debug("remove end tmpAllKeys contains key:" + key
                                + " then remove the key from tmpAllKeys size:" + tmpAllKeys.size());
                    }
                }
                for (String key : tmpAllKeys) {
                    if (key.contains(groupName)
                            && key.endsWith(HiveDefinePartNameEnum.SRC_TABLE_NAME_SUBFIX.getValue())) {
                        if (ParseXMLFileUtil.setTaskConfigParam(key, 0)) {
                            logger.debug(
                                    String.format("#######将组【%s】中的 【key: %s】【value: 设置为空字符串】更新到缓存成功", key, historyLog));
                        }
                    }
                }
            } // for
            Thread.currentThread().sleep(3000);

        } catch (SQLException e) {
            logger.error("close the statement err, caused by: " + e.getMessage());
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            if (null != stmt && false == stmt.isClosed()) {
                stmt.close();
            }
            if (null != conn && false == conn.isClosed()) {
                conn.close();
            }
        }
    }

    /**
     * 全量导入的结果可以直接放入到去重表
     *
     * @param taskConfig
     * @param tableName
     * @param startTime
     * @param endTime
     * @param filterConditions
     * @throws Exception
     */
    public static void loadIntoDuplicate(TaskPropertiesConfig taskConfig, String tableName, Date startTime,
                                         Date endTime, String filterConditions, Integer jdbcTimeOut) throws Exception {
        Connection conn = null;
        String fullTable = tableName + HiveDefinePartNameEnum.CLN_TABLE_NAME_SUBFIX.getValue();// 全量表
        try {
            conn = getConn(taskConfig.getTargetDbEntity().getDbName()taskConfig.getSourceDbEntity().getConnectionUrl(),
                    taskConfig.getSourceDbEntity().getUserName(), taskConfig.getSourceDbEntity().getPassword(),
                    jdbcTimeOut);

            // 1.创建全量表，如果不存在
            String sql = " CREATE TABLE IF NOT EXISTS " + fullTable + " LIKE " + tableName;
            logger.info(String.format("=============>创建去重表：【sql=%s】", sql));
            PreparedStatement ps = conn.prepareStatement(sql);
            executeHiveStatementAndClose(ps);

            String truncateFullTableSql = "TRUNCATE TABLE " + fullTable;
            PreparedStatement truncateFullTableStatement = conn.prepareStatement(truncateFullTableSql);
            executeHiveStatementAndClose(truncateFullTableStatement);
            StringBuilder createFullTableSql = new StringBuilder("insert into table ");
            createFullTableSql.append(fullTable).append(" partition (biz_date) ").append(" select * from ")
                    .append(tableName).append(" t ").append("where 1=1 ").append(filterConditions);
            logger.info(String.format("进入去重表【%s】时的sql语句：【%s】", fullTable, createFullTableSql));
            PreparedStatement createFullTableStatement = conn.prepareStatement(createFullTableSql.toString());
            executeHiveStatementAndClose(createFullTableStatement);
        } catch (Exception e) {
            logger.error(String.format("表【%s】全量去重出现异常：", fullTable), e);
            throw e;
        } finally {
            closeHiveConnectionQuietly(conn);
        }
    }

    /**
     * 全量导入的结果可以直接放入到去重表
     *
     * @param taskConfig
     * @throws Exception
     */
    public static void fullTableCleaningDate(RemoteJobInvokeParamsDto remoteJobInvokeParamsDto, String jobId,
                                             TaskPropertiesConfig taskConfig, String globleHadoopParams,
                                             Integer jdbcTimeOut, String jobName) throws Exception {
        Connection conn = null;
        String fullTable = "";

        // fullTable += HiveDefinePartNameEnum.DB_NAME_ODS.getValue();
        conn = getConn(taskConfig.getTargetDbEntity().getDbName(),taskConfig.getTargetDbEntity().getConnectionUrl(), taskConfig.getTargetDbEntity().getUserName(),
                taskConfig.getTargetDbEntity().getPassword(), jdbcTimeOut);
        logger.debug("URL:" + taskConfig.getTargetDbEntity().getConnectionUrl() + " userName:"
                + taskConfig.getTargetDbEntity().getUserName() + " passWd:"
                + taskConfig.getTargetDbEntity().getPassword());
        fullTable += taskConfig.getTargetTable();
        String srcTable = HiveDefinePartNameEnum.DB_NAME_SRC.getValue();
        srcTable += taskConfig.getSourceTable();

        try {
            setHadoopParams(globleHadoopParams, taskConfig, conn, jobName);
            StringBuffer truncateFullTableSql = new StringBuffer("truncate table  ");
            truncateFullTableSql.append(fullTable);

            logger.info(String.format("清空全量去重表【%s】时的sql语句：【%s】", fullTable, truncateFullTableSql));
            String selectColumns = taskConfig.getSelectColumnsStr();
            PreparedStatement truncateFullTableStatement = conn.prepareStatement(truncateFullTableSql.toString());
            executeHiveStatementAndClose(truncateFullTableStatement);
            logger.info(String.format("Truncate 全量去重表【%s】成功", fullTable));

            StringBuilder createFullTableSql = new StringBuilder("insert into table ");
            createFullTableSql.append(fullTable);
            if (taskConfig.getSyncType().equals(SyncTypeEnum.SYNC_TYPE_1.getValue())) {
                createFullTableSql.append(" partition( ");
                createFullTableSql.append(HiveDefinePartNameEnum.PARTITION_DATE_NAME.getValue());
                createFullTableSql.append(" ) ");
            }
            createFullTableSql.append(" select ");
            createFullTableSql.append(selectColumns);
            createFullTableSql.append(" from ");
            createFullTableSql.append(srcTable);
            createFullTableSql.append(" src_t ");
            createFullTableSql.append(" where 1==1 ");
            createFullTableSql.append(taskConfig.getFilterConditionsStr());

            logger.info(String.format("开始向全量表【%s】插入数据，详细sql语句：【%s】", fullTable, createFullTableSql));
            PreparedStatement createFullTableStatement = conn.prepareStatement(createFullTableSql.toString());
            executeHiveStatementAndClose(createFullTableStatement);
            logger.info(String.format("插入全量表【%s】成功", fullTable));

        } catch (Exception e) {
            logger.error(String.format("表【%s】全量去重出现异常：", fullTable), e);
            throw e;
        } finally {
            closeHiveConnectionQuietly(conn);
        }
    }

    /**
     * 数据去重 1、创建全量表，如果不存在 2、删除临时表，如果存在 3、将去重后的全量数据插入临时表 4、删除全量表 5、将临时表重命名为全量表
     *
     * @param taskConfig     数据库配置信息
     * @param tableName      表名
     * @param uniqueKeys     唯一字段名
     * @param selectPreMonth
     * @param jdbcTimeout
     * @param selectPreMonth 时间范围
     * @param timeSpan
     * @throws SQLException
     */
    public static void removeDuplicate(String jobName,
                                       TaskPropertiesConfig taskConfig, String globleHadoopParams, Integer jdbcTimeout,
                                       Integer selectPreMonth, Date... timeSpan
    ) throws Exception {
        /*
         * 初始化变量
         */
        String targetTableName = taskConfig.getTargetTable(); // 清洗的ods表
        String sourceTableName = taskConfig.getSourceTable();// src表
        String startTime = null;// 开始时间
        String endTime = null;// 结束时间
        String partition_date = HiveDefinePartNameEnum.PARTITION_DATE_NAME.getValue();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        if (timeSpan.length != 2) {
            throw new IllegalArgumentException("数据去重失败: 非法参数，需要制定开始和结束时间");
        }
        startTime = sdf.format(timeSpan[0]);
        endTime = sdf.format(timeSpan[1]);

        logger.info(
                "去重时间范围: { " + DateUtils.formatDatetime(startTime) + " - " + DateUtils.formatDatetime(endTime) + " }");

        List<String> timeColumns = taskConfig.getSyncTimeColumn();// 增量时间字段
        if (timeColumns == null || timeColumns.size() == 0) {
            throw new IllegalArgumentException("数据去重失败: 非法参数，没有指定增量时间字段");
        }

        List<String> pkColumns = taskConfig.getPrimaryKeys();// 主键字段
        if (pkColumns == null || pkColumns.size() == 0) {
            throw new IllegalArgumentException("数据去重失败: 非法参数，没有指定主键");
        }

        Connection sourceConn = null;

        try {
            sourceConn = getConn(taskConfig.getTargetDbEntity().getDbName(),taskConfig.getSourceDbEntity().getConnectionUrl(),
                    taskConfig.getSourceDbEntity().getUserName(), taskConfig.getSourceDbEntity().getPassword(),
                    jdbcTimeout);
            setHadoopParams(globleHadoopParams, taskConfig, sourceConn, jobName);
            logger.info(String.format("=======>增量去重逻辑部分：【dbName=%s】【tableName=%s】【uniqueKeys=%s】【url=%s】",
                    taskConfig.getSourceDbEntity().getDbName(), targetTableName, taskConfig.getPrimaryKeysStr(),
                    taskConfig.getSourceDbEntity().getConnectionUrl()));

            /**
             * 清洗的月份计算
             */
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(timeSpan[0]);
            calendar.add(Calendar.MONTH, -selectPreMonth);
            String startMonth = new SimpleDateFormat("yyyyMM").format(calendar.getTime());
            String endMonth = new SimpleDateFormat("yyyyMM").format(timeSpan[1]);

            // 1.将去重后的数据回写到去重表

            StringBuilder keyStrs = new StringBuilder();
            StringBuilder keyThlStrs = new StringBuilder();
            for (String key : taskConfig.getPrimaryKeys()) {
                keyStrs.append(" and src_t." + key + " = stn." + key);
            }
            // THL中捕获联合主键时，采用逗号连接各个值（要保证xml中的primaryKey联合主键顺序与binlogCapture中的一致）
            if (taskConfig.getPrimaryKeys().size() > 1) {
                keyThlStrs.append(" and concat(src_t.")
                        .append(taskConfig.getPrimaryKeysStr().replace(",", ",',',src_t.")).append(")")
                        .append(" = rgibt.").append(HiveDefinePartNameEnum.CDC_TABLE_ID_COLUMN_VALUE.getValue());
            } else {
                keyThlStrs.append(" and src_t.").append(taskConfig.getPrimaryKeysStr()).append(" = rgibt.")
                        .append(HiveDefinePartNameEnum.CDC_TABLE_ID_COLUMN_VALUE.getValue());
            }

            // 1.转移数据至临时表
            long currentTime = System.currentTimeMillis();
            StringBuilder hql = new StringBuilder();
            hql.append("create TABLE ").append(taskConfig.getTargetDbEntity().getDbName()).append(".")
                    .append(targetTableName).append(HiveDefinePartNameEnum.TMP_TABLE_NAME_SUBFIX.getValue())
                    .append(currentTime).append(" as select ")
                    .append(taskConfig.getSelectColumnsStr().replace("src_t.partition_date",
                            "date_format(src_t." + taskConfig.getSyncTimeColumn().get(0)))
                    .append(",'yyyyMM') as partition_date from ").append(taskConfig.getTargetDbEntity().getDbName())
                    .append(".").append(targetTableName).append(" src_t left join ")
                    .append(taskConfig.getSourceDbEntity().getDbName()).append(".").append(sourceTableName)
                    .append(" stn on (stn.").append(partition_date).append(" >= '").append(startTime)
                    .append("' and stn.").append(partition_date).append(" <= '").append(endTime).append("' ")
                    .append(keyStrs).append(" ) left join ").append(taskConfig.getSourceDbEntity().getDbName())
                    .append(".")
                    .append(sourceTableName.replace(HiveDefinePartNameEnum.SRC_TABLE_NAME_SUBFIX.getValue(),
                            HiveDefinePartNameEnum.THL_TABLE_NAME_SUBFIX.getValue()))
                    .append(" rgibt on (rgibt.").append(partition_date).append(" >= ").append(startTime)
                    .append(" and rgibt.").append(partition_date).append(" <= ").append(endTime).append(" ")
                    .append(keyThlStrs).append(" ) where src_t.").append(partition_date).append(" >= ")
                    .append(startMonth).append(" and src_t.").append(partition_date).append(" <= ").append(endMonth)
                    .append(" and stn.").append(taskConfig.getPrimaryKeys().get(0)).append(" is null and rgibt.")
                    .append(HiveDefinePartNameEnum.CDC_TABLE_ID_COLUMN_VALUE.getValue()).append(" is null ")
                    .append(taskConfig.getFilterConditionsStr()).append(" union all select ")
                    .append(taskConfig.getSelectColumnsStr()).append(" from (select ")
                    .append(taskConfig.getSelectColumnsStr().replace("src_t.partition_date",
                            "date_format(src_t." + taskConfig.getSyncTimeColumn().get(0)))
                    .append(",'yyyyMM') as partition_date,row_number() over(partition by ")
                    .append(taskConfig.getPrimaryKeysStr()).append(" order by ")
                    .append(taskConfig.getSyncTimeColumnStr()).append(" desc ) rm from ")
                    .append(taskConfig.getSourceDbEntity().getDbName()).append(".").append(sourceTableName)
                    .append(" src_t where src_t.").append(partition_date).append(" >= ").append(startTime)
                    .append(" and src_t.").append(partition_date).append(" <= ").append(endTime).append(" ")
                    .append(taskConfig.getFilterConditionsStr()).append(") src_t where src_t.rm = 1");

            logger.info(String.format("1.进入去重表【%s】时的sql语句：【%s】", targetTableName, hql.toString()));

            PreparedStatement hqlStatement = sourceConn.prepareStatement(hql.toString());
            executeHiveStatementAndClose(hqlStatement);

            // 2.删除分区
            StringBuffer sbBuffer = new StringBuffer("alter table ");
            sbBuffer.append(taskConfig.getTargetDbEntity().getDbName()).append(".").append(targetTableName)
                    .append(" drop if exists partition(");
            while (true) {
                Date cmpDate = calendar.getTime();
                String pt = new SimpleDateFormat("yyyyMM").format(cmpDate);
                String dropPtStr = sbBuffer.toString() + partition_date + "=" + pt + ") PURGE ";
                logger.info(String.format("2.删除分区【%s】时的sql语句：【%s】", targetTableName, dropPtStr));
                hqlStatement = sourceConn.prepareStatement(dropPtStr);
                executeHiveStatementAndClose(hqlStatement);
                if (pt.equals(endTime.substring(0, endTime.length() - 2))) {
                    break;
                } else {
                    calendar.setTime(cmpDate);
                    calendar.add(Calendar.MONTH, 1);
                }
            }

            // 3.回写数据
            StringBuffer insertSql = new StringBuffer();
            insertSql.append("insert into table ").append(taskConfig.getTargetDbEntity().getDbName()).append(".")
                    .append(targetTableName).append(" partition(").append(partition_date).append(") select * from ")
                    .append(taskConfig.getTargetDbEntity().getDbName()).append(".").append(targetTableName)
                    .append(HiveDefinePartNameEnum.TMP_TABLE_NAME_SUBFIX.getValue()).append(currentTime);
            logger.info(String.format("3.回写数据【%s】时的sql语句：【%s】", targetTableName, insertSql.toString()));
            hqlStatement = sourceConn.prepareStatement(insertSql.toString());
            executeHiveStatementAndClose(hqlStatement);

            // 4.删除临时表
            StringBuffer dropTable = new StringBuffer();
            dropTable.append("drop table if exists ").append(taskConfig.getTargetDbEntity().getDbName()).append(".")
                    .append(targetTableName).append(HiveDefinePartNameEnum.TMP_TABLE_NAME_SUBFIX.getValue())
                    .append(currentTime);
            logger.info(String.format("4.删除临时表【%s】时的sql语句：【%s】", targetTableName, dropTable.toString()));
            hqlStatement = sourceConn.prepareStatement(dropTable.toString());
            executeHiveStatementAndClose(hqlStatement);

        } catch (Throwable e) {
            logger.error("数据去重失败: " + e.getMessage());
            throw new SQLException(e);
        } finally {
            closeHiveConnectionQuietly(sourceConn);
        }
    }

    /**
     * 数据去重 1、数据表清洗
     *
     * @param taskConfig     数据库配置信息
     * @param tableName      表名
     * @param uniqueKeys     唯一字段名
     * @param selectPreMonth
     * @param jdbcTimeout
     * @param selectPreMonth 时间范围
     * @param timeSpan
     * @throws SQLException
     */
    public static void removeDuplicate2(String jobName,
                                        TaskPropertiesConfig taskConfig, String globleHadoopParams, Integer jdbcTimeout,
                                        Integer selectPreMonth, Date... timeSpan) throws Exception {
        /*
         * 初始化变量
         */
        String targetTableName = taskConfig.getTargetTable(); // 清洗的ods表
        String sourceTableName = taskConfig.getSourceTable();// src表
        String startTime = null;// 开始时间
        String endTime = null;// 结束时间
        String partition_date = HiveDefinePartNameEnum.PARTITION_DATE_NAME.getValue();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        if (timeSpan.length != 2) {
            throw new IllegalArgumentException("数据去重失败: 非法参数，需要制定开始和结束时间");
        }
        startTime = sdf.format(timeSpan[0]);
        endTime = sdf.format(timeSpan[1]);

        logger.info(
                "去重时间范围: { " + DateUtils.formatDatetime(startTime) + " - " + DateUtils.formatDatetime(endTime) + " }");

        List<String> timeColumns = taskConfig.getSyncTimeColumn();// 增量时间字段
        if (timeColumns == null || timeColumns.size() == 0) {
            throw new IllegalArgumentException("数据去重失败: 非法参数，没有指定增量时间字段");
        }

        List<String> pkColumns = taskConfig.getPrimaryKeys();// 主键字段
        if (pkColumns == null || pkColumns.size() == 0) {
            throw new IllegalArgumentException("数据去重失败: 非法参数，没有指定主键");
        }

        Connection sourceConn = null;

        try {
            sourceConn = getConn(taskConfig.getTargetDbEntity().getDbName(),taskConfig.getSourceDbEntity().getConnectionUrl(),
                    taskConfig.getSourceDbEntity().getUserName(), taskConfig.getSourceDbEntity().getPassword(),
                    jdbcTimeout);
            setHadoopParams(globleHadoopParams, taskConfig, sourceConn, jobName);
            logger.info(String.format("=======>增量去重逻辑部分：【dbName=%s】【tableName=%s】【uniqueKeys=%s】【url=%s】",
                    taskConfig.getSourceDbEntity().getDbName(), targetTableName, taskConfig.getPrimaryKeysStr(),
                    taskConfig.getSourceDbEntity().getConnectionUrl()));

            /**
             * 清洗的月份计算
             */
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(timeSpan[0]);
            calendar.add(Calendar.MONTH, -selectPreMonth);
            String startMonth = new SimpleDateFormat("yyyyMM").format(calendar.getTime());
            String endMonth = new SimpleDateFormat("yyyyMM").format(timeSpan[1]);

            // 1.将去重后的数据回写到去重表

            StringBuilder keyStrs = new StringBuilder();
            StringBuilder keyThlStrs = new StringBuilder();
            for (String key : taskConfig.getPrimaryKeys()) {
                keyStrs.append(" and src_t." + key + " = stn." + key);
            }
            // THL中捕获联合主键时，采用逗号连接各个值（要保证xml中的primaryKey联合主键顺序与binlogCapture中的一致）
            if (taskConfig.getPrimaryKeys().size() > 1) {
                keyThlStrs.append(" and concat(src_t.")
                        .append(taskConfig.getPrimaryKeysStr().replace(",", ",',',src_t.")).append(")")
                        .append(" = rgibt.").append(HiveDefinePartNameEnum.CDC_TABLE_ID_COLUMN_VALUE.getValue());
            } else {
                keyThlStrs.append(" and src_t.").append(taskConfig.getPrimaryKeysStr()).append(" = rgibt.")
                        .append(HiveDefinePartNameEnum.CDC_TABLE_ID_COLUMN_VALUE.getValue());
            }

            // 1.清洗环节
            StringBuilder hql = new StringBuilder();
            hql.append("insert overwrite TABLE ").append(taskConfig.getTargetDbEntity().getDbName()).append(".")
                    .append(targetTableName).append(" partition(")
                    .append(HiveDefinePartNameEnum.PARTITION_DATE_NAME.getValue()).append(") select ")
                    .append(taskConfig.getSelectColumnsStr().replace("src_t.partition_date",
                            "date_format(src_t." + taskConfig.getSyncTimeColumn().get(0)))
                    .append(",'yyyyMM') as partition_date from ").append(taskConfig.getTargetDbEntity().getDbName())
                    .append(".").append(targetTableName).append(" src_t left join ")
                    .append(taskConfig.getSourceDbEntity().getDbName()).append(".").append(sourceTableName)
                    .append(" stn on (stn.").append(partition_date).append(" >= '").append(startTime)
                    .append("' and stn.").append(partition_date).append(" <= '").append(endTime).append("' ")
                    .append(keyStrs).append(" ) left join ").append(taskConfig.getSourceDbEntity().getDbName())
                    .append(".")
                    .append(sourceTableName.replace(HiveDefinePartNameEnum.SRC_TABLE_NAME_SUBFIX.getValue(),
                            HiveDefinePartNameEnum.THL_TABLE_NAME_SUBFIX.getValue()))
                    .append(" rgibt on (rgibt.").append(partition_date).append(" >= ").append(startTime)
                    .append(" and rgibt.").append(partition_date).append(" <= ").append(endTime).append(" ")
                    .append(keyThlStrs).append(" ) where src_t.").append(partition_date).append(" >= ")
                    .append(startMonth).append(" and src_t.").append(partition_date).append(" <= ").append(endMonth)
                    .append(" and stn.").append(taskConfig.getPrimaryKeys().get(0)).append(" is null and rgibt.")
                    .append(HiveDefinePartNameEnum.CDC_TABLE_ID_COLUMN_VALUE.getValue()).append(" is null ")
                    .append(taskConfig.getFilterConditionsStr()).append(" union all select ")
                    .append(taskConfig.getSelectColumnsStr()).append(" from (select ")
                    .append(taskConfig.getSelectColumnsStr().replace("src_t.partition_date",
                            "date_format(src_t." + taskConfig.getSyncTimeColumn().get(0)))
                    .append(",'yyyyMM') as partition_date,row_number() over(partition by ")
                    .append(taskConfig.getPrimaryKeysStr()).append(" order by ")
                    .append(taskConfig.getSyncTimeColumnStr()).append(" desc ) rm from ")
                    .append(taskConfig.getSourceDbEntity().getDbName()).append(".").append(sourceTableName)
                    .append(" src_t where src_t.").append(partition_date).append(" >= ").append(startTime)
                    .append(" and src_t.").append(partition_date).append(" <= ").append(endTime).append(" ")
                    .append(taskConfig.getFilterConditionsStr()).append(") src_t where src_t.rm = 1");

            logger.info(String.format("1.进入去重表【%s】时的sql语句：【%s】", targetTableName, hql.toString()));

            PreparedStatement hqlStatement = sourceConn.prepareStatement(hql.toString());

            executeHiveStatementAndClose(hqlStatement);

            logger.info(String.format("----ODS表【%s】清洗完成！", targetTableName));

        } catch (Throwable e) {
            logger.error("数据去重失败: " + e.getMessage());
            throw new SQLException(e);
        } finally {
            closeHiveConnectionQuietly(sourceConn);
        }
    }

    /**
     * 数据清洗 1、装载区表的增量数据与正式表进行比对，当存在旧数据时，先删除 2、将装载区的增量数据插入至正式表
     * 3、将物理删除轨迹表中的记录从正式表中剔除掉
     *
     * @param jobName
     * @param taskConfig
     * @param jdbcTimeout
     * @param lastestSeqValue
     * @param taskStartTime
     * @param taskEndTime
     * @throws Exception
     */
    public static Long cleaningData(String jobName, TaskPropertiesConfig taskConfig, String globleHadoopParams, Integer jdbcTimeout,
                                    Long lastestSeqValue, Timestamp taskStartTime, Timestamp taskEndTime, int selectPreMonth, String ifcast)
            throws Exception {

        Connection conn = null;
        String message = "";
        try {
            Long maxSeqNo = getMaxSeqValue(taskConfig,
                    new SimpleDateFormat(DateFormatStrEnum.JAVA_YYYY_MM_DD_HH_MM_SS.getValue()).format(taskEndTime));
            if (maxSeqNo < 0 && lastestSeqValue > maxSeqNo) {
                message = String.format("查询sequence no 出现异常.....[lastestSeqValue=%d][maxSeqValue=%d]", lastestSeqValue,
                        maxSeqNo);
                logger.info(message);
                throw new SQLException(message);
            }

            // 1.删除更新、删除的记录
            logger.info("1.删除更新、删除的记录");

            String delSqlStr = getCleaningDataDelSql(taskConfig, ifcast);
            message = String.format("删除ods去重表的数据：【%s】 【groupName：%s】【triggerName：%s】", delSqlStr,
                    taskConfig.getGroupName(), taskConfig.getTriggerName());
            logger.info(message);

            conn = getConn(taskConfig.getTargetDbEntity().getDbName(),taskConfig.getSourceDbEntity().getConnectionUrl(),
                    taskConfig.getSourceDbEntity().getUserName(), taskConfig.getSourceDbEntity().getPassword(),
                    jdbcTimeout);
            if (conn == null) {
                logger.error(String.format("获取连接失败： 【url: %s userName:%s passWord:%s】",
                        taskConfig.getSourceDbEntity().getConnectionUrl(), taskConfig.getSourceDbEntity().getUserName(),
                        taskConfig.getSourceDbEntity().getPassword()));
            }

            setHadoopParams(globleHadoopParams, taskConfig, conn, jobName);

            PreparedStatement delDataStatement = conn.prepareStatement(delSqlStr);
            // 限定查询数据范围：默认为1年内的数据
            Calendar calendar = Calendar.getInstance();
            Date date = new Date();
            DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            date = sdf.parse(
                    new SimpleDateFormat(DateFormatStrEnum.JAVA_YYYY_MM_DD_HH_MM_SS.getValue()).format(taskStartTime));
            calendar.setTime(date);
            calendar.add(Calendar.MONTH, -selectPreMonth);
            int cleandDataMonth = Integer
                    .parseInt(new SimpleDateFormat(DateFormatStrEnum.PARTITION_DATE_YYYY_MM_DD.getValue())
                            .format(calendar.getTime()));
            delDataStatement.setInt(1, cleandDataMonth);
            delDataStatement.setInt(2,
                    Integer.parseInt(new SimpleDateFormat(DateFormatStrEnum.PARTITION_DATE_YYYY_MM_DD.getValue())
                            .format(taskStartTime)));
            delDataStatement.setInt(3, getHivePartitionValue(taskEndTime));
            logger.info(String.format(
                    "设置清洗参数：月份【cleandDataMonth=%s】 同步开始日期【taskStartTime=%s】 同步结束日期【taskEndTime=%s】【groupName：%s】【triggerName：%s】",
                    cleandDataMonth,
                    Integer.parseInt(new SimpleDateFormat(DateFormatStrEnum.PARTITION_DATE_YYYY_MM_DD.getValue())
                            .format(taskStartTime)),
                    Integer.parseInt(new SimpleDateFormat(DateFormatStrEnum.PARTITION_DATE_YYYY_MM_DD.getValue())
                            .format(taskEndTime)),
                    taskConfig.getGroupName(), taskConfig.getTriggerName()));
            if (isHaseThlTask(taskConfig)) {
                delDataStatement.setLong(4, lastestSeqValue);
                delDataStatement.setLong(5, maxSeqNo);
                logger.info(String.format("设置清洗参数：lastestSeqValue【%d】 maxSeqValue【%d】 【groupName：%s】【triggerName：%s】",
                        lastestSeqValue, maxSeqNo, taskConfig.getGroupName(), taskConfig.getTriggerName()));
            }

            logger.info(String.format("开始删除ods中的重复数据！去重月份【%s】 同步开始日期【%s】 lastestSeqValue【%d】 maxSeqValue【%d】",
                    selectPreMonth, cleandDataMonth, lastestSeqValue, maxSeqNo));

            executeHiveStatementAndClose(delDataStatement);

            message = String.format("删除增量数据 成功！ 【groupName：%s】【triggerName：%s】", taskConfig.getGroupName(),
                    taskConfig.getTriggerName());
            logger.info(message);

            // 2.插入增量数据
            String insertOdsSql = getCleaningDataInsertSql(taskConfig);
            Integer startTime = 0;
            Integer endTime = 0;

            PreparedStatement insertDataStatement = conn.prepareStatement(insertOdsSql);
            if (isHaseThlTask(taskConfig)) {
                insertDataStatement.setLong(1, lastestSeqValue);
                insertDataStatement.setLong(2, maxSeqNo);
                startTime = Integer
                        .parseInt(new SimpleDateFormat(DateFormatStrEnum.PARTITION_DATE_YYYY_MM_DD.getValue())
                                .format(taskStartTime));
                endTime = getHivePartitionValue(taskEndTime);
                insertDataStatement.setInt(3, startTime);
                insertDataStatement.setInt(4, endTime);
                logger.info(String.format(
                        "设置插入ods参数：下标表位置【lastestSeqValue=%s】 thl表位置【maxSeqNo=%s】 同步开始日期【taskStartTime=%s】"
                                + "同步结束日期【taskEndTime=%s】 同步开始日期【startTime=%s】 同步结束日期【endTime=%s】"
                                + "【groupName：%s】【triggerName：%s】",
                        lastestSeqValue, maxSeqNo,
                        Integer.parseInt(new SimpleDateFormat(DateFormatStrEnum.PARTITION_DATE_YYYY_MM_DD.getValue())
                                .format(taskStartTime)),
                        Integer.parseInt(new SimpleDateFormat(DateFormatStrEnum.PARTITION_DATE_YYYY_MM_DD.getValue())
                                .format(taskEndTime)),
                        startTime, endTime, taskConfig.getGroupName(), taskConfig.getTriggerName()));
            }

            message = String.format(
                    "开始插入增量数据 【groupName：%s】【triggerName：%s】【lastestSeqValue：%s】【maxSeqNo：%s】【startTime：%s】【endTime：%s】 sql=【%s】",
                    taskConfig.getGroupName(), taskConfig.getTriggerName(), lastestSeqValue, maxSeqNo, startTime,
                    endTime, insertOdsSql);
            logger.info(message);

            executeHiveStatementAndClose(insertDataStatement);

            message = String.format("插入增量数据 成功！ 【groupName：%s】【triggerName：%s】", taskConfig.getGroupName(),
                    taskConfig.getTriggerName());
            logger.info(message);

            return maxSeqNo;
        } catch (Throwable throwAble) {
            logger.error("数据去重失败！  " + "【groupName：】" + taskConfig.getGroupName() + "【triggerName：】 ："
                    + taskConfig.getTriggerName() + throwAble.getMessage());
            throw new SQLException(throwAble);
        } finally {
            closeHiveConnectionQuietly(conn);
        }

    }

    // 根据结束时间创建分区字段
    public static int getHivePartitionValue(Date syncEndTime) {
        SimpleDateFormat sdf = new SimpleDateFormat(DateFormatStrEnum.PARTITION_DATE_YYYY_MM_DD.getValue());
        Date hivePartitionTime = new Date(syncEndTime.getTime() - 1000);
        String hivePartitionValueStr = sdf.format(hivePartitionTime);
        int hivePartitionValue = Integer.parseInt(hivePartitionValueStr);
        return hivePartitionValue;

    }

    /**
     * 从hive中获取此刻该删除表中的最大sequence值 要限定该任务的截止时间，以免在索引下标位置漏掉需要删除的记录
     *
     * @param taskConfig
     * @param endTimeStr
     * @return
     */
    private static Long getMaxSeqValue(TaskPropertiesConfig taskConfig, String endTimeStr) {
        String thlTable = HiveDefinePartNameEnum.DB_NAME_SRC.getValue();
        thlTable += taskConfig.getSourceTable().replace(HiveDefinePartNameEnum.SRC_TABLE_NAME_SUBFIX.getValue(),
                HiveDefinePartNameEnum.THL_TABLE_NAME_SUBFIX.getValue());

        StringBuilder sbBuilder = new StringBuilder();
        sbBuilder.append("select max(");
        sbBuilder.append(HiveDefinePartNameEnum.CDC_TABLE_PK_COL.getValue());
        sbBuilder.append(") from ");
        sbBuilder.append(thlTable);
        sbBuilder.append(" where ");
        sbBuilder.append(HiveDefinePartNameEnum.CDC_DML_TYPE_COL.getValue());
        sbBuilder.append(" = 0 ");
        Connection con = null;
        Statement stmt = null;
        try {
            con = getConn(taskConfig.getTargetDbEntity().getDbName(),taskConfig.getSourceDbEntity().getConnectionUrl(),
                    taskConfig.getSourceDbEntity().getUserName(), taskConfig.getSourceDbEntity().getPassword(), 30);
            stmt = con.createStatement();

            logger.info(String.format("查询最大sequence值的sql语句[ %s ]", sbBuilder.toString()));

            ResultSet rs = stmt.executeQuery(sbBuilder.toString());
            rs.next();
            return rs.getLong(1);
        } catch (SQLException e) {
            logger.error("error during execUpdate hive sql:" + sbBuilder.toString(), e);
        } finally {
            closeHiveStatementQuietly(stmt);
            closeHiveConnectionQuietly(con);
        }
        logger.error(String.format("在表【%s】中查询最大sequence失败，请检查是否HiveServer2出现异常", thlTable));
        return -1L;
    }

    /**
     * 获取插入ods正式表的sql语句
     *
     * @param taskConfig
     * @return
     */
    private static String getCleaningDataInsertSql(TaskPropertiesConfig taskConfig) {
        StringBuilder sbBuilder = new StringBuilder();
        String odsTable = HiveDefinePartNameEnum.DB_NAME_ODS.getValue();
        odsTable += taskConfig.getTargetTable();
        String srcTable = HiveDefinePartNameEnum.DB_NAME_SRC.getValue();
        srcTable += taskConfig.getSourceTable();
        String thlTable = HiveDefinePartNameEnum.DB_NAME_SRC.getValue();
        thlTable += taskConfig.getSourceTable().replace(HiveDefinePartNameEnum.SRC_TABLE_NAME_SUBFIX.getValue(),
                HiveDefinePartNameEnum.THL_TABLE_NAME_SUBFIX.getValue());
        String primaryKeysStr = getPrimaryKeys(taskConfig, "");

        String selectColumnsStr = taskConfig.getSelectColumnsStr();
        boolean isHasTableOfTHL = isHaseThlTask(taskConfig);

        sbBuilder.append("insert into ");
        sbBuilder.append(odsTable);
        sbBuilder.append(" select ");
        sbBuilder.append(selectColumnsStr);
        sbBuilder.append(" from ");
        sbBuilder.append(srcTable);
        if (true == isHasTableOfTHL) {
            sbBuilder.append(" src_t left outer join ");
            sbBuilder.append(thlTable);
            if (taskConfig.getPrimaryKeys().size() == 1) {
                sbBuilder.append(" thl on src_t.");
            } else if (taskConfig.getPrimaryKeys().size() > 1) {
                sbBuilder.append(" thl on ");
            }
            sbBuilder.append(primaryKeysStr);
            sbBuilder.append(" = thl.");
            sbBuilder.append(HiveDefinePartNameEnum.CDC_TABLE_ID_COLUMN_VALUE.getValue());
            sbBuilder.append(" and thl.");
            sbBuilder.append(HiveDefinePartNameEnum.CDC_TABLE_PK_COL.getValue());
            sbBuilder.append(" >= ? ");
            sbBuilder.append(" and thl.");
            sbBuilder.append(HiveDefinePartNameEnum.CDC_TABLE_PK_COL.getValue());
            sbBuilder.append(" <= ? ");
            sbBuilder.append("  where thl.");
            sbBuilder.append(HiveDefinePartNameEnum.CDC_TABLE_ID_COLUMN_VALUE.getValue());
            sbBuilder.append(" is null ");
            sbBuilder.append(" and src_t.");
            sbBuilder.append(HiveDefinePartNameEnum.PARTITION_DATE_NAME.getValue());
            sbBuilder.append(" >= ? and src_t.");
            sbBuilder.append(HiveDefinePartNameEnum.PARTITION_DATE_NAME.getValue());
            sbBuilder.append(" <= ? ");
        } else {
            sbBuilder.append(" src_t where 1==1 ");
        }
        sbBuilder.append(taskConfig.getFilterConditionsStr());
        return sbBuilder.toString();
    }

    /**
     * 判断去重环节是否含有thl 任务
     *
     * @param taskConfig
     * @return
     */
    private static boolean isHaseThlTask(TaskPropertiesConfig taskConfig) {
        List<Integer> dependIdList = taskConfig.getDependencyTaskIds();
        if (dependIdList.size() >= 1) {
            Integer dependId = dependIdList.get(0);
            TaskPropertiesConfig propertyConfig = ParseXMLFileUtil.getTaskConfig(dependId);
            if (null != propertyConfig && 100 == propertyConfig.getTaskType() && !propertyConfig.getTriggerName()
                    .contains(HiveDefinePartNameEnum.THL_TABLE_NAME_SUBFIX.getValue())) {
                return false;
            }
        }
        return true;
    }

    /**
     * 获取 主键或者联合主键
     *
     * @param taskConfig
     * @return
     */
    private static String getPrimaryKeys(TaskPropertiesConfig taskConfig, String tagPrimarykey) {
        String primaryKeys = "";
        int primaryKeySize = taskConfig.getPrimaryKeys().size();
        if (1 == primaryKeySize) {
            primaryKeys += tagPrimarykey;
            primaryKeys += taskConfig.getPrimaryKeys().get(0);
        } else if (primaryKeySize > 1) {
            String tmpConcat = "concat(";
            for (int i = 0; i < primaryKeySize; i++) {
                tmpConcat += tagPrimarykey;
                tmpConcat += taskConfig.getPrimaryKeys().get(i);
                if (i != primaryKeySize - 1) {
                    tmpConcat += ",";
                    tmpConcat += "','";
                    tmpConcat += ",";
                }
            }
            tmpConcat += ")";
            primaryKeys = tmpConcat;
        }
        return primaryKeys;
    }

    /**
     * 获取删除ods正式表的语句
     *
     * @param taskConfig
     * @return
     */
    private static String getCleaningDataDelSql(TaskPropertiesConfig taskConfig, String ifcast) {

        boolean isHasTableOfTHL = isHaseThlTask(taskConfig);

        StringBuilder sbBuilder = new StringBuilder();
        sbBuilder.append("delete from ");
        sbBuilder.append(HiveDefinePartNameEnum.DB_NAME_ODS.getValue());
        sbBuilder.append(taskConfig.getTargetTable());
        sbBuilder.append(" where ");
        sbBuilder.append(HiveDefinePartNameEnum.PARTITION_DATE_NAME.getValue());
        sbBuilder.append(" >= ? and ");
        sbBuilder.append(getPrimaryKeys(taskConfig, ""));
        sbBuilder.append(" in ( select ");
        sbBuilder.append(getPrimaryKeys(taskConfig, "src_t."));
        sbBuilder.append(" from ");
        sbBuilder.append(HiveDefinePartNameEnum.DB_NAME_SRC.getValue());
        sbBuilder.append(taskConfig.getSourceTable());
        sbBuilder.append(" src_t where src_t.");
        sbBuilder.append(HiveDefinePartNameEnum.PARTITION_DATE_NAME.getValue());
        sbBuilder.append(" >= ? and src_t.");
        sbBuilder.append(HiveDefinePartNameEnum.PARTITION_DATE_NAME.getValue());
        sbBuilder.append(" <= ? ");
        sbBuilder.append(taskConfig.getFilterConditionsStr());
        if (true == isHasTableOfTHL) {
            sbBuilder.append(" union all select ");
            sbBuilder.append(HiveDefinePartNameEnum.CDC_TABLE_ID_COLUMN_VALUE.getValue());
            sbBuilder.append(" from ");
            sbBuilder.append(HiveDefinePartNameEnum.DB_NAME_SRC.getValue());
            sbBuilder
                    .append(taskConfig.getSourceTable().replace(HiveDefinePartNameEnum.SRC_TABLE_NAME_SUBFIX.getValue(),
                            HiveDefinePartNameEnum.THL_TABLE_NAME_SUBFIX.getValue()));
            sbBuilder.append(" thl where thl.");
            sbBuilder.append(HiveDefinePartNameEnum.CDC_TABLE_PK_COL.getValue());
            sbBuilder.append(" >= ? ");
            sbBuilder.append(" and thl.");
            sbBuilder.append(HiveDefinePartNameEnum.CDC_TABLE_PK_COL.getValue());
            sbBuilder.append(" <= ?)");
        } else {
            sbBuilder.append("  )");
        }

        return sbBuilder.toString();
    }

    public static boolean execUpdate(TaskDatabaseConfig dbConfig, TaskPropertiesConfig taskConfig,
                                     String globleHadoopParams,
                                     String sql, Integer jdbcTimeOut) throws Exception {
        Connection conn = null;
        Statement stmt = null;
        try {
            conn = getConn(taskConfig.getTargetDbEntity().getDbName(),dbConfig.getConnectionUrl(), dbConfig.getUserName(), dbConfig.getPassword(), jdbcTimeOut);
            // String groupName = taskConfig.getGroupName();
            // if (!groupName.equals("gtp_kettle")) {// gtp_kettle组名的任务不设置
            setHadoopParams(globleHadoopParams, taskConfig, conn);
            // }
            stmt = conn.createStatement();
            int rs = stmt.executeUpdate(sql);
            if (rs >= 0) {
                return true;
            } else {
                return false;
            }
        } catch (SQLException e) {
            logger.error("error during execUpdate hive sql:" + sql, e);
            throw e;
        } finally {
            closeHiveStatementQuietly(stmt);
            closeHiveConnectionQuietly(conn);
        }
    }

    private static void executeHiveStatementAndClose(PreparedStatement hiveStatement) throws SQLException {
        try {
            hiveStatement.execute();
        } catch (SQLException e) {
            logger.error("error during executing hive statement: " + e.getMessage());
            throw e;
        } finally {
            closeHiveStatementQuietly(hiveStatement);
        }
    }

    public static void closeHiveResultSetQuietly(ResultSet resultSet) {
        try {
            if (resultSet != null) {
                resultSet.close();
            }
        } catch (SQLException e) {
            logger.error("error in closing hive resultSet: " + e.getMessage());
        }
    }

    public static void closeHiveStatementQuietly(Statement hiveStatement) {
        try {
            if (hiveStatement != null) {
                hiveStatement.close();
            }
        } catch (SQLException e) {
            logger.error("error in closing hive statement: " + e.getMessage());
        }
    }

    public static void closeHiveConnectionQuietly(Connection connection) {
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException e) {
            logger.error("error in closing hive connection: " + e.getMessage());
        }
    }

    /**
     * 设置sqoop导入参数
     *
     * @param taskConfig
     * @param params
     */
    public static void setSqoopParams(TaskPropertiesConfig taskConfig, Map<String, String> params,
                                      String exportParams) {
        if (taskConfig == null || params == null)
            return;

        if (exportParams.contains(",")) {
            String[] exportParam = exportParams.split(",");
            for (String keyValueParam : exportParam) {
                String[] keyValues = keyValueParam.split("=");
                if (keyValues.length != 2) {
                    logger.warn(
                            "在bdp-dht.properties中读取 【dc.import.params 或dc.export.params】 参数无效！eg:【dc.export.params=--escaped-by=\\006,--enclosed-by=\\006】");
                    continue;
                }
                logger.info("在bdp-dht.properties中读取 【dc.import.params 或dc.export.params】 参数有效！key =" + keyValues[0]
                        + " value =" + keyValues[1]);
                if (keyValues[0].trim().equals("mapred.job.name") || keyValues[0].trim().equals("mapreduce.job.queuename")) {
                    Date date = new Date();
                    // currentTime 开始日期
                    Calendar beginDate = Calendar.getInstance();
                    beginDate.setTime(date);
                    String currentTime = format.format(date.getTime());

                    String jobName = String.format("%s-%s-%s_%s", keyValues[1].trim(), taskConfig.getGroupName(),
                            taskConfig.getTriggerName(), currentTime);
//                    params.put(keyValues[0].trim(), jobName);

                    logger.info("在bdp-dht.properties中读设置jobName！key =" + keyValues[0] + " value =" + jobName);
                } else {
                    params.put(keyValues[0].trim(), keyValues[1].trim());
                    logger.info("在bdp-dht.properties中读设置jobName！key =" + keyValues[0].trim() + " value ="
                            + keyValues[1].trim());
                }
//                params.put(keyValues[0], keyValues[1]);
            }
        }

        Map<String, String> sqoopParam = taskConfig.getSqoopParam();
        sqoopParam = sqoopParam == null || sqoopParam.size() < 1 ? null : sqoopParam;

        if (sqoopParam == null) {
            String message = String.format("未设置sqoop参数！ 【groupName：%s】【triggerName：%s】", taskConfig.getGroupName(),
                    taskConfig.getTriggerName());
            logger.info(message);
            return;
        }

        String sqoopParams = System.lineSeparator();

        String operator = "=";
        String message = "";

        String paramKey = "";
        String paramValue = "";

        for (Map.Entry<String, String> entry : sqoopParam.entrySet()) {
            paramKey = "--";
            paramKey += entry.getKey().trim();
            paramValue = entry.getValue().trim();
            String command = "";
            command += paramKey;
            command += operator;
            command += paramValue;
            command += System.lineSeparator();
            if (params.containsKey(entry.getKey().trim())) {
                message = String.format(
                        "更新bdp-dht.properties中的sqoop参数！ 从配置文件【groupName：%s】【triggerName：%s】中读取【key = %s】【value = %s】",
                        taskConfig.getGroupName(), taskConfig.getTriggerName(), entry.getKey().trim(),
                        entry.getValue().trim());
            } else {
                message = String.format("设置sqoop参数！ 从配置文件【groupName：%s】【triggerName：%s】中读取【key = %s】【value = %s】",
                        taskConfig.getGroupName(), taskConfig.getTriggerName(), entry.getKey().trim(),
                        entry.getValue().trim());
            }
            logger.info(message);
            params.put(paramKey, paramValue);

            sqoopParams += command;
            sqoopParams += System.lineSeparator();
        }
        if (sqoopParams.length() >= 1) {
            message = String.format("设置sqoop参数！ 【groupName：%s】【triggerName：%s】【%s】", taskConfig.getGroupName(),
                    taskConfig.getTriggerName(), sqoopParams);
            logger.info(message);
        }
    }

    /**
     * 设置hadoop集群参数
     *
     * @param taskConfig
     * @param conn
     */
    public static void setHadoopParams(String globleHadoopParams, TaskPropertiesConfig taskConfig, Connection conn)
            throws SQLException {
        if (taskConfig == null || conn == null || globleHadoopParams == null)
            return;
        Map<String, String> hadoopParamsMap = new LinkedHashMap<String, String>();
        logger.info("globleHadoopParams:" + globleHadoopParams);
        if (globleHadoopParams.contains(",")) {
            String[] hadoopParams = globleHadoopParams.split(",");
            for (String keyValueParam : hadoopParams) {
                String[] keyValues = keyValueParam.split("=");
                if (keyValues.length != 2) {
                    logger.warn(
                            "在bdp-dht.properties中读取 【dc.hadoop.thl.params 或dc.hadoop.clnd.params】 参数无效！eg:【dc.export.params=--escaped-by=\\006,--enclosed-by=\\006】");
                    continue;
                }
                logger.info("在bdp-dht.properties中读取 【dc.hadoop.thl.params 或dc.hadoop.thl.params】 参数有效！key ="
                        + keyValues[0] + " value =" + keyValues[1]);
                if (keyValues[0].trim().contains("mapred.job.name")) {
                    Date date = new Date();
                    // currentTime 开始日期
                    Calendar beginDate = Calendar.getInstance();
                    beginDate.setTime(date);
                    String currentTime = format.format(date.getTime());

                    String jobName = String.format("%s-%s-%s_%s", keyValues[1].trim(), taskConfig.getGroupName(),
                            taskConfig.getTriggerName(), System.currentTimeMillis());
                    hadoopParamsMap.put(keyValues[0].trim(), jobName);

                    logger.info("在bdp-dht.properties中读设置jobName！key =" + keyValues[0] + " value =" + jobName);
                } else {
                    hadoopParamsMap.put(keyValues[0].trim(), keyValues[1].trim());
                }
            }
        }
        Map<String, String> hiveParam = taskConfig.getHiveParam();
        hiveParam = hiveParam == null || hiveParam.size() < 1 ? null : hiveParam;

        // if(hiveParam == null) return;

        String haveParams = System.lineSeparator();

        String operator = "=";
        Statement state = null;
        String message = "";
        try {
            state = conn.createStatement();

            String paramKey = "";
            String paramValue = "";
            // 将单独的map key value 全部设置到groubleHadoopParamsMap
            if (hiveParam != null) {
                for (Map.Entry<String, String> entry : hiveParam.entrySet()) {
                    if (hadoopParamsMap.containsKey(entry.getKey().trim())) {
                        message = String.format(
                                "更新bdp-dht.properties中的sqoop参数！ 从配置文件【groupName：%s】【triggerName：%s】中读取【key = %s】【value = %s】",
                                taskConfig.getGroupName(), taskConfig.getTriggerName(), entry.getKey().trim(),
                                entry.getValue().trim());
                        logger.info(message);
                    } else {
                        message = String.format(
                                "设置sqoop参数！ 从配置文件【groupName：%s】【triggerName：%s】中读取【key = %s】【value = %s】",
                                taskConfig.getGroupName(), taskConfig.getTriggerName(), entry.getKey().trim(),
                                entry.getValue().trim());
                    }
                    hadoopParamsMap.put(entry.getKey().trim(), entry.getValue().trim());
                    logger.info(message);
                }
            }
            // 遍历groubleHadoopParamsMap
            for (Map.Entry<String, String> entry : hadoopParamsMap.entrySet()) {
                paramKey = entry.getKey().trim();
                paramValue = entry.getValue().trim();
                String command = "set ";
                command += paramKey;
                command += operator;
                command += paramValue;
                command += System.lineSeparator();
                state.execute(command);

                haveParams += command;
                haveParams += System.lineSeparator();
            }
            if (haveParams.length() >= 1) {
                message = String.format("通过 HiveServer2 设置集群参数 成功！ 【groupName：%s】【triggerName：%s】【%s】",
                        taskConfig.getGroupName(), taskConfig.getTriggerName(), haveParams);
                logger.info(message);
            }
        } catch (SQLException e) {
            message = String.format("通过 HiveServer2 设置集群参数 失败！ 【groupName：%s】【triggerName：%s】【%s】",
                    taskConfig.getGroupName(), taskConfig.getTriggerName(), e.getMessage());
            logger.error(message);
        } finally {
            if (state != null && false == state.isClosed()) {
                try {
                    state.close();
                } catch (SQLException e) {
                    logger.error(String.format("============initConfig【关闭statement失败】", e.getMessage()));
                }
            }
        }

    }

    public static void main(String[] args) throws Exception {


//        TaskPropertiesConfig taskConfig = new TaskPropertiesConfig();
//        List<String> list = new ArrayList<String>();
//        list.add("create_time");
//        list.add("update_time");
//        taskConfig.setSyncTimeColumn(list);
//        List<String> pks = new ArrayList<String>();
//        pks.add("id");
//        taskConfig.setPrimaryKeys(pks);
//        taskConfig.setIsSlaveTable(1);
//        // Hive 数据库连接信息
//        TaskDatabaseConfig dbConfig = new TaskDatabaseConfig();
//        dbConfig.setIpAddr("172.17.210.120");
//        dbConfig.setPort("10000");
//        dbConfig.setUserName("root");
//        dbConfig.setPassword(null);
//        dbConfig.setDbType(2);
//        dbConfig.setCharset("utf-8");
//        dbConfig.setDbName("dc_ods");

        // simpleTest(dbConfig, tableName);

        /*
         * 测试流程 1、清空Hive表item_group,ful_item_group 2、调度导入任务 3、执行去重任务，得到一个全量表
         * 4、验证数据: SELECT COUNT(group_no),COUNT(DISTINCT group_no) FROM
         * ful_item_group; 5、更新数据源数据 6、再次调度导入任务 7、再次执行去重任务 8、验证数据: SELECT
         * COUNT(1),COUNT(DISTINCT group_no) FROM item_group; SELECT
         * COUNT(1),COUNT(DISTINCT group_no) FROM ful_item_group;
         */
//        taskConfig.setSourceDbEntity(dbConfig);
        // removeDuplicate(taskConfig, uniqueKeys, "", 30, 6, new Date(),
        // DateUtils.getNextDay(new Date()));
    }

    public static void delDataHandler(DelDataHandler delDataHandler) throws Exception {
        TaskPropertiesConfig taskConfig = delDataHandler.getTaskConfig();
        Connection conn = null;
        String fullTable = delDataHandler.getTaskConfig().getSourceTable();
        String tmpTable = delDataHandler.getTaskConfig().getSourceTable() + "_deltmp";
        SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmmss");
        String timeSubixStr = formatter.format(new Date());
        String bakTable = fullTable + "_del_bak" + timeSubixStr;
        try {
            conn = getConn(taskConfig.getTargetDbEntity().getDbName(),taskConfig.getSourceDbEntity().getConnectionUrl(),
                    taskConfig.getSourceDbEntity().getUserName(), taskConfig.getSourceDbEntity().getPassword(), 30);

            // 1.创建临时表和备份表，如果不存在
            String dropTmpTableSql = "DROP TABLE IF EXISTS " + tmpTable;
            logger.info(String.format("=============>drop去重删除修复临时表：【sql=%s】", dropTmpTableSql));
            PreparedStatement dropTmpTableStatement = conn.prepareStatement(dropTmpTableSql);
            executeHiveStatementAndClose(dropTmpTableStatement);

            String createTmpSql = " CREATE TABLE IF NOT EXISTS " + tmpTable + " LIKE " + fullTable;
            logger.info(String.format("=============>创建去重删除修复临时表：【sql=%s】", createTmpSql));
            PreparedStatement createTmpPs = conn.prepareStatement(createTmpSql);
            executeHiveStatementAndClose(createTmpPs);

            String createBakSql = " CREATE TABLE IF NOT EXISTS " + bakTable + " LIKE " + fullTable;
            logger.info(String.format("=============>创建去重删除修复备份表：【sql=%s】", createBakSql));
            PreparedStatement createBakPs = conn.prepareStatement(createBakSql);
            executeHiveStatementAndClose(createBakPs);

            // 2.向备份表插入全部数据
            StringBuilder bakDataHql = new StringBuilder();
            bakDataHql.append("insert overwrite table ").append(bakTable).append(" partition (biz_date) ")
                    .append(" select * FROM " + fullTable);
            logger.info(String.format("=============>备份数据：【sql=%s】", bakDataHql.toString()));
            PreparedStatement bakDataHqlStatement = conn.prepareStatement(bakDataHql.toString());
            executeHiveStatementAndClose(bakDataHqlStatement);

            // 3.将修复的数据回写到去重表
            StringBuilder hql = new StringBuilder();
            String pk = taskConfig.getPrimaryKeys().get(0);
            hql.append("INSERT OVERWRITE TABLE ").append(tmpTable).append(" PARTITION (biz_date) ");
            hql.append(" SELECT t1.* FROM " + fullTable).append(" t1 ");
            hql.append(" LEFT OUTER JOIN (select t2.* from ");
            String delTableName = delDataHandler.getDelDataTbName();
            hql.append(delTableName + "_cln").append(" t2 where ");
            String timeFilter = getTimeFilterDel("t2", delDataHandler.getNotProcessMinTime(),
                    delDataHandler.getNotProcessMaxTime());
            // 时间过滤
            hql.append(timeFilter);
            hql.append(" and  t2.table_name='").append(delDataHandler.getTbName()).append("' ").append(" ) t3 ");
            // 关联字段
            hql.append(" ON (1=1 ");
            hql.append(" AND t1." + pk + " = t3.id_column_value");

            hql.append(" ) WHERE t3.id_column_value IS NULL ");
            logger.info(String.format("进入去重删除修复【%s】时的sql语句：【%s】", fullTable, hql.toString()));

            PreparedStatement hqlStatement = conn.prepareStatement(hql.toString());
            executeHiveStatementAndClose(hqlStatement);
            // 5.drop全量表
            String dropFullTableSql = " DROP TABLE IF EXISTS " + fullTable;
            logger.info(String.format("=============>drop全量表：【sql=%s】", dropFullTableSql));
            PreparedStatement dropFullTableStatement = conn.prepareStatement(dropFullTableSql);
            executeHiveStatementAndClose(dropFullTableStatement);
            // 6.rename tmp表to全量表
            String renameTmpTableSql = " ALTER TABLE  " + tmpTable + " RENAME TO " + fullTable;
            logger.info(String.format("=============>rename临时表to全量表：【sql=%s】", renameTmpTableSql));
            PreparedStatement renameTmpTableStatement = conn.prepareStatement(renameTmpTableSql);
            executeHiveStatementAndClose(renameTmpTableStatement);
            // 5.drop备份表
            String dropBakTableSql = " DROP TABLE IF EXISTS " + bakTable;
            logger.info(String.format("=============>drop备份表：【sql=%s】", dropBakTableSql));
            PreparedStatement dropBakTableStatement = conn.prepareStatement(dropBakTableSql);
            executeHiveStatementAndClose(dropBakTableStatement);

        } catch (SQLException e) {
            logger.error("数据删除修复失败: " + e.getMessage());
            // 6.rename 备份表to全量表
            try {
                String renameBakTableSql = " ALTER TABLE  " + bakTable + " RENAME TO " + fullTable;
                logger.info(String.format("=============>rename备份表to全量表：【sql=%s】", renameBakTableSql));
                PreparedStatement renameBakTableStatement = conn.prepareStatement(renameBakTableSql);
                executeHiveStatementAndClose(renameBakTableStatement);
            } catch (Exception ee) {
                throw new SQLException(e);
            }
            throw new SQLException(e);
        } finally {
            closeHiveConnectionQuietly(conn);
        }

    }

    private static String getTimeFilterDel(String alias, Date notProcessMinTime, Date notProcessMaxTime) {
        StringBuilder whereFieldBuffer = new StringBuilder();
        DateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        ;
        String startTimeStr = sdf.format(notProcessMinTime);
        String endTimeStr = sdf.format(notProcessMaxTime);
        whereFieldBuffer.append(alias);
        whereFieldBuffer.append(".");
        whereFieldBuffer.append("biz_date");
        whereFieldBuffer.append(">=");
        whereFieldBuffer.append(startTimeStr);
        whereFieldBuffer.append(" AND ");
        whereFieldBuffer.append(alias);
        whereFieldBuffer.append(".");
        whereFieldBuffer.append("biz_date");
        whereFieldBuffer.append("<=");
        whereFieldBuffer.append(endTimeStr);
        return whereFieldBuffer.toString();
    }

    public static boolean execute(TaskDatabaseConfig dbConfig, TaskExeSQLPropertiesConfig taskConfig, String sql,
                                  Integer jdbcTimeOut) throws Exception {
        // public static boolean execute(TaskExeSQLPropertiesConfig taskConfig,
        // String sql, Integer jdbcTimeOut) throws Exception{
        Connection conn = null;
        Statement stmt = null;
        try {
            conn = getConn(taskConfig.getTargetDbEntity().getDbName(),dbConfig.getConnectionUrl(), dbConfig.getUserName(), dbConfig.getPassword(), jdbcTimeOut); // 得到数据连接
            // conn = getConn1(); //得到数据连接
            stmt = conn.createStatement();
            boolean rs = stmt.execute(sql); // 执行sql结果
            return rs;
        } catch (SQLException e) {
            logger.error("error during execUpdate hive sql:" + sql, e);
            throw e;
        } finally {
            closeHiveStatementQuietly(stmt);
            closeHiveConnectionQuietly(conn);
        }
    }

    public static boolean executeUpdate(TaskDatabaseConfig dbConfig, TaskExeSQLPropertiesConfig taskConfig, String sql,
                                        Integer jdbcTimeOut) throws Exception {
        // public static boolean executeUpdate(TaskExeSQLPropertiesConfig
        // taskConfig, String sql, Integer jdbcTimeOut) throws Exception{
        Connection conn = null;
        Statement stmt = null;
        try {
            conn = getConn(taskConfig.getTargetDbEntity().getDbName(),dbConfig.getConnectionUrl(), dbConfig.getUserName(), dbConfig.getPassword(), jdbcTimeOut); // 得到数据连接
            // conn = getConn1(); //得到数据连接
            stmt = conn.createStatement();
            int rs = stmt.executeUpdate(sql); // 执行sql结果
            if (rs >= 0) {
                return true;
            }
        } catch (SQLException e) {
            logger.error("error during execUpdate hive sql:" + sql, e);
            throw e;
        } finally {
            closeHiveStatementQuietly(stmt);
            closeHiveConnectionQuietly(conn);
        }
        return false;
    }

    public static ArrayList<Object[]> executeQuery(TaskDatabaseConfig dbConfig, TaskExeSQLPropertiesConfig taskConfig,
                                                   String sql, Integer jdbcTimeOut) throws Exception {
        // public static ArrayList<Object []>
        // executeQuery(TaskExeSQLPropertiesConfig taskConfig, String sql,
        // Integer jdbcTimeOut) throws Exception{
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        ArrayList<Object[]> arrayList = new ArrayList<Object[]>();
        try {
            conn = getConn(dbConfig.getConnectionUrl(), dbConfig.getUserName(), dbConfig.getPassword(), jdbcTimeOut); // 得到数据连接
            // conn = getConn1(); //得到数据连接
            stmt = conn.createStatement();
            rs = stmt.executeQuery(sql);
            ResultSetMetaData rsmd = rs.getMetaData();
            int column = rsmd.getColumnCount();
            while (rs.next()) {
                Object[] ob = new Object[column];
                for (int i = 1; i <= column; i++) {
                    ob[i - 1] = rs.getObject(i);
                }
                arrayList.add(ob);
            }
            return arrayList;
        } catch (SQLException e) {
            logger.error("error during execUpdate hive sql:" + sql, e);
            throw e;
        } finally {
            closeHiveStatementQuietly(stmt);
            closeHiveConnectionQuietly(conn);
            try {
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
                throw e;
            }
        }
    }


    public static void removeDuplicate3(TaskPropertiesConfig taskConfig, String globleHadoopParams, Integer jdbcTimeout,
                                        Integer selectPreMonth, Date... timeSpan) throws Exception {
        /*
         * 初始化变量
         */
        String targetTableName = taskConfig.getTargetTable(); // 清洗的ods表
        String sourceTableName = taskConfig.getSourceTable();// src表
        String startTime = null;// 开始时间
        String endTime = null;// 结束时间
        String partition_date = HiveDefinePartNameEnum.PARTITION_DATE_NAME.getValue();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        if (timeSpan.length != 2) {
            throw new IllegalArgumentException("数据去重失败: 非法参数，需要制定开始和结束时间");
        }
        startTime = sdf.format(timeSpan[0]);
        endTime = sdf.format(timeSpan[1]);
        //获取清洗月份
        List<String> cleanMonthRange = DateUtils.getMonthsByBeginAndEnd(timeSpan[0], timeSpan[1], "yyyyMMdd");
        logger.info(
                "去重时间范围: { " + DateUtils.formatDatetime(startTime) + " - " + DateUtils.formatDatetime(endTime) + " }");
        //获取清洗的占位sql语句
        String placeholderSQL = cleanPlaceholderSQL(taskConfig, cleanMonthRange);
        placeholderSQL = placeholderSQL.replaceAll("src_t.", "");
        List<String> timeColumns = taskConfig.getSyncTimeColumn();// 增量时间字段
        if (timeColumns == null || timeColumns.size() == 0) {
            throw new IllegalArgumentException("数据去重失败: 非法参数，没有指定增量时间字段");
        }

        List<String> pkColumns = taskConfig.getPrimaryKeys();// 主键字段
        if (pkColumns == null || pkColumns.size() == 0) {
            throw new IllegalArgumentException("数据去重失败: 非法参数，没有指定主键");
        }

        Connection sourceConn = null;

        try {
            sourceConn = getConn(taskConfig.getTargetDbEntity().getDbName(),taskConfig.getSourceDbEntity().getConnectionUrl(),
                    taskConfig.getSourceDbEntity().getUserName(), taskConfig.getSourceDbEntity().getPassword(),
                    jdbcTimeout);
            setHadoopParams(globleHadoopParams, taskConfig, sourceConn);
            logger.info(String.format("=======>增量去重逻辑部分：【dbName=%s】【tableName=%s】【uniqueKeys=%s】【url=%s】",
                    taskConfig.getSourceDbEntity().getDbName(), targetTableName, taskConfig.getPrimaryKeysStr(),
                    taskConfig.getSourceDbEntity().getConnectionUrl()));

            /**
             * 清洗的月份计算
             */
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(timeSpan[0]);
            calendar.add(Calendar.MONTH, -selectPreMonth);
            String startMonth = new SimpleDateFormat("yyyyMM").format(calendar.getTime());
            String endMonth = new SimpleDateFormat("yyyyMM").format(timeSpan[1]);

            // 1.将去重后的数据回写到去重表

            StringBuilder keyStrs = new StringBuilder();
            StringBuilder keyThlStrs = new StringBuilder();
            for (String key : taskConfig.getPrimaryKeys()) {
                keyStrs.append(" and src_t." + key + " = stn." + key);
            }
            // THL中捕获联合主键时，采用逗号连接各个值（要保证xml中的primaryKey联合主键顺序与binlogCapture中的一致）
            if (taskConfig.getPrimaryKeys().size() > 1) {
                keyThlStrs.append(" and concat(src_t.")
                        .append(taskConfig.getPrimaryKeysStr().replace(",", ",',',src_t.")).append(")")
                        .append(" = rgibt.").append(HiveDefinePartNameEnum.CDC_TABLE_ID_COLUMN_VALUE.getValue());
            } else {
                keyThlStrs.append(" and src_t.").append(taskConfig.getPrimaryKeysStr()).append(" = rgibt.")
                        .append(HiveDefinePartNameEnum.CDC_TABLE_ID_COLUMN_VALUE.getValue());
            }
            String cleanColumnsStr = taskConfig.getSelectColumnsStr();
            String cleanPkColumn = cleanColumnsStr.split(",")[0];
            // 1.清洗环节
            StringBuilder hql = new StringBuilder();
            hql.append("insert overwrite TABLE ").append(taskConfig.getTargetDbEntity().getDbName()).append(".")
                    .append(targetTableName).append(" partition(")
                    .append(HiveDefinePartNameEnum.PARTITION_DATE_NAME.getValue()).append(") select ")
                    .append(cleanColumnsStr.replace("src_t.partition_date",
                            "date_format(src_t." + taskConfig.getSyncTimeColumn().get(0)))
                    .append(",'yyyyMM') as partition_date from ").append(taskConfig.getTargetDbEntity().getDbName())
                    .append(".").append(targetTableName).append(" src_t left join ")
                    .append(taskConfig.getSourceDbEntity().getDbName()).append(".").append(sourceTableName)
                    .append(" stn on (stn.").append(partition_date).append(" >= '").append(startTime)
                    .append("' and stn.").append(partition_date).append(" <= '").append(endTime).append("' ")
                    .append(keyStrs).append(" ) left join ").append(taskConfig.getSourceDbEntity().getDbName())
                    .append(".")
                    .append(sourceTableName.replace(HiveDefinePartNameEnum.SRC_TABLE_NAME_SUBFIX.getValue(),
                            HiveDefinePartNameEnum.THL_TABLE_NAME_SUBFIX.getValue()))
                    .append(" rgibt on (rgibt.").append(partition_date).append(" >= ").append(startTime)
                    .append(" and rgibt.").append(partition_date).append(" <= ").append(endTime).append(" ")
                    .append(keyThlStrs).append(" ) where src_t.").append(partition_date).append(" >= ")
                    .append(startMonth).append(" and src_t.").append(partition_date).append(" <= ").append(endMonth)
                    .append(" and stn.").append(taskConfig.getPrimaryKeys().get(0)).append(" is null and rgibt.")
                    .append(HiveDefinePartNameEnum.CDC_TABLE_ID_COLUMN_VALUE.getValue()).append(" is null ")
                    .append(taskConfig.getFilterConditionsStr())
                    //过滤占位记录
                    .append("and " + cleanPkColumn + " not in (-999999)").append(" union all select ")
                    .append(taskConfig.getSelectColumnsStr()).append(" from (select ")
                    .append(taskConfig.getSelectColumnsStr().replace("src_t.partition_date",
                            "date_format(src_t." + taskConfig.getSyncTimeColumn().get(0)))
                    .append(",'yyyyMM') as partition_date,row_number() over(partition by ")
                    .append(taskConfig.getPrimaryKeysStr()).append(" order by ")
                    .append(taskConfig.getSyncTimeColumnStr()).append(" desc ) rm from ")
                    .append(taskConfig.getSourceDbEntity().getDbName()).append(".").append(sourceTableName)
                    .append(" src_t where src_t.").append(partition_date).append(" >= ").append(startTime)
                    .append(" and src_t.").append(partition_date).append(" <= ").append(endTime).append(" ")
                    .append(taskConfig.getFilterConditionsStr()).append(") src_t where src_t.rm = 1");


            //hql.append("insert overwrite table test2.tb_partition partition(partition_date) select 1, 'aaaa' ,201707 ");


            logger.info(String.format("1.进入去重表【%s】时的sql语句：【%s】", targetTableName, hql.append(placeholderSQL).toString()));

            PreparedStatement hqlStatement = sourceConn.prepareStatement(hql.toString());
            executeHiveStatementAndClose(hqlStatement);

            logger.info(String.format("----ODS表【%s】清洗完成！", targetTableName));

        } catch (Throwable e) {
            logger.error("数据去重失败: " + e.getMessage());
            throw new SQLException(e);
        } finally {
            closeHiveConnectionQuietly(sourceConn);
        }
    }

    /**
     * @param taskConfig 任务配置信息
     * @param cleanRange 清洗范围
     * @return
     */
    public static String cleanPlaceholderSQL(TaskPropertiesConfig taskConfig, List<String> cleanRange) {
        //获取清洗字段
        String cleanColumnsStr = taskConfig.getSelectColumnsStr();
        StringBuilder specialSQL = new StringBuilder();
        if (null != cleanColumnsStr) {
            String[] cleanColumnsArr = cleanColumnsStr.split(",");
            for (String cleanMonth : cleanRange) {
                specialSQL.append(" union all select ");
                for (int i = 0; i < cleanColumnsArr.length; i++) {
                    if (i == 0) {
                        specialSQL.append("-999999 as " + cleanColumnsArr[i] + ",");
                    } else if (cleanColumnsArr[i].trim().startsWith("from_unixtime")) {
                        continue;
                    } else if (cleanColumnsArr[i].trim().contains("ods_update_time")) {
                        specialSQL.append("null as ods_update_time ,");
                    } else if (!cleanColumnsArr[i].equals("src_t.partition_date")) {
                        specialSQL.append("null as " + cleanColumnsArr[i] + ",");
                    } else {
                        specialSQL.append(cleanMonth + " as " + cleanColumnsArr[i]);
                    }
                }
            }
            return specialSQL.toString();
        } else {
            logger.error("数据去重失败: " + new Exception());
        }
        return "";
    }

    public static Map<String, String> generateSqoopProperties(String exportParams) {
        Map<String, String> res = new HashMap<String, String>();
        if (exportParams.contains(",")) {
            String[] exportParam = exportParams.split(",");
            for (String keyValueParam : exportParam) {
                String[] keyValues = keyValueParam.split("=");
                if (keyValues.length != 2) {
                    logger.warn(
                            "在bdp-dht.properties中读取 【dc.import.params 或dc.export.params】 参数无效！eg:【dc.export.params=--escaped-by=\\006,--enclosed-by=\\006】");
                    continue;
                }
                logger.info("在bdp-dht.properties中读取 【dc.import.params 或dc.export.params】 参数有效！key =" + keyValues[0]
                        + " value =" + keyValues[1]);
                if (keyValues[0].trim().equals("mapred.job.name") || keyValues[0].trim().equals("mapreduce.job.queuename")) {
                    res.put(keyValues[0], keyValues[1]);
                }
            }
        }
        return res;
    }


    /**
     * 设置sqoop导入参数
     *
     * @param taskConfig
     * @param params
     */
    public static void setSqoopParamProperties(TaskPropertiesConfig taskConfig, Map<String, String> params,
                                               String exportParams, SqoopParams sqoopMap) {
        if (taskConfig == null || params == null)
            return;
        Map<String, String> res = new HashMap<String, String>();
        if (exportParams.contains(",")) {
            String[] exportParam = exportParams.split(",");
            for (String keyValueParam : exportParam) {
                String[] keyValues = keyValueParam.split("=");
                if (keyValues.length != 2) {
                    logger.warn(
                            "在bdp-dht.properties中读取 【dc.import.params 或dc.export.params】 参数无效！eg:【dc.export.params=--escaped-by=\\006,--enclosed-by=\\006】");
                    continue;
                }
                logger.info("在bdp-dht.properties中读取 【dc.import.params 或dc.export.params】 参数有效！key =" + keyValues[0]
                        + " value =" + keyValues[1]);
                if (keyValues[0].trim().equals("mapred.job.name") || keyValues[0].trim().equals("mapreduce.job.queuename")) {
                    Date date = new Date();
                    // currentTime 开始日期
                    Calendar beginDate = Calendar.getInstance();
                    beginDate.setTime(date);
                    String currentTime = format.format(date.getTime());
                    String jobName = String.format("%s-%s-%s_%s", keyValues[1].trim(), taskConfig.getGroupName(),
                            taskConfig.getTriggerName(), System.currentTimeMillis());
                    res.put(keyValues[0].trim(), jobName);
                    sqoopMap.setProperties(res);
                    logger.info("在bdp-dht.properties中读设置jobName！key =" + keyValues[0] + " value =" + jobName);
                } else {
                    params.put(keyValues[0].trim(), keyValues[1].trim());
                    logger.info("在bdp-dht.properties中读设置jobName！key =" + keyValues[0].trim() + " value ="
                            + keyValues[1].trim());
                }
//                params.put(keyValues[0], keyValues[1]);
            }
        }

        Map<String, String> sqoopParam = taskConfig.getSqoopParam();
        sqoopParam = sqoopParam == null || sqoopParam.size() < 1 ? null : sqoopParam;

        if (sqoopParam == null) {
            String message = String.format("未设置sqoop参数！ 【groupName：%s】【triggerName：%s】", taskConfig.getGroupName(),
                    taskConfig.getTriggerName());
            logger.info(message);
            return;
        }

        String sqoopParams = System.lineSeparator();

        String operator = "=";
        String message = "";

        String paramKey = "";
        String paramValue = "";

        for (Map.Entry<String, String> entry : sqoopParam.entrySet()) {
            paramKey = "--";
            paramKey += entry.getKey().trim();
            paramValue = entry.getValue().trim();
            String command = "";
            command += paramKey;
            command += operator;
            command += paramValue;
            command += System.lineSeparator();
            if (params.containsKey(entry.getKey().trim())) {
                message = String.format(
                        "更新bdp-dht.properties中的sqoop参数！ 从配置文件【groupName：%s】【triggerName：%s】中读取【key = %s】【value = %s】",
                        taskConfig.getGroupName(), taskConfig.getTriggerName(), entry.getKey().trim(),
                        entry.getValue().trim());
            } else {
                message = String.format("设置sqoop参数！ 从配置文件【groupName：%s】【triggerName：%s】中读取【key = %s】【value = %s】",
                        taskConfig.getGroupName(), taskConfig.getTriggerName(), entry.getKey().trim(),
                        entry.getValue().trim());
            }
            logger.info(message);
            params.put(paramKey, paramValue);

            sqoopParams += command;
            sqoopParams += System.lineSeparator();
        }
        if (sqoopParams.length() >= 1) {
            message = String.format("设置sqoop参数！ 【groupName：%s】【triggerName：%s】【%s】", taskConfig.getGroupName(),
                    taskConfig.getTriggerName(), sqoopParams);
            logger.info(message);
        }
    }

    public static boolean execUpdate(TaskDatabaseConfig dbConfig, TaskPropertiesConfig taskConfig,
                                     String globleHadoopParams, String sql, Integer jdbcTimeOut
            , RemoteJobInvokeParamsDto remoteJobInvokeParamsDto, String jobId) throws Exception {
        Connection conn = null;
        Statement stmt = null;
        try {
            conn = getConn(taskConfig.getTargetDbEntity().getDbName(),dbConfig.getConnectionUrl(), dbConfig.getUserName(), dbConfig.getPassword(), jdbcTimeOut);
            // String groupName = taskConfig.getGroupName();
            // if (!groupName.equals("gtp_kettle")) {// gtp_kettle组名的任务不设置
            setHadoopParams(globleHadoopParams, taskConfig, conn, remoteJobInvokeParamsDto, jobId);
            // }
            stmt = conn.createStatement();
            int rs = stmt.executeUpdate(sql);
            if (rs >= 0) {
                return true;
            } else {
                return false;
            }
        } catch (SQLException e) {
            logger.error("error during execUpdate hive sql:" + sql, e);
            throw e;
        } finally {
            closeHiveStatementQuietly(stmt);
            closeHiveConnectionQuietly(conn);
        }
    }


    /**
     * 设置hadoop集群参数
     *
     * @param taskConfig
     * @param conn
     */
    public static void setHadoopParams(String globleHadoopParams,
                                       TaskPropertiesConfig taskConfig, Connection conn,
                                       RemoteJobInvokeParamsDto remoteJobInvokeParamsDto,
                                       String jobId)
            throws SQLException {
        if (taskConfig == null || conn == null || globleHadoopParams == null)
            return;
        Map<String, String> hadoopParamsMap = new LinkedHashMap<String, String>();
        logger.info("globleHadoopParams:" + globleHadoopParams);
        if (globleHadoopParams.contains(",")) {
            String[] hadoopParams = globleHadoopParams.split(",");
            for (String keyValueParam : hadoopParams) {
                String[] keyValues = keyValueParam.split("=");
                if (keyValues.length != 2) {
                    logger.warn(
                            "在bdp-dht.properties中读取 【dc.hadoop.thl.params 或dc.hadoop.clnd.params】 参数无效！eg:【dc.export.params=--escaped-by=\\006,--enclosed-by=\\006】");
                    continue;
                }
                logger.info("在bdp-dht.properties中读取 【dc.hadoop.thl.params 或dc.hadoop.thl.params】 参数有效！key ="
                        + keyValues[0] + " value =" + keyValues[1]);
                if (keyValues[0].trim().contains("mapred.job.name")) {
                    Date date = new Date();
                    // currentTime 开始日期
                    Calendar beginDate = Calendar.getInstance();
                    beginDate.setTime(date);
                    String currentTime = format.format(date.getTime());
                    String jobName = String.format("%s-%s-%s_%s", keyValues[1].trim(), taskConfig.getGroupName(),
                            taskConfig.getTriggerName(), System.currentTimeMillis());
                    hadoopParamsMap.put(keyValues[0].trim(), jobName);

                    logger.info("在bdp-dht.properties中读设置jobName！key =" + keyValues[0] + " value =" + jobName);
                } else {
                    hadoopParamsMap.put(keyValues[0].trim(), keyValues[1].trim());
                }
            }
        }
        Map<String, String> hiveParam = taskConfig.getHiveParam();
        hiveParam = hiveParam == null || hiveParam.size() < 1 ? null : hiveParam;

        // if(hiveParam == null) return;

        String haveParams = System.lineSeparator();

        String operator = "=";
        Statement state = null;
        String message = "";
        try {
            state = conn.createStatement();

            String paramKey = "";
            String paramValue = "";
            // 将单独的map key value 全部设置到groubleHadoopParamsMap
            if (hiveParam != null) {
                for (Map.Entry<String, String> entry : hiveParam.entrySet()) {
                    if (hadoopParamsMap.containsKey(entry.getKey().trim())) {
                        message = String.format(
                                "更新bdp-dht.properties中的sqoop参数！ 从配置文件【groupName：%s】【triggerName：%s】中读取【key = %s】【value = %s】",
                                taskConfig.getGroupName(), taskConfig.getTriggerName(), entry.getKey().trim(),
                                entry.getValue().trim());
                        logger.info(message);
                    } else {
                        message = String.format(
                                "设置sqoop参数！ 从配置文件【groupName：%s】【triggerName：%s】中读取【key = %s】【value = %s】",
                                taskConfig.getGroupName(), taskConfig.getTriggerName(), entry.getKey().trim(),
                                entry.getValue().trim());
                    }
                    hadoopParamsMap.put(entry.getKey().trim(), entry.getValue().trim());
                    logger.info(message);
                }
            }
            // 遍历groubleHadoopParamsMap
            for (Map.Entry<String, String> entry : hadoopParamsMap.entrySet()) {
                paramKey = entry.getKey().trim();
                paramValue = entry.getValue().trim();
                String command = "set ";
                command += paramKey;
                command += operator;
                command += paramValue;
                command += System.lineSeparator();
                state.execute(command);

                haveParams += command;
                haveParams += System.lineSeparator();
            }
            if (haveParams.length() >= 1) {
                message = String.format("通过 HiveServer2 设置集群参数 成功！ 【groupName：%s】【triggerName：%s】【%s】",
                        taskConfig.getGroupName(), taskConfig.getTriggerName(), haveParams);
                logger.info(message);
            }
        } catch (SQLException e) {
            message = String.format("通过 HiveServer2 设置集群参数 失败！ 【groupName：%s】【triggerName：%s】【%s】",
                    taskConfig.getGroupName(), taskConfig.getTriggerName(), e.getMessage());
            logger.error(message);
        } finally {
            if (state != null && false == state.isClosed()) {
                try {
                    state.close();
                } catch (SQLException e) {
                    logger.error(String.format("============initConfig【关闭statement失败】", e.getMessage()));
                }
            }
        }
    }

    public static boolean execUpdate(TaskDatabaseConfig dbConfig, TaskPropertiesConfig taskConfig,
                                     String globleHadoopParams,
                                     String sql, Integer jdbcTimeOut, String jobName) throws Exception {
        Connection conn = null;
        Statement stmt = null;
        try {
            conn = getConn(taskConfig.getTargetDbEntity().getDbName(),dbConfig.getConnectionUrl(), dbConfig.getUserName(), dbConfig.getPassword(), jdbcTimeOut);
            // String groupName = taskConfig.getGroupName();
            // if (!groupName.equals("gtp_kettle")) {// gtp_kettle组名的任务不设置
            setHadoopParams(globleHadoopParams, taskConfig, conn, jobName);
            // }
            stmt = conn.createStatement();
            int rs = stmt.executeUpdate(sql);
            if (rs >= 0) {
                return true;
            } else {
                return false;
            }
        } catch (SQLException e) {
            logger.error("error during execUpdate hive sql:" + sql, e);
            throw e;
        } finally {
            closeHiveStatementQuietly(stmt);
            closeHiveConnectionQuietly(conn);
        }
    }

    /**
     * 设置hadoop集群参数
     *
     * @param taskConfig
     * @param conn
     */
    public static void setHadoopParams(String globleHadoopParams,
                                       TaskPropertiesConfig taskConfig, Connection conn, String jobName)
            throws SQLException {
        if (taskConfig == null || conn == null || globleHadoopParams == null)
            return;
        Map<String, String> hadoopParamsMap = new LinkedHashMap<String, String>();
        logger.info("globleHadoopParams:" + globleHadoopParams);
        if (globleHadoopParams.contains(",")) {
            String[] hadoopParams = globleHadoopParams.split(",");
            for (String keyValueParam : hadoopParams) {
                String[] keyValues = keyValueParam.split("=");
                if (keyValues.length != 2) {
                    logger.warn(
                            "在bdp-dht.properties中读取 【dc.hadoop.thl.params 或dc.hadoop.clnd.params】 参数无效！eg:【dc.export.params=--escaped-by=\\006,--enclosed-by=\\006】");
                    continue;
                }
                logger.info("在bdp-dht.properties中读取 【dc.hadoop.thl.params 或dc.hadoop.thl.params】 参数有效！key ="
                        + keyValues[0] + " value =" + keyValues[1]);
                if (keyValues[0].trim().contains("mapred.job.name")) {
                    Date date = new Date();
                    // currentTime 开始日期
                    Calendar beginDate = Calendar.getInstance();
                    beginDate.setTime(date);
                    String currentTime = format.format(date.getTime());
                    hadoopParamsMap.put(keyValues[0].trim(), jobName);

                    logger.info("在bdp-dht.properties中读设置jobName！key =" + keyValues[0] + " value =" + jobName);
                } else {
                    hadoopParamsMap.put(keyValues[0].trim(), keyValues[1].trim());
                }
            }
        }
        Map<String, String> hiveParam = taskConfig.getHiveParam();
        hiveParam = hiveParam == null || hiveParam.size() < 1 ? null : hiveParam;

        // if(hiveParam == null) return;

        String haveParams = System.lineSeparator();

        String operator = "=";
        Statement state = null;
        String message = "";
        try {
            state = conn.createStatement();

            String paramKey = "";
            String paramValue = "";
            // 将单独的map key value 全部设置到groubleHadoopParamsMap
            if (hiveParam != null) {
                for (Map.Entry<String, String> entry : hiveParam.entrySet()) {
                    if (hadoopParamsMap.containsKey(entry.getKey().trim())) {
                        message = String.format(
                                "更新bdp-dht.properties中的sqoop参数！ 从配置文件【groupName：%s】【triggerName：%s】中读取【key = %s】【value = %s】",
                                taskConfig.getGroupName(), taskConfig.getTriggerName(), entry.getKey().trim(),
                                entry.getValue().trim());
                        logger.info(message);
                    } else {
                        message = String.format(
                                "设置sqoop参数！ 从配置文件【groupName：%s】【triggerName：%s】中读取【key = %s】【value = %s】",
                                taskConfig.getGroupName(), taskConfig.getTriggerName(), entry.getKey().trim(),
                                entry.getValue().trim());
                    }
                    hadoopParamsMap.put(entry.getKey().trim(), entry.getValue().trim());
                    logger.info(message);
                }
            }
            // 遍历groubleHadoopParamsMap
            for (Map.Entry<String, String> entry : hadoopParamsMap.entrySet()) {
                paramKey = entry.getKey().trim();
                paramValue = entry.getValue().trim();
                String command = "set ";
                command += paramKey;
                command += operator;
                command += paramValue;
                command += System.lineSeparator();
                state.execute(command);

                haveParams += command;
                haveParams += System.lineSeparator();
            }
            if (haveParams.length() >= 1) {
                message = String.format("通过 HiveServer2 设置集群参数 成功！ 【groupName：%s】【triggerName：%s】【%s】",
                        taskConfig.getGroupName(), taskConfig.getTriggerName(), haveParams);
                logger.info(message);
            }
        } catch (SQLException e) {
            message = String.format("通过 HiveServer2 设置集群参数 失败！ 【groupName：%s】【triggerName：%s】【%s】",
                    taskConfig.getGroupName(), taskConfig.getTriggerName(), e.getMessage());
            logger.error(message);
        } finally {
            if (state != null && false == state.isClosed()) {
                try {
                    state.close();
                } catch (SQLException e) {
                    logger.error(String.format("============initConfig【关闭statement失败】", e.getMessage()));
                }
            }
        }

    }
}
