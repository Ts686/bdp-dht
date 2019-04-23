package cn.wonhigh.dc.client.common.util;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import cn.wonhigh.dc.client.common.constans.MessageConstant;
import com.alibaba.druid.pool.DruidDataSource;
import com.yougou.logistics.base.common.utils.EncryptionUtils;
import org.apache.log4j.Logger;

import cn.wonhigh.dc.client.common.model.TaskDatabaseConfig;
import cn.wonhigh.dc.client.common.model.TaskProPropertiesConfig;
import cn.wonhigh.dc.client.common.model.TaskPropertiesConfig;

/**
 * pg工具类
 *
 * @author jiang.pl
 */
public class PgSqlUtils {

    private static final Logger logger = Logger.getLogger(PgSqlUtils.class);
    private static String jdbcUser = "root";
    private static Integer jdbcTimeout = 30;
    private static Connection ct = null;
    private static PreparedStatement ps = null;
    private static ResultSet rs = null;

    private static Properties properties = null;
    private static DruidDataSource dataSourceDcdb = null;
    private static DruidDataSource dataSourceSports = null;

    static {
        //初始化hive的durid线程池
        properties = PropertyFile.getProps("");
        dataSourceDcdb = new DruidDataSource();

        dataSourceDcdb.setUrl(properties.getProperty("postgresql.db.url.dcdb"));
        dataSourceDcdb.setDriverClassName(properties.getProperty("postgresql.db.driverClass"));
        dataSourceDcdb.setUsername(properties.getProperty("postgresql.db.dcdb.username"));
        dataSourceDcdb.setPassword(properties.getProperty("postgresql.db.dcdb.password"));
        dataSourceDcdb.setTestWhileIdle(Boolean.valueOf(properties.getProperty("hive.testWhileIdle")));
        dataSourceDcdb.setValidationQuery(properties.getProperty("hive.validationQuery"));
        dataSourceDcdb.setMaxActive(Integer.valueOf(properties.getProperty("hive.max.active")));
        dataSourceDcdb.setInitialSize(Integer.valueOf(properties.getProperty("hive.initialSize")));
        dataSourceDcdb.setRemoveAbandoned(Boolean.valueOf(properties.getProperty("hive.removeAbandoned")));
        dataSourceDcdb.setRemoveAbandonedTimeout(Integer.valueOf(properties.getProperty("hive.removeAbandonedTimeout")));

        dataSourceSports = new DruidDataSource();
        dataSourceSports.setUrl(properties.getProperty("postgresql.db.url.sports"));
        dataSourceSports.setDriverClassName(properties.getProperty("postgresql.db.driverClass"));
        dataSourceSports.setUsername(properties.getProperty("postgresql.db.sports.username"));
        dataSourceSports.setPassword(properties.getProperty("postgresql.db.sports.password"));
        dataSourceSports.setTestWhileIdle(Boolean.valueOf(properties.getProperty("hive.testWhileIdle")));
        dataSourceSports.setValidationQuery(properties.getProperty("hive.validationQuery"));
        dataSourceSports.setMaxActive(Integer.valueOf(properties.getProperty("hive.max.active")));
        dataSourceSports.setInitialSize(Integer.valueOf(properties.getProperty("hive.initialSize")));
        dataSourceSports.setRemoveAbandoned(Boolean.valueOf(properties.getProperty("hive.removeAbandoned")));
        dataSourceSports.setRemoveAbandonedTimeout(Integer.valueOf(properties.getProperty("hive.removeAbandonedTimeout")));


    }

    /**
     * 从druid连接池中获取对应连接
     *
     * @param dataSource
     * @return
     */
    public static Connection getPostgresqlConnectionFromDruid(DruidDataSource dataSource) {

        Connection connection = null;

        try {
            connection = dataSource.getConnection();
        } catch (SQLException e) {
            logger.error("从druid连接池中获取postgresql连接失败。");
            e.printStackTrace();
        }

        return connection;
    }

    public static Connection getConn(String url, String user, String passwd)
            throws SQLException {
        logger.debug("postsql JDBC连接信息: " + url);
        Connection connection = null;
        if (url.contains("dc_sports")) {
            connection = getPostgresqlConnectionFromDruid(dataSourceSports);
            logger.info("获取dc_sports库的pg连接池连接");

        } else if (url.contains("dc_db")) {
            connection = getPostgresqlConnectionFromDruid(dataSourceDcdb);
            logger.info("获取dc_db库的pg连接池连接");

        }

        if (null != connection) {
            return connection;

        }

        logger.info("postsql连接池获取失败，直接创建连接");
        DriverManager.setLoginTimeout(jdbcTimeout);
        return DriverManager.getConnection(url, user == null ? jdbcUser : user, passwd == null ? "" : passwd);
    }

    /**
     * 执行pgsql 存储过程
     */
    public static int execPgPro(ArrayList<Object> array, TaskDatabaseConfig dbConfig, TaskProPropertiesConfig task) {
        Connection conn = null;
        try {
            conn = getConn(dbConfig.getConnectionUrl(), dbConfig.getUserName(), dbConfig.getPassword());
            String pstr = "";
            for (int j = 0; j < array.size(); j++) {
                pstr += "?,";
            }
            CallableStatement callStmt = conn.prepareCall("{call " + task.getPrcedurename() + "(" + pstr + "?)}");
            // 参数index从1开始，依次 1,2,3...
            // callStmt.setDate(1,  datadate);
            for (int i = 1; i <= array.size(); i++) {
                callStmt.setObject(i, array.get(i - 1));
            }
            callStmt.registerOutParameter(2, java.sql.Types.INTEGER);
            callStmt.execute();
            int i = callStmt.getInt(2);
            callStmt.close();
            conn.close();
            return i;
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return 0;
        }
    }

    public static void simpleTest(TaskDatabaseConfig dbConfig, String tableName)
            throws SQLException {
        Connection conn = getConn(dbConfig.getConnectionUrl(), dbConfig.getUserName(), dbConfig.getPassword());
        CallableStatement callStmt = conn.prepareCall("{call test_pro(?,?)}");
        // 参数index从1开始，依次 1,2,3...
        callStmt.setDate(1, new java.sql.Date(0));
        callStmt.registerOutParameter(2, java.sql.Types.INTEGER);
        callStmt.execute();
        int i = callStmt.getInt(2);
        System.out.println(i);
        callStmt.close();
        conn.close();
    }

    /**
     * 执行sql
     */
    public static boolean execSql(TaskDatabaseConfig dbConfig, String sql)
            throws SQLException {
        Connection conn = getConn(dbConfig.getConnectionUrl(), dbConfig.getUserName(), dbConfig.getPassword());
        Statement Stm = conn.createStatement();
        int result = Stm.executeUpdate(sql);
        Stm.close();
        conn.close();
        if (result >= 0) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * 计算日期差
     *
     * @param smdate
     * @param bdate
     * @return
     * @throws ParseException
     */
    public static int daysBetween(Date smdate, Date bdate) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        smdate = sdf.parse(sdf.format(smdate));
        bdate = sdf.parse(sdf.format(bdate));
        Calendar cal = Calendar.getInstance();
        cal.setTime(smdate);
        long time1 = cal.getTimeInMillis();
        cal.setTime(bdate);
        long time2 = cal.getTimeInMillis();
        long between_days = (time2 - time1) / (1000 * 3600 * 24);
        return Integer.parseInt(String.valueOf(between_days));
    }

    /**
     * 得到下一个执行周期时间
     *
     * @param args
     * @throws Exception
     */
    public static Date getNextFormatDate(Date date) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        try {
            date = sdf.parse(sdf.format(date));
        } catch (ParseException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        cal.add(Calendar.DATE, 1);
        return cal.getTime();
    }

    public static Date getPreFormatDate(Date date) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        try {
            date = sdf.parse(sdf.format(date));
        } catch (ParseException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        cal.add(Calendar.DATE, -1);
        return cal.getTime();
    }

    public static Date getFormatDate(Date date) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        try {
            date = sdf.parse(sdf.format(date));
        } catch (ParseException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return date;
    }


    public static void main(String[] args) throws Exception {
//		TaskPropertiesConfig taskConfig = new TaskPropertiesConfig();
//		List<String> list = new ArrayList<String>();
//		list.add("create_time");
//		// list.add("update_time");
//		taskConfig.setSyncTimeColumn(list);
//		// Hive 数据库连接信息
//		TaskDatabaseConfig dbConfig = new TaskDatabaseConfig();
//		dbConfig.setIpAddr("172.17.209.165");
//		dbConfig.setPort("5432");
//		dbConfig.setUserName("usr_dc_ods");
//		dbConfig.setPassword("usr_dc_ods");
//		dbConfig.setDbType(1);
//		dbConfig.setCharset("utf-8");
//		dbConfig.setDbName("dc_pg");
//		String tableName = "ztest";
//		//String[] uniqueKeys = new String[] { "group_no" };
//        simpleTest(dbConfig, tableName);
//		System.out.println(dbConfig.getConnectionUrl());

//		Connection postgresqlConnectionFromDruid = getPostgresqlConnectionFromDruid(dataSourceDcdb);
//		System.out.println(postgresqlConnectionFromDruid);
//
//
//		Connection sp = getPostgresqlConnectionFromDruid(dataSourceSports);
//		System.out.println(sp);

        Connection conn1 = getConn("jdbc:postgresql://10.240.12.21:5432/dc_db",
                "usr_dc_ods", "ScTLNUXy");
        System.out.println(conn1);

        Connection conn2 = getConn("jdbc:postgresql://10.240.12.38:5432/dc_sports",
                "sports_dc_rw", "aUhyT78I");
        System.out.println(conn2);

//		Connection conn = getConn("jdbc:postgresql://172.17.209.1:5432/postgres",
//				"postgres", "Pg2018J");
//		System.out.println(conn);


    }

    public static Connection getCt() {
        return ct;
    }


    public static PreparedStatement getPs() {
        return ps;
    }


    public static ResultSet getRs() {
        return rs;
    }

    public static int executeUpdate(TaskDatabaseConfig dbConfig, String sql, String[] parameters) {
        try {
            ct = getConn(dbConfig.getConnectionUrl(), dbConfig.getUserName(), dbConfig.getPassword());
            ps = ct.prepareStatement(sql);
            if (parameters != null && !parameters.equals("")) {
                for (int i = 0; i < parameters.length; i++) {
                    ps.setString(i + 1, parameters[i]);
                }
            }
            int result = ps.executeUpdate();
            if (parameters.length >= 2) {
                logger.info(String.format("更新完成表【all_task_latest_status】的状态 %d，sql=【%s】" +
                        "【groupName=%s】【schedulerName=%s】！", result, sql, parameters[0], parameters[1]));
            }

            return result;
        } catch (SQLException e) {
            e.printStackTrace();
            logger.debug("JDBC executeSql执行异常: " + e.getMessage());
            logger.error(String.format("JDBC executeSql执行异常:%s  sql=【%s】" +
                    "【groupName=%s】【schedulerName=%s】！", e.getMessage(), sql, parameters[0], parameters[1]));
            throw new RuntimeException(e.getMessage());
        } finally {
            close(rs, ps, ct);
        }
    }


    public static ResultSet executeQuery(TaskDatabaseConfig dbConfig, String sql, String[] parameters) {

        try {
            ct = getConn(dbConfig.getConnectionUrl(), dbConfig.getUserName(), dbConfig.getPassword());
            ps = ct.prepareStatement(sql);
            if (parameters != null && !parameters.equals("")) {
                for (int i = 0; i < parameters.length; i++) {
                    ps.setString(i + 1, parameters[i]);
                }
            }
            logger.info("任务表执行更新/插入开始");
            rs = ps.executeQuery();
            logger.info("任务表执行更新/插入完成 ");
            return rs;
        } catch (Exception e) {
            e.printStackTrace();
            logger.debug("JDBC executeQuery执行异常: " + e.getMessage());
            throw new RuntimeException(e.getMessage());
        } finally {
        }
    }


    /**
     * 关闭连接资源
     *
     * @param rs
     * @param ps
     * @param ct
     */
    public static void close(ResultSet rs, PreparedStatement ps, Connection ct) {

        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
                logger.debug("JDBC ResultSet关闭失败: " + e.getMessage());
            }
            rs = null;
        }
        if (ps != null) {
            try {
                ps.close();
            } catch (SQLException e) {
                e.printStackTrace();
                logger.debug("JDBC Statement关闭失败: " + e.getMessage());
            }
            ps = null;
        }
        if (ct != null) {
            try {
                ct.close();
            } catch (SQLException e) {
                e.printStackTrace();
                logger.debug("JDBC Connection关闭失败: " + e.getMessage());
            }
            ct = null;
        }
    }

}