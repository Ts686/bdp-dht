package cn.wonhigh.dc.client.common.util;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import cn.wonhigh.dc.client.common.model.DelDataHandler;
import cn.wonhigh.dc.client.common.model.TaskDatabaseConfig;
import cn.wonhigh.dc.client.common.model.TaskPropertiesConfig;

/**
 * TODO: 增加描述
 * 
 * @author wangl
 * @date 2016年1月19日 上午11:28:30
 * @version 0.9.9
 * @copyright wonhigh.cn 
 */
public class DbUtils {
	private static final Logger logger = Logger.getLogger(DbUtils.class);

	public static List<DelDataHandler> getDelDatHandl(String driverClassName, String connectionUrl, String userName,
			String password, String targetTable) throws SQLException {
		ResultSet rs = null;
		PreparedStatement ps = null;
		Connection conn = getConn(driverClassName, connectionUrl, userName, password);
		String sql = "select table_name tbName,max(yw_update_time) maxTime,min(yw_update_time) minTime from "+targetTable +" where process_status=0 group by table_name"; 
		ps = conn.prepareStatement(sql);

		logger.info("开始执行sql："+sql);
		try {
			rs = ps.executeQuery();
		} catch (SQLException e) {
			logger.error(e);
			throw e;
		}
		List<DelDataHandler> delDataHandList = new ArrayList<DelDataHandler>();
		while (rs.next()) {
			DelDataHandler delHandLer = new DelDataHandler();
			delHandLer.setTbName(rs.getString("tbName"));
			delHandLer.setNotProcessMaxTime(new java.util.Date(rs.getTimestamp("maxTime").getTime()));
			delHandLer.setNotProcessMinTime(new java.util.Date(rs.getTimestamp("minTime").getTime()));
			delHandLer.setDelDataTbName(targetTable);
			delDataHandList.add(delHandLer);
		}

		if (rs != null) {
			try {
				rs.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		if (ps != null) {
			try {
				ps.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return delDataHandList;
	}

	private static void setNotProcessMaxTime(Date date) {
		// TODO Auto-generated method stub

	}

	public static Connection getConn(String driver, String url, String user, String passwd) throws SQLException {
		try {
			Class.forName(driver);
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
		DriverManager.setLoginTimeout(30);
		return DriverManager.getConnection(url, user == null ? "root" : user, passwd == null ? "" : passwd);
	}

	public static void delDataHandler(DelDataHandler delDataHandler) throws SQLException {
		TaskPropertiesConfig taskConfig = delDataHandler.getTaskConfig();
		TaskDatabaseConfig targetDbEntity = taskConfig.getTargetDbEntity();
		Connection conn = getConn(targetDbEntity.getDriverClassName(), targetDbEntity.getConnectionUrl(),
				targetDbEntity.getUserName(), targetDbEntity.getPassword());
		boolean rs = false;
		PreparedStatement ps = null;
		String pk=taskConfig.getPrimaryKeys().get(0);
		String minTimeStr =DateUtils.formatDatetime( delDataHandler.getNotProcessMinTime(), "yyyy-MM-dd HH:mm:ss");
		String maxTimeStr=DateUtils.formatDatetime( delDataHandler.getNotProcessMaxTime(), "yyyy-MM-dd HH:mm:ss");
		String sql = "delete from "+taskConfig.getTargetTable()+" t where exists (select 1 From "+delDataHandler.getDelDataTbName()+" p where t."+pk+"=p.id_column_value and p.table_name='"+delDataHandler.getTbName()+"' and p.yw_update_time >='"+minTimeStr+"' and p.yw_update_time <='"+maxTimeStr+"' and p.process_status=0)";
		ps = conn.prepareStatement(sql);
		logger.info("开始执行sql："+sql);
		try {
			rs = ps.execute();
		} catch (SQLException e) {
			logger.error(e);
			throw e;
		}
	}

	public static void updateProcessStatus(DelDataHandler delDataHandler) throws SQLException {
		TaskPropertiesConfig taskConfig = delDataHandler.getTaskConfig();
		TaskDatabaseConfig targetDbEntity = taskConfig.getTargetDbEntity();
		Connection conn = getConn(targetDbEntity.getDriverClassName(), targetDbEntity.getConnectionUrl(),
				targetDbEntity.getUserName(), targetDbEntity.getPassword());
		boolean rs = false;
		PreparedStatement ps = null;
		String minTimeStr =DateUtils.formatDatetime( delDataHandler.getNotProcessMinTime(), "yyyy-MM-dd HH:mm:ss");
		String maxTimeStr=DateUtils.formatDatetime( delDataHandler.getNotProcessMaxTime(), "yyyy-MM-dd HH:mm:ss");
		String sql = "update "+delDataHandler.getDelDataTbName()+" t set process_status=1 where t.table_name='"+delDataHandler.getTbName()+"'  and t.yw_update_time >='"+minTimeStr+"' and t.yw_update_time <='"+maxTimeStr+"' and t.process_status=0";
		ps = conn.prepareStatement(sql);
		logger.info("开始执行sql："+sql);
		try {
			rs = ps.execute();
		} catch (SQLException e) {
			logger.error(e);
			throw e;
		}
		
	}
}
