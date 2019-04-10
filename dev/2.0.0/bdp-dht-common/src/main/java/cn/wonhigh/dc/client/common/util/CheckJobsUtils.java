package cn.wonhigh.dc.client.common.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.yougou.logistics.base.common.exception.ManagerException;

import cn.wonhigh.dc.client.common.model.TaskDatabaseConfig;
import cn.wonhigh.dc.client.common.model.TaskPropertiesConfig;

public class CheckJobsUtils {
	/**
	 * @param args
	 */
	private static final Logger logger = Logger
			.getLogger(CheckJobsUtils.class);
	public   Connection dcconn = null;
	public   Connection pgconn = null;
	public  void updateSuccJobs() throws SQLException{
		init();
		
		//1  truncate  the target table
		Statement stmt=pgconn.createStatement();
		stmt.execute("truncate table client_task_final_status");
		stmt.close();
		//2  query the success jobs
		String dcsql="SELECT scheduler_name,group_name, max(sync_end_time) as sync_end_time, max(create_time) as create_time FROM client_task_status_log  t where    t.group_name like '%_hive' and t.task_status='FINISHED' and create_time>date_add(curdate(), interval 0 day)  group by scheduler_name,group_name   ";
		Statement dcstm=dcconn.createStatement();
	    Statement pgstm=pgconn.createStatement( );
		ResultSet rs=dcstm.executeQuery(dcsql);
		int id=1;
		String schedulername="";
		String groupName="";
		while(rs.next()){
			schedulername= rs.getString("scheduler_name");
			groupName=rs.getString("group_name");  
			TaskPropertiesConfig taskconfig = ParseXMLFileUtil.getTaskConfig(groupName, schedulername);
			String insertsql="INSERT INTO  client_task_final_status  VALUES ('"+id+"', '"+taskconfig.getTargetTable()+"',  1, '"+ rs.getString("sync_end_time")+"','"+rs.getString("create_time")+"', now())";
			logger.info(insertsql);
			System.out.println(insertsql);
			pgstm.execute(insertsql );
			id++;
		}
		pgstm.close();
		dcstm.close();
		rs.close();
		dcconn.close();
		pgconn.close();
	
		//3  insert the success jobs  into target table 
	}
	public   void init() {
		// ParseXMLFileUtil.initTask();
		Properties prop = PropertyFile.getProprties("dal-db-config.properties");
		try {
			dcconn = getConn(prop.getProperty("db.driverClass"),
					prop.getProperty("db.url"),
					prop.getProperty("db.username"),
					prop.getProperty("db.password"));
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			logger.error(e);;
		}
		int tid = Integer.valueOf(PropertyFile.getProps("").getProperty(
				"target.db.id"));
		pgconn = getConnById(tid, "postgresql");

		 
	}
	
	public static Connection getConn(String driver, String url, String user,
			String passwd) throws SQLException {
		try {
			Class.forName(driver);
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
		DriverManager.setLoginTimeout(30);
		return DriverManager.getConnection(url, user == null ? "root" : user,
				passwd == null ? "" : passwd);
	}
	public   Connection getConnById(int id, String dbtype) {
		    Connection conn=null;
			String driver = "";
			if (dbtype == "postgressql") {
				driver = "org.postgresql.Driver";
			} else {
				// the default driver is mysql
				driver = "com.mysql.jdbc.Driver";
			}
			TaskDatabaseConfig dbconf = ParseXMLFileUtil.getDbById(id);

			try {
				Class.forName(driver);
			} catch (ClassNotFoundException e) {
				throw new RuntimeException(e);
			}
			DriverManager.setLoginTimeout(30);
			try {
				conn = DriverManager.getConnection(dbconf.getConnectionUrl(),
						dbconf.getUserName(), dbconf.getPassword());
			} catch (SQLException e) {
				logger.error(e);;
			}
		return conn;
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
			ParseXMLFileUtil.initTask();
		} catch (ManagerException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
       try {
		new CheckJobsUtils().updateSuccJobs();
	} catch (SQLException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	}
}
