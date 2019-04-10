package cn.wonhigh.dc.client.common.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ConcurrentMap;

import org.apache.log4j.Logger;

import cn.wonhigh.dc.client.common.model.TaskDatabaseConfig;
import cn.wonhigh.dc.client.common.model.TaskPropertiesConfig;

import com.yougou.logistics.base.common.exception.ManagerException;

public class DataValidationUtil {
	private static final Logger logger = Logger
			.getLogger(DataValidationUtil.class);
	private Date syncEndDate;
	private Date syncBeginDate;
	public DataValidationUtil(Date syncBeg,Date syncEnd){  
	    this.syncBeginDate=syncBeg; 
		this.syncEndDate=syncEnd;
	   }  
	public Map<Integer, Connection> cacheConn = new HashMap<Integer, Connection>();
	public Connection dcconn = null;
	public Connection pgconn = null;
	private static final String dateFormat = "yyyy-MM-dd HH:mm:ss";
	public static final String SPLITONE = ":";
	public static final String SPLITTWO = ",";

	public static void main(String args[]) throws ManagerException,
			SQLException {

	}

	public String getDVReport() {
		String report = "";
		init();
		// begin
		ConcurrentMap<String, TaskPropertiesConfig> taskmap = ParseXMLFileUtil
				.getTaskList();
		// Set taskset=taskmap.entrySet();
		for (Entry<String, TaskPropertiesConfig> task : taskmap.entrySet()) {
			// System.out.println(task.getKey());
			String strarr[] = task.getKey().split(
					ParseXMLFileUtil.SPLITE_CHAR_4);
			if ((strarr[0].indexOf("hive") == -1)
					&& (strarr[1].indexOf("_cln") == -1)) {
				TaskPropertiesConfig taskconfig = task.getValue();
				String groupName = strarr[0];
				String triggerName = strarr[1];
				String sourcetable = taskconfig.getSourceTable();
				String targettable = taskconfig.getTargetTable();
				//List<String>
				// 主从关系判断
				String pgsql = "";
				String mysql = "";
				SimpleDateFormat sdf=new SimpleDateFormat(dateFormat);
				String syncBeginTimeStr = sdf.format(syncBeginDate);
				String syncEndTimeStr = sdf.format(DateUtils.nextDateBegin(syncEndDate));
				if (taskconfig.getIsOverwrite() == 1) {
					mysql = "select count(*) as count from  " + sourcetable
							+ " t where 1=1 ";
					pgsql = "select count(*) as count from  " + targettable;
				} else if (taskconfig.getIsSlaveTable() == 0) {
					/*
					 * sybegintime=getSycBeginTime(triggerName); sytime =
					 * getSycTime(triggerName); if (syclnlist != null) { String
					 * sql="("; for (String sycln : syclnlist) { if (sycln !=
					 * null) { sql += "("+sycln +" > '"+sybegintime
					 * +"' and "+sycln + "< '" + sytime + "' ) or"; } }
					 * 
					 * sql=sql.substring(0, sql.length()-2)+")";
					 */
					StringBuffer whereField = new StringBuffer(" where 1=1 ");
//					String syncBeginTimeStr = getSycBeginTime(triggerName);
//					String syncEndTimeStr = getSycTime(triggerName);
					List<String> syncTimeColumn = taskconfig
							.getSyncTimeColumn();
						StringBuffer whereFieldBuffer = new StringBuffer(
								" and ( ");
						//构造同步时间字段
						String whereFiledTime = "";
						List<String> selectColumns=taskconfig.getSelectColumns();
						for (int i = 0; i < selectColumns.size(); i++) {
							String ywUpdateTimeFiled = selectColumns.get(i);
							if (ywUpdateTimeFiled.contains(" yw_update_time")) {
								ywUpdateTimeFiled = selectColumns.get(i);
								String ywUpdateTimeFiledNew = ywUpdateTimeFiled.replace(SPLITONE, SPLITTWO);
								//selectColumns.set(i, ywUpdateTimeFiledNew);
								whereFiledTime = ywUpdateTimeFiledNew.substring(0, ywUpdateTimeFiled.indexOf("as yw_update_time"))
										.trim();
							}
						}
						/*
						 * for (int j = 0; j < syncTimeColumn.size(); j++) { if
						 * (j == syncTimeColumn.size() - 1) {
						 * whereFieldBuffer.append("(t." + syncTimeColumn.get(j)
						 * + ">=" + "" + "'" + syncBeginTimeStr + "'" + "");
						 * whereFieldBuffer.append(" and ");
						 * whereFieldBuffer.append("t." + syncTimeColumn.get(j)
						 * + "<" + "" + "'" + syncEndTimeStr + "'" + "");
						 * whereFieldBuffer.append(" ) "); } else {
						 * whereFieldBuffer.append("(t." + syncTimeColumn.get(j)
						 * + ">=" + "" + "'" + syncBeginTimeStr + "'" + "");
						 * whereFieldBuffer.append(" and ");
						 * whereFieldBuffer.append("t." + syncTimeColumn.get(j)
						 * + "<" + "" + "'" + syncEndTimeStr + "'" + "");
						 * whereFieldBuffer.append(" ) ");
						 * whereFieldBuffer.append(" or "); } }
						 */
						if (syncTimeColumn.size() >=1) {
							whereFieldBuffer.append(whereFiledTime).append(" >= ").append("'")
							.append(syncBeginTimeStr).append("'").append(" and ").append(whereFiledTime)
							.append(" < ").append("'").append(syncEndTimeStr).append("'");
						}  else {
							throw new IllegalArgumentException(
									"时间戳字段个数存在问题.......");
						}
						whereFieldBuffer.append(")");
						whereField.append(whereFieldBuffer.toString());

						mysql = "select count(*) as count from  " + sourcetable
								+ " t    " + whereField.toString();
						pgsql = "select count(*) as count from  " + targettable
								+ " t  " + whereField.toString();

				} else {

					// String whereField = "where (  ";
					TaskPropertiesConfig taskConfigParent = ParseXMLFileUtil
							.getTaskConfig(taskconfig.getDependencyTaskIds()
									.get(0));
					List<String> syncTimeColumn = taskConfigParent
							.getSyncTimeColumn();
					List<String> selectColumns = taskconfig.getSelectColumns();
					//处理查询字段中含dc业务时间的
					String whereFiledTime = "";
					for (int i = 0; i < selectColumns.size(); i++) {
						String ywUpdateTimeFiled = selectColumns.get(i);
						if (ywUpdateTimeFiled.contains(" yw_update_time")) {
							String ywUpdateTimeFiledNew = ywUpdateTimeFiled;
								String[] ywUpdateTimeFiledList = ywUpdateTimeFiled.replace(SPLITONE, SPLITTWO).split(",");
								if(!ywUpdateTimeFiled.contains("t.")){
								if (!ywUpdateTimeFiled.contains("(")) {
									ywUpdateTimeFiledNew = "t." + ywUpdateTimeFiled.replace(SPLITONE, SPLITTWO).trim();
								} else {
									String[] ywUpdateTimeFiledFirst = ywUpdateTimeFiledList[0].split("\\(");
									ywUpdateTimeFiledNew = ywUpdateTimeFiledFirst[0] + "(t." + ywUpdateTimeFiledFirst[1].trim();
									for (int j = 1; j < ywUpdateTimeFiledList.length; j++) {
										ywUpdateTimeFiledNew += ",t." + ywUpdateTimeFiledList[j].trim();

									}
								}
								}
							whereFiledTime = ywUpdateTimeFiledNew.substring(0, ywUpdateTimeFiledNew.indexOf("as yw_update_time"));
						}
					}
					// 构造where条件子句
					/*
					 * sytime = getSycTime(taskConfigParent.gettriggerName());
					 * sybegintime
					 * =getSycBeginTime(taskConfigParent.gettriggerName());
					 * syclnlist = taskConfigParent.getSyncTimeColumn(); if
					 * (syclnlist != null) { whereField += "("; for (String
					 * sycln : syclnlist) { if (sycln != null) { whereField +=
					 * "  ( t." + sycln + "> '" + sybegintime + "' and    t." +
					 * sycln + "< '" + sytime + "')   or"; } }
					 * whereField=whereField.substring(0,
					 * whereField.length()-2)+")"; } whereField += ")";
					 */
					// 构造where条件子句
					StringBuffer whereField = new StringBuffer("where 1=1 ");
					
						StringBuffer whereFieldBuffer = new StringBuffer(
								" and ( ");
						/*
						 * for (int j = 0; j < syncTimeColumn.size(); j++) { if
						 * (j == syncTimeColumn.size() - 1) {
						 * whereFieldBuffer.append("(t." + syncTimeColumn.get(j)
						 * + ">=" + "" + "'" + syncBeginTimeStr + "'" + "");
						 * whereFieldBuffer.append(" and ");
						 * whereFieldBuffer.append("t." + syncTimeColumn.get(j)
						 * + "<" + "" + "'" + syncEndTimeStr + "'" + "");
						 * whereFieldBuffer.append(" ) "); } else {
						 * whereFieldBuffer.append("(t." + syncTimeColumn.get(j)
						 * + ">=" + "" + "'" + syncBeginTimeStr + "'" + "");
						 * whereFieldBuffer.append(" and ");
						 * whereFieldBuffer.append("t." + syncTimeColumn.get(j)
						 * + "<" + "" + "'" + syncEndTimeStr + "'" + "");
						 * whereFieldBuffer.append(" ) ");
						 * whereFieldBuffer.append(" or "); } }
						 */
						if (syncTimeColumn.size() >=1) {
							whereFieldBuffer.append(whereFiledTime.trim()).append(" >= ")
							.append("'").append(syncBeginTimeStr).append("'").append(" and ").append(whereFiledTime.trim())
							.append(" < ").append("'").append(syncEndTimeStr).append("'");
						} else {
							throw new IllegalArgumentException(
									"时间戳字段个数存在问题.......");
						}
						whereFieldBuffer.append(")");
					    whereField.append(whereFieldBuffer.toString());
						// join连接字段
						String joinField = "";
						List<String> relationColumnParent = taskConfigParent
								.getRelationColumns();
						List<String> relationColumnSlave = taskconfig
								.getRelationColumns();
						if ((relationColumnParent == null || relationColumnParent
								.size() <= 0)
								&& (relationColumnSlave == null || relationColumnSlave
										.size() <= 0)) {
							relationColumnParent.add("bill_no");
							relationColumnSlave.add("bill_no");
						} else if ((relationColumnParent != null && relationColumnParent
								.size() > 0)
								&& (relationColumnSlave == null || relationColumnSlave
										.size() <= 0)) {
							relationColumnSlave = relationColumnParent;
						} else if ((relationColumnSlave != null && relationColumnSlave
								.size() > 0)
								&& (relationColumnParent == null || relationColumnParent
										.size() <= 0)) {
							relationColumnParent = relationColumnSlave;
						}
						StringBuffer joinFieldBuffer = new StringBuffer(
								" on ( ");
						for (int j = 0; j < Math.min(
								relationColumnSlave.size(),
								relationColumnParent.size()); j++) {
							if (j == Math.min(relationColumnSlave.size(),
									relationColumnParent.size()) - 1) {
								joinFieldBuffer.append("t.");
								joinFieldBuffer.append(relationColumnParent
										.get(j));
								joinFieldBuffer.append("=");
								joinFieldBuffer.append(" p.");
								joinFieldBuffer.append(relationColumnSlave
										.get(j));

							} else {
								joinFieldBuffer.append("t.");
								joinFieldBuffer.append(relationColumnParent
										.get(j));
								joinFieldBuffer.append("=");
								joinFieldBuffer.append(" p.");
								joinFieldBuffer.append(relationColumnSlave
										.get(j));
								joinFieldBuffer.append(" and ");
							}
						}
						joinFieldBuffer.append(") ");
						joinField = joinFieldBuffer.toString();
						
				
						// 拼接查询sql字段
						mysql = "select count(*) as count  from "
								+ taskconfig.getSourceTable()
								+ " p inner join "
								+ taskConfigParent.getSourceTable() + " t "
								+ joinField + whereField;
						pgsql = "select count(*) as count  from "
								+ taskconfig.getTargetTable()
								+ " p inner join "
								+ taskConfigParent.getTargetTable() + " t "
								+ joinField + whereField;

					
				}

				// TODO not sure is there
				String filterConditions = " ";
				if (taskconfig.getFilterConditions() != null) {
					StringBuffer filters = new StringBuffer();
					for (String filter : taskconfig.getFilterConditions()) {
						filters.append(" ");
						filters.append(filter);
						filters.append(" ");
					}
					filterConditions = filters.toString();
				}
				mysql += filterConditions;
				// System.out.println(sql);
				pgsql = pgsql.replaceAll("ifnull", "COALESCE");
				logger.info("查询sql：" + mysql);
				logger.info("查询sql：" + pgsql);
				// System.out.println("==========================");

				  System.out.println(mysql);
				  System.out.println(pgsql);

				Connection conn = getConnById(taskconfig.getSourceDbId(),
						"mysql");
				if (conn == null) {
					continue;
				}
				Statement stm = null;
				try {
					stm = conn.createStatement();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					logger.error(e);
					;
				}
				ResultSet rs = null;
				try {
					rs = stm.executeQuery(mysql);
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					logger.error(e);
					;
				}
				int tcount = 0;
				tcount = getTDBCount(pgsql);
				int scount = 0;
				try {
					if (rs.next()) {
						scount = rs.getInt("count");
					}
				} catch (Exception e) {
					// TODO Auto-generated catch block
					logger.error(e);
					;
				}
				if (rs != null) {
					try {
						rs.close();
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						logger.error(e);
						;
					}
				}
				if (stm != null) {
					try {
						stm.close();
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						logger.error(e);
						;
					}
				}
				//  System.out.println(targettable+":原表 -"+scount+"-目标表-"+tcount);
				if (scount != tcount) {
					// report += "调度组名： " + groupName + "--调度任务名：" + triggerName
					// + " --业务表数据增量数据 ：" + scount + "--目标表数据增量数据：" + tcount
					// + "\n";
					// report += "执行sql-业务库：" + mysql + "\n";
					// report += "执行sql-目标库：" + pgsql + "\n";

					report += "  <tr> <td colspan='4' height='18'> </td> </tr> <tr> <td colspan='4' height='18' style='text-align:center;'> <strong>数据比对情况分析</strong> "
							+ " </td> </tr> <tr> <td height='18' style='text-align:center;'> 调度组名 </td> <td style='text-align:center;'>  "
							+ groupName
							+ " </td> <td style='text-align:center;'> 调度任务名</td> <td style='text-align:center;'> "
							+ triggerName
							+ "  </td> </tr> <tr> <td height='18' style='text-align:center;'> 业务表数据增量数据  </td>"
							+ " <td style='text-align:center;'> "
							+ scount
							+ " </td> <td style='text-align:center;'> 目标表数据增量数据</td> <td style='text-align:center;'> "
							+ tcount
							+ " </td> </tr> <tr> <td height='18' style='text-align:center;'> 执行sql-业务库 </td> <td colspan='3' > "
							+ mysql
							+ " </td> </tr> <tr> <td height='18' style='text-align:center;'>"
							+ " 执行sql-目标库 </td> <td colspan='3' > "
							+ pgsql
							+ " </td> </tr>";

				}
			}
		}
		// end
		if (dcconn != null) {
			try {
				dcconn.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				logger.error(e);
				;
			}
		}
		closeConn();
		return report;

	}

	public String getSycTime(String scheduler_name) {
		String sql = "SELECT sync_end_time FROM client_task_status_log where scheduler_name ='"
				+ scheduler_name
				+ "' and group_name like '%_hive' and task_status='FINISHED' order by sync_end_time desc limit 1";
		Statement stm = null;
		try {
			stm = dcconn.createStatement();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			logger.error(e);
			;
		}
		ResultSet rs = null;
		try {
			rs = stm.executeQuery(sql);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			logger.error(e);
			;
		}
		String syctime = "";
		try {
			if (rs.next()) {
				syctime = rs.getString("sync_end_time");
				syctime = syctime.substring(0, syctime.length() - 2);
			} else {
				syctime = "2099-01-01 00:00:00";
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			logger.error(e);
			;
		}
		if (stm != null) {
			try {
				stm.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				logger.error(e);
				;
			}
		}
		if (rs != null) {
			try {
				rs.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				logger.error(e);
				;
			}
		}
		return syctime;
	}

	public String getSycBeginTime(String scheduler_name) {
		String sql = "SELECT sync_end_time FROM client_task_status_log where scheduler_name ='"
				+ scheduler_name
				+ "' and group_name like '%_hive' and task_status='FINISHED' order by sync_end_time desc limit 1,1";
		Statement stm = null;
		try {
			stm = dcconn.createStatement();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			logger.error(e);
			;
		}
		ResultSet rs = null;
		try {
			rs = stm.executeQuery(sql);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			logger.error(e);
			;
		}
		String syctime = "";
		try {
			if (rs.next()) {
				syctime = rs.getString("sync_end_time");
				syctime = syctime.substring(0, syctime.length() - 2);
			} else {
				syctime = "1970-01-01 08:00:00";
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			logger.error(e);
			;
		}
		if (stm != null) {
			try {
				stm.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				logger.error(e);
				;
			}
		}
		if (rs != null) {
			try {
				rs.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				logger.error(e);
				;
			}
		}
		return syctime;
	}

	public String getSucReport() throws SQLException {
		Properties prop = PropertyFile.getProprties("dal-db-config.properties");
		Connection conn = null;
		try {
			conn = getConn(prop.getProperty("db.driverClass"),
					prop.getProperty("db.url"),
					prop.getProperty("db.username"),
					prop.getProperty("db.password"));
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			logger.error(e);
			;
		}
		String sucSql = "SELECT count(DISTINCT (CASE WHEN ctsl.group_name NOT LIKE '%hive' AND ctsl.task_status = 'FINISHED' AND ctsl.group_name NOT LIKE 'dc_common' AND ctsl.scheduler_name NOT LIKE '%cln' THEN concat(ctsl.scheduler_name,ctsl.group_name) END)) import_success, count(DISTINCT (CASE WHEN ctsl.group_name NOT LIKE '%hive' AND ctsl.task_status = 'FINISHED' AND ctsl.scheduler_name LIKE '%cln' THEN concat(ctsl.scheduler_name,ctsl.group_name) END)) remove_success,  count(DISTINCT (CASE WHEN ctsl.group_name LIKE '%hive' AND ctsl.task_status = 'FINISHED' AND ctsl.scheduler_name NOT LIKE '%cln' THEN concat(ctsl.scheduler_name,ctsl.group_name) END)) export_success, (select count(distinct concat(b.group_name,b.scheduler_name)) from (select a.group_name,a.scheduler_name,max(a.create_time) as create_time from client_task_status_log a where a.create_time >= date_format(now(), '%Y-%m-%d 00:00:00') group by a.group_name,a.scheduler_name) b,client_task_status_log c where c.create_time >= date_format(now(), '%Y-%m-%d 00:00:00') and b.group_name = c.group_name and b.scheduler_name = c.scheduler_name and b.create_time = c.create_time and c.task_status = 'INTERRUPTED' ) exception_count FROM  client_task_status_log ctsl WHERE  ctsl.create_time >= date_format(now(), '%Y-%m-%d 00:00:00') and ctsl.task_status = 'FINISHED'";
		Statement stm = null;
		try {
			stm = conn.createStatement();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			logger.error(e);
			;
			throw e;
		}
		ResultSet sucRs = null;
		try {
			sucRs = stm.executeQuery(sucSql);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			logger.error(e);
			;
		}
		
		
		String import_success = "";
		String remove_success = "";
		String export_success = "";
		String exception_count = "";
		String exceptionReport="  <tr> <td colspan='4' height='18'> </td> </tr> <tr> <td colspan='4' height='18' style='text-align:center;'> <strong>异常任务信息</strong> ";
		try {
			if (sucRs.next()) {
				import_success = sucRs.getString("import_success");
				remove_success = sucRs.getString("remove_success");
				export_success = sucRs.getString("export_success");
				exception_count = sucRs.getString("exception_count");
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			logger.error(e);
			;
		}
		
		String exceptSql="select distinct CONCAT(b.group_name,'@',b.scheduler_name) exceptions from (select a.group_name,a.scheduler_name,max(a.create_time) as create_time from client_task_status_log a where a.create_time >= date_format(now(), '%Y-%m-%d 00:00:00') group by a.group_name,a.scheduler_name) b,client_task_status_log c where c.create_time >= date_format(now(), '%Y-%m-%d 00:00:00') and b.group_name = c.group_name and b.scheduler_name = c.scheduler_name and b.create_time = c.create_time and c.task_status = 'INTERRUPTED'";
		ResultSet exceptRs = null;
		try {
			exceptRs = stm.executeQuery(exceptSql);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			logger.error(e);
		}
		try {
			while(exceptRs.next()) {
				String except = exceptRs.getString("exceptions");
				if(except!=null && except.length()>0){
					String[] groupScheduName=except.split("@");
				    String groupName=groupScheduName[0];
				    String triggerName=groupScheduName[1];
				    exceptionReport += " </td> </tr> <tr> <td height='18' width='20%' style='text-align:center;'> 异常调度组名</td> <td style='text-align:center;'>  "
						+ groupName
						+ " </td> <td width='20%' style='text-align:center;'> 异常调度任务名 </td> <td style='text-align:center;'> "
						+ triggerName
						+ "  </td> </tr>";
				}
			}
			logger.info(exceptionReport);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			logger.error(e);
		}
		
		if (sucRs != null) {
			try {
				sucRs.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				logger.error(e);
				;
			}
		}
		if (exceptRs != null) {
			try {
				sucRs.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				logger.error(e);
				;
			}
		}
		if (conn != null) {
			try {
				conn.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				logger.error(e);
				;
			}
		}

		String reportemail = "<tr>      <td colspan='4' height='18' style='text-align:center;'>       <strong>数据同步总体执行情况</strong>       </td>     </tr>     <tr>      <td height='18' width='20%' style='text-align:center;'>       导入成功数       </td>      <td colspan='3' style='text-align:center;'>       "
				+ import_success
				+ "      </td>     </tr>     <tr>      <td height='18' style='text-align:center;'>       去重成功数       </td>      <td colspan='3' style='text-align:center;'>       "
				+ remove_success
				+ "      </td>     </tr>     <tr>      <td height='18' style='text-align:center;'>       导出成功数       </td>      <td colspan='3' style='text-align:center;'>       "
				+ export_success
				+ "      </td>     </tr>     <tr>      <td height='18' style='text-align:center;'>       异常数       </td>      <td colspan='3' style='text-align:center;'>      "
				+ exception_count + "      </td>     </tr>    ";

		return reportemail+exceptionReport;
	}

	public int getTDBCount(String pgsql) {
		Statement stm = null;
		try {
			if(pgconn==null){
				int tid = Integer.valueOf(PropertyFile.getProps("").getProperty(
						"target.db.id"));
				pgconn = getConnById(tid, "postgresql");
			}
			stm = pgconn.createStatement();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			logger.error(e);
			;
		}
		ResultSet rs = null;
		try {
			rs = stm.executeQuery(pgsql);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			logger.error(e);
			;
		}
		int count = 0;
		try {
			if (rs.next()) {
				count = rs.getInt("count");
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			logger.error(e);
			;
		} catch (NullPointerException e) {
			// TODO Auto-generated catch block
			logger.error(e);
			;
		} catch (Exception e) {
			logger.error(e);
			;
		}
		if (rs != null) {
			try {
				rs.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				logger.error(e);
				;
			}
		}
		if (stm != null) {
			try {
				stm.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				logger.error(e);
				;
			}
		}
		return count;

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

	public Connection getConnById(int id, String dbtype) {
		Connection conn = cacheConn.get(id);
		if (conn == null) {
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
				logger.error(e);
				;
			}
			cacheConn.put(id, conn);
		}
		return conn;
	}

	// close all of the connection
	public void closeConn() {
		for (Map.Entry<Integer, Connection> conne : cacheConn.entrySet()) {
			try {
				if (conne.getValue() != null) {
					conne.getValue().close();
				}
			} catch (SQLException e) {
				logger.error(e);
				;
			}
		}
	}

	public void init() {
		// ParseXMLFileUtil.initTask();
		Properties prop = PropertyFile.getProprties("dal-db-config.properties");
		try {
			dcconn = getConn(prop.getProperty("db.driverClass"),
					prop.getProperty("db.url"),
					prop.getProperty("db.username"),
					prop.getProperty("db.password"));
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			logger.error(e);
			;
		}
		int tid = Integer.valueOf(PropertyFile.getProps("").getProperty(
				"target.db.id"));
		pgconn = getConnById(tid, "postgresql");

		// ConcurrentMap<String, TaskPropertiesConfig>
		// taskmap=ParseXMLFileUtil.getTaskList();
		// //Set taskset=taskmap.entrySet();
		// for(Entry<String, TaskPropertiesConfig> task:taskmap.entrySet()){
		// // System.out.println(task.getKey());
		// String strarr[]=task.getKey().split(ParseXMLFileUtil.SPLITE_CHAR_4);
		// if(strarr[0].indexOf("hive")==-1){
		// String groupName=strarr[0];
		// String triggerName=strarr[1];
		// String
		// tmpsql="SELECT sync_end_time FROM client_task_status_log where scheduler_name ='"+triggerName+"' and group_name like '%_hive' and task_status='FINISHED' order by sync_end_time desc limit 1";
		//
		// }
		// }
	}
}
