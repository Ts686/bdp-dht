package cn.wonhigh.dc.client.manager;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Resource;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jmx.export.annotation.ManagedResource;
import org.springframework.stereotype.Service;

import cn.wonhigh.dc.client.common.constans.CommonEnumCollection;
import cn.wonhigh.dc.client.common.constans.CommonEnumCollection.DbTypeCollecEnum;
import cn.wonhigh.dc.client.common.constans.CommonEnumCollection.ParamNameEnum;
import cn.wonhigh.dc.client.common.constans.MessageConstant;
import cn.wonhigh.dc.client.common.model.ClientTaskStatusLog;
import cn.wonhigh.dc.client.common.model.TabDupPrimaryConfig;
import cn.wonhigh.dc.client.common.model.TaskDatabaseConfig;
import cn.wonhigh.dc.client.common.model.TaskPropertiesConfig;
import cn.wonhigh.dc.client.common.util.ExceptionHandleUtils;
import cn.wonhigh.dc.client.common.util.ParseXMLFileUtil;
import cn.wonhigh.dc.client.manager.cache.RinseStatusAndLogCache;
import cn.wonhigh.dc.client.manager.jms.SendMsg2AMQ;
import cn.wonhigh.dc.client.service.ClientTaskStatusLogService;
import cn.wonhigh.dc.client.service.TabDupPrimaryConfigService;
import cn.wonhigh.dc.client.service.jms.JmsClusterMgr;
import cn.wonhigh.dc.scheduler.common.api.dto.TbColumnDiffDto;
import cn.wonhigh.dc.scheduler.common.api.service.RemoteRepoJobServiceExtWithParams;

import com.yougou.logistics.base.common.enums.JobBizStatusEnum;
import com.yougou.logistics.base.common.vo.scheduler.RemoteJobInvokeParamsDto;

/**
 * Hive重复主键值核查
 * 
 * @author zhang.rq
 * @since 2016-07-26
 */

@Service
@ManagedResource(objectName = TabDupPrimaryConfigTaskImpl.MBEAN_NAME, description = "Hive重复主键值核查")
public class TabDupPrimaryConfigTaskImpl implements
		RemoteRepoJobServiceExtWithParams<TbColumnDiffDto> {

	public static final String MBEAN_NAME = "dc:client=TabDupPrimaryConfigTaskImpl";

	private static final Logger logger = Logger
			.getLogger(TabDupPrimaryConfigTaskImpl.class);

	private String dateFormat = "yyyy-MM-dd HH:mm:ss";

	@Resource
	private ClientTaskStatusLogService clientTaskStatusLogService;

	@Resource
	private TabDupPrimaryConfigService tabDupPrimaryConfigService;

	@Autowired
	private JmsClusterMgr jmsClusterMgr;

	/**
	 * 
	 * @param hiveUrl
	 * @param hiveUser
	 * @param hivePwd
	 * @param searchMap
	 * @return
	 * @throws SQLException
	 */
	private List<TbColumnDiffDto> queryHiveTable(String taskName,
			String hiveUrl, String hiveUser, String hivePwd,
			Map<String, List<String>> searchMap) throws SQLException {

		Connection conSql = null;
		Statement state = null;
		ResultSet voRs = null;

		List<TbColumnDiffDto> dtoRet = new ArrayList<TbColumnDiffDto>();

		// 表相关信息
		String db_ods = CommonEnumCollection.HiveDefinePartNameEnum.DB_NAME_ODS
				.getValue();
		String ods = CommonEnumCollection.HiveDefinePartNameEnum.ODS_TABLE_NAME_SUBFIX
				.getValue();
		
		Integer tabDupPrimaryDate=MessageConstant.TAB_DUP_PRIMARY_CONFIG_VALUE;
		if(tabDupPrimaryDate==null||tabDupPrimaryDate<1) tabDupPrimaryDate=365;
		String partiName=CommonEnumCollection.HiveDefinePartNameEnum.PARTITION_DATE_NAME.getValue();
		
		Date beginDate = new Date();
		Calendar date = Calendar.getInstance();
		date.setTime(beginDate);
		date.set(Calendar.DATE, date.get(Calendar.DATE) - tabDupPrimaryDate);
		DateFormat format=new SimpleDateFormat("yyyyMMdd");
	    String time=format.format(date.getTime());
		
		String partitionQuery=" where "+partiName+" >= "+time;//配置时间
		
		for (Map.Entry<String, List<String>> entry : searchMap.entrySet()) {
			String sqlTemplate = "select concat(@query) from  @table  @where "
					+ " group by concat(@query) "
					+ " having count(concat(@query))>1";
			try {
				conSql = getConn(hiveUrl, hiveUser, hivePwd,
						DbTypeCollecEnum.HIVE.getValue());
				String[] tabAndGroup = entry.getKey().split("-");
				String groupName = tabAndGroup[0];
				String tbName = db_ods + groupName + "_" + tabAndGroup[1] + ods;
				String voTbName = groupName + "_" + tabAndGroup[1] + ods;
				String primaryKey = "";
				List<String> keys = entry.getValue();
				if (keys == null || keys.size() < 1)
					continue;

				String queryStr = "";
				for (String item : keys) {
					if (!queryStr.equals(""))
						queryStr += ",";
					queryStr = queryStr + "'[" + item + "='," + item + ",']'";
					primaryKey += item + ",";
				}
				sqlTemplate = sqlTemplate.replaceAll("@query", queryStr);
				sqlTemplate = sqlTemplate.replaceAll("@table", tbName);
				sqlTemplate = sqlTemplate.replaceAll("@where", partitionQuery);
				
//				logger.info("@where条件是:"+partitionQuery);

				state = conSql.createStatement();
				voRs = state.executeQuery(sqlTemplate);
				logger.info(String.format("查询hive库  【%s】", sqlTemplate));

				primaryKey = primaryKey.substring(0, primaryKey.length() - 1);

				// hive 条数读取
				while (voRs != null && voRs.next()) {
					String primaryValue = voRs.getString(1).trim();
					TbColumnDiffDto dto = new TbColumnDiffDto(null, primaryKey,
							primaryValue, groupName, taskName, voTbName, null);
					dtoRet.add(dto);
				}
			} catch (SQLException e) {
				throw new RuntimeException(
						"Hive重复主键值核查TabDupPrimaryConfigTaskImpl：queryHiveTable() "
								+ "数据查询失败！！     " + e);
			} finally {
				try {
					if (voRs != null)
						voRs.close();
					if (state != null)
						state.close();
					if (conSql != null)
						conSql.close();
				} catch (SQLException e) {
					throw new RuntimeException(
							"Hive重复主键值核查TabDupPrimaryConfigTaskImpl：queryHiveTable()"
									+ "连接关闭失败！！！     " + e);
				}
			}
		}
		return dtoRet;

	}

	/**
	 * 获取数据库连接
	 * 
	 * @param connectionUrl
	 * @param userName
	 * @param passWord
	 * @param dbType
	 * @return
	 */
	public Connection getConn(String connectionUrl, String userName,
			String passWord, Integer dbType) throws RuntimeException {

		String driver = "";
		if (dbType == DbTypeCollecEnum.ORACLE.getValue()) {
			driver = "oracle.jdbc.driver.OracleDriver";
		} else if (dbType == DbTypeCollecEnum.HIVE.getValue()) {
			driver = DbTypeCollecEnum.HIVE.getDriverName();
		} else {
			driver = "com.mysql.jdbc.Driver";
		}

		Connection conn = null;
		try {
			Class.forName(driver);
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
		DriverManager.setLoginTimeout(30);
		try {
			System.out.println("ConnectionUrl	: " + connectionUrl);
			System.out.println("userName:	" + userName);
			// System.out.println("passWord: " + passWord);

			conn = DriverManager.getConnection(connectionUrl, userName,
					passWord);
		} catch (SQLException e) {
			throw new RuntimeException("数据源连接失败！！！ " + connectionUrl + "   "
					+ e);
		}
		return conn;
	}

	@Override
	public List<TbColumnDiffDto> executeCkJobsWithParams(String jobId,
			String triggerName, String groupName,
			RemoteJobInvokeParamsDto remoteJobInvokeParamsDto) {
		JobBizStatusEnum jobBizStatusEnum = null;
		List<TbColumnDiffDto> retList = new ArrayList<TbColumnDiffDto>();

		// 如果重复调用，则忽略本次请求
		JobBizStatusEnum jobStauts = RinseStatusAndLogCache
				.getTaskStatusByJobId(jobId);
		if (jobStauts != null
				&& !jobStauts.name()
						.equals(JobBizStatusEnum.INTERRUPTED.name())) {
			logger.info(String.format("【jobId为：%s】的任务被重复调用", jobId));
			return retList;
		}

		// 记录开始时间和结束时间
		Date syncBeginTime = null;
		Date syncEndTime = null;
		String startTimeStr = null;
		String endTimeStr = null;
		if (remoteJobInvokeParamsDto != null) {
			startTimeStr = remoteJobInvokeParamsDto.getParam("startTime");
			endTimeStr = remoteJobInvokeParamsDto.getParam("endTime");
			SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
			if (startTimeStr != null && endTimeStr != null) {
				logger.info(String.format("【jobId为：%s】的任务被调用,", jobId)
						+ "开始时间:" + startTimeStr + ";" + "结束时间:" + endTimeStr
						+ ".");
				try {
					syncBeginTime = sdf.parse(startTimeStr);
					syncEndTime = sdf.parse(endTimeStr);
				} catch (ParseException e) {
					logger.error(e);
					SendMsg2AMQ.updateStatusAndSend(jobId,
							JobBizStatusEnum.INTERRUPTED, jmsClusterMgr);
					RinseStatusAndLogCache.removeTaskByJobId(jobId);
					return retList;
				}
			}
		}

		try {
			logger.info("--------开始进入调度方法--初始化------------");
			jobBizStatusEnum = JobBizStatusEnum.INITIALIZING;
			String messageLocalZe = "初始化中";
			addTaskLog(jobId, triggerName, groupName, jobBizStatusEnum,
					messageLocalZe, syncBeginTime, syncEndTime);
			SendMsg2AMQ.updateStatusAndSend(jobId, jobBizStatusEnum,
					jmsClusterMgr);

			// 读取_clnd.xml名字
			String clnd = CommonEnumCollection.HiveDefinePartNameEnum.CLEANED_TABLE_NAME_SUBFIX
					.getValue();

			logger.info("--------获取任务配置信息------------");
			// 从mysql中获取需要统计配置的表信息
			List<TabDupPrimaryConfig> tabs = tabDupPrimaryConfigService
					.getTabDupPrimaryConfigs();

			if (tabs == null || tabs.size() < 1) {
				logger.error(String.format("没有配置查询重复组件表", groupName,
						triggerName));
				SendMsg2AMQ.updateStatusAndSend(jobId,
						JobBizStatusEnum.INTERRUPTED, jmsClusterMgr);
				RinseStatusAndLogCache.removeTaskByJobId(jobId);
				return retList;// 查询不到数据
			}

			// 存储查询关联表和关联的主键，这里只查询ods表
			Map<String, List<String>> searchMap = new HashMap<String, List<String>>();
			logger.info("xml名称是："+tabs.get(0).getGroupName()+"    "+	tabs.get(0).getTableName() + clnd);
			TaskPropertiesConfig config= ParseXMLFileUtil.getTaskConfig(
					tabs.get(0).getGroupName(),
					tabs.get(0).getTableName() + clnd);
			if (config == null) {
				logger.error(String.format("获取缓存中的xml失败：组名【%s】调度名【%s】",
						groupName, triggerName));
				SendMsg2AMQ.updateStatusAndSend(jobId,
						JobBizStatusEnum.INTERRUPTED, jmsClusterMgr);
				RinseStatusAndLogCache.removeTaskByJobId(jobId);
				return retList;
			}
			
			TaskDatabaseConfig db =config.getSourceDbEntity();
			String hiveUrl = db.getConnectionUrl();
			String hiveUser = db.getUserName();
			String hivePwd = db.getPassword();

			// 拼接查询SQL
			for (TabDupPrimaryConfig tabItem : tabs) {
				groupName = tabItem.getGroupName();
				String tableName = tabItem.getTableName();
				// 1、获取任务配置信息
				TaskPropertiesConfig taskConfig = ParseXMLFileUtil
						.getTaskConfig(groupName, tableName + clnd);
				if (taskConfig == null) {
					logger.error(String.format("获取缓存中的xml失败：组名【%s】调度名【%s】",
							groupName, triggerName));
					SendMsg2AMQ.updateStatusAndSend(jobId,
							JobBizStatusEnum.INTERRUPTED, jmsClusterMgr);
					RinseStatusAndLogCache.removeTaskByJobId(jobId);
					return retList;
				}

				logger.info(String.format(
						"获取任务配置信息成功！开始时间【%s】 结束时间：【%s】 组名【%s】 调度名【%s】",
						syncBeginTime, syncEndTime, groupName, triggerName));

				searchMap.put(groupName + "-" + tableName,
						taskConfig.getPrimaryKeys());
			}

			// 2、更新实例状态--初始化
			jobBizStatusEnum = JobBizStatusEnum.RUNNING;
			String message = "运行中";
			addTaskLog(jobId, triggerName, groupName, jobBizStatusEnum,
					message, syncBeginTime, syncEndTime);
			SendMsg2AMQ.updateStatusAndSend(jobId, jobBizStatusEnum,
					jmsClusterMgr);

			logger.info(String.format("【2】 开始查询hive 表数据！组名【%s】调度名【%s】",
					groupName, triggerName));
			retList = queryHiveTable(triggerName, hiveUrl, hiveUser, hivePwd,
					searchMap);
			for (TbColumnDiffDto dto : retList) {
				logger.info("客户端返回给服务器端的值：" + dto.toString());
			}

		} catch (Throwable e) {
			jobBizStatusEnum = JobBizStatusEnum.INTERRUPTED;
			SendMsg2AMQ.updateStatusAndSend(jobId, jobBizStatusEnum,
					jmsClusterMgr);
			String errMsg = ExceptionHandleUtils.getExceptionMsg(e);
			String message = String.format(
					"【groupName：%s】--【triggerName：%s】任务时失败", groupName,
					triggerName)
					+ errMsg;
			logger.error(message);
			addTaskLog(jobId, triggerName, groupName, jobBizStatusEnum,
					message.substring(0, Math.min(message.length(), 800)),
					syncBeginTime, syncEndTime);
			SendMsg2AMQ.updateStatusAndSend(jobId, jobBizStatusEnum,
					jmsClusterMgr);
			RinseStatusAndLogCache.removeTaskByJobId(jobId);
			return retList;
		}

		jobBizStatusEnum = JobBizStatusEnum.FINISHED;
		String messageLocalTh = "执行Hive重复主键值核查查询成功:"
				+ String.format("【groupName=%s】【schedulerName=%s】执行成功！",
						groupName, triggerName);
		addTaskLog(jobId, triggerName, groupName, jobBizStatusEnum,
				messageLocalTh, syncBeginTime, syncEndTime);
		// 成功后，需要传mq并删除缓存
		SendMsg2AMQ.updateStatusAndSend(jobId, jobBizStatusEnum, jmsClusterMgr);
		RinseStatusAndLogCache.removeTaskByJobId(jobId);
		logger.info(String.format("完成调度任务：%s ===>结束时间：%s！组名【%s】调度名【%s】",
				syncBeginTime, syncEndTime, groupName, triggerName));
		logger.info(String.format("【5】 将数据校验结果返回调度！组名【%s】调度名【%s】", groupName,
				triggerName));
		return retList;
	}

	/**
	 * 任务状态日志入库
	 * 
	 * @param taskId
	 * @param triggerName
	 * @param groupName
	 * @param taskDesc
	 */
	private void addTaskLog(String taskId, String triggerName,
			String groupName, JobBizStatusEnum jobBizStatus, String taskDesc,
			Date... endTime) {
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

	@Override
	public TbColumnDiffDto executeCkJobWithParams(String arg0, String arg1,
			String arg2, RemoteJobInvokeParamsDto arg3) {
		return null;
	}

	@Override
	public void executeJobWithParams(String arg0, String arg1, String arg2,
			RemoteJobInvokeParamsDto arg3) {

	}

	@Override
	public JobBizStatusEnum getJobStatus(String jobId, String triggerName,
			String groupName) {
		Map<String, Object> params = new HashMap<String, Object>();
		params.put(ParamNameEnum.TASK_ID.getValue(), jobId);
		params.put(ParamNameEnum.TASK_NAME.getValue(), triggerName);
		params.put(ParamNameEnum.GROUP_NAME.getValue(), groupName);
		ClientTaskStatusLog clientTaskStatusLog = clientTaskStatusLogService
				.findLastestStatus(params);
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
	public String getLogs(String arg0, String arg1, String arg2, long arg3) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void initializeJob(String arg0, String arg1, String arg2) {
		// TODO Auto-generated method stub

	}

	@Override
	public void pauseJob(String arg0, String arg1, String arg2) {
		// TODO Auto-generated method stub

	}

	@Override
	public void restartJob(String arg0, String arg1, String arg2) {
		// TODO Auto-generated method stub

	}

	@Override
	public void resumeJob(String arg0, String arg1, String arg2) {
		// TODO Auto-generated method stub

	}

	@Override
	public void stopJob(String arg0, String arg1, String arg2) {
		// TODO Auto-generated method stub

	}

	public void main(String[] args) {
		executeCkJobsWithParams("asdfegdfsa", "pro_product_ck", "ck_test", null);
	}
}
