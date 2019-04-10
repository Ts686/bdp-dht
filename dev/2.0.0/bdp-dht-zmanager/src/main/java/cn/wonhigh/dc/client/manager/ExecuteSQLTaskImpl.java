package cn.wonhigh.dc.client.manager;

import java.io.File;
import java.net.URI;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Resource;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.jmx.export.annotation.ManagedOperationParameter;
import org.springframework.jmx.export.annotation.ManagedOperationParameters;
import org.springframework.jmx.export.annotation.ManagedResource;
import org.springframework.stereotype.Service;

import cn.wonhigh.dc.client.common.constans.CommonEnumCollection;
import cn.wonhigh.dc.client.common.constans.CommonEnumCollection.CDCTableColumnEnum;
import cn.wonhigh.dc.client.common.constans.CommonEnumCollection.HiveDefinePartNameEnum;
import cn.wonhigh.dc.client.common.model.ClientTaskStatusLog;
import cn.wonhigh.dc.client.common.model.TaskExeSQLPropertiesConfig;
import cn.wonhigh.dc.client.common.model.TaskPropertiesConfig;
import cn.wonhigh.dc.client.common.util.GernerateUuidUtils;
import cn.wonhigh.dc.client.common.util.HiveUtils;
import cn.wonhigh.dc.client.common.util.ParseSQLXMLFileUtil;
import cn.wonhigh.dc.client.common.util.ParseXMLFileUtil;
import cn.wonhigh.dc.client.manager.cache.RinseStatusAndLogCache;
import cn.wonhigh.dc.client.manager.jms.SendMsg2AMQ;
import cn.wonhigh.dc.client.service.ClientTaskStatusLogService;
import cn.wonhigh.dc.client.service.jms.JmsClusterMgr;

import com.yougou.logistics.base.common.enums.JobBizStatusEnum;
import com.yougou.logistics.base.common.interfaces.RemoteJobServiceExtWithParams;
import com.yougou.logistics.base.common.vo.scheduler.RemoteJobInvokeParamsDto;

/**
 * 执行sql
 *
 * @author xiao.py
 *
 */
@Service
@ManagedResource(objectName = ExecuteSQLTaskImpl.MBEAN_NAME, description = "hive sql执行")
public class ExecuteSQLTaskImpl implements RemoteJobServiceExtWithParams {

	public static final String MBEAN_NAME = "dc:client=ExecuteSQLTaskImpl";

	private static final Logger logger = Logger.getLogger(ExecuteSQLTaskImpl.class);

	@Value("${dc.date.format.default}")
	private String dateFormat = "yyyy-MM-dd HH:mm:ss";

	@Resource
	private ClientTaskStatusLogService clientTaskStatusLogService;

	@Autowired
	private JmsClusterMgr jmsClusterMgr; //发生消息的对象
	
	@Value("${jdbc.hive.timeout}")
	private String jdbcTimeout = "30";

	public void setclientTaskStatusLogService(ClientTaskStatusLogService clientTaskStatusLogService) {
		this.clientTaskStatusLogService = clientTaskStatusLogService;
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
	 */
	@Override
	public void executeJobWithParams(String jobId, String taskName, String groupName,
									 RemoteJobInvokeParamsDto remoteJobInvokeParamsDto) {

		String startTimeStr = null;
		String endTimeStr = null;

		Date syncEndTime = null;
		Date syncBeginTime = null;
		
		long createbeginTime = 0;

		long createEndTime = 0;
		
		JobBizStatusEnum jobBizStatusEnum =  JobBizStatusEnum.INTERRUPTED; 

		//如果重复调用，则忽略本次请求
		jobId = jobId.trim(); //去作用空格
		JobBizStatusEnum jobStauts = RinseStatusAndLogCache.getTaskStatusByJobId(jobId); 
		if (jobStauts != null && !jobStauts.name().equals(JobBizStatusEnum.INTERRUPTED.name())) { 
			logger.info(String.format("【jobId为：%s】的任务被重复调用", jobId));
			return;
		}
		if (remoteJobInvokeParamsDto != null) {  

			startTimeStr = remoteJobInvokeParamsDto.getParam("startTime");
			endTimeStr = remoteJobInvokeParamsDto.getParam("endTime");

			SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);  
			if (startTimeStr != null && endTimeStr != null) {  
				logger.info(String.format("【jobId为：%s】的任务被调用,", jobId) + "开始时间:" + startTimeStr + ";" + "结束时间:"
						+ endTimeStr + ".");
				try {
					syncBeginTime = sdf.parse(startTimeStr);
					syncEndTime = sdf.parse(endTimeStr);
					
					Calendar cal = Calendar.getInstance();
					cal.setTime(syncBeginTime);
					createbeginTime =  cal.getTimeInMillis();
					
					cal.setTime(syncEndTime);
					createEndTime =  cal.getTimeInMillis();
					
					startTimeStr = sdf.format(syncBeginTime);
					endTimeStr = sdf.format(syncEndTime);
					
				} catch (ParseException e) {
					e.printStackTrace();
					String message = String.format("【jobId为：%s】的任务被调用,", jobId) + "日期格式转换异常.";
					logger.error(message);
					jobBizStatusEnum = JobBizStatusEnum.INTERRUPTED; 
					
					addTaskLog(jobId, taskName, groupName, jobBizStatusEnum, message); 
					SendMsg2AMQ.updateStatusAndSend(jobId, jobBizStatusEnum, jmsClusterMgr); 
					RinseStatusAndLogCache.removeTaskByJobId(jobId); 
					return;
				}
			} else { 
				String message = String.format("【jobId为：%s】的任务被调用,", jobId) + "传入的开始和结束时间为空.";
				logger.error(message);
				jobBizStatusEnum = JobBizStatusEnum.INTERRUPTED; 
				
				addTaskLog(jobId, taskName, groupName, jobBizStatusEnum, message); 
				SendMsg2AMQ.updateStatusAndSend(jobId, jobBizStatusEnum, jmsClusterMgr); 
				RinseStatusAndLogCache.removeTaskByJobId(jobId);  
				return;
			}

		} else {  
			String message = String.format("【jobId为：%s】的任务被调用,", jobId) + "传入的参数为空.";
			logger.error(message);
			jobBizStatusEnum = JobBizStatusEnum.INTERRUPTED;
			
			addTaskLog(jobId, taskName, groupName, jobBizStatusEnum, message);
			SendMsg2AMQ.updateStatusAndSend(jobId, jobBizStatusEnum, jmsClusterMgr);
			RinseStatusAndLogCache.removeTaskByJobId(jobId);
			return;
		}
		try {
			taskName = taskName.trim();
			
			jobBizStatusEnum = JobBizStatusEnum.INITIALIZING;  
			String message = "初始化中。。。。。。";
			addTaskLog(jobId, taskName, groupName, jobBizStatusEnum, message);  
			logger.info(message);
			SendMsg2AMQ.updateStatusAndSend(jobId, jobBizStatusEnum, jmsClusterMgr); 

			

			message  = "开始读取执行sql的xml任务配置信息,";
			logger.info(String.format("***********>%s：【groupName：%s】【triggerName：%s】", message, groupName, taskName));

			TaskExeSQLPropertiesConfig taskConfig = checkTaskExecCondition(groupName, taskName, jobId);  
			if (taskConfig == null) { 
				jobBizStatusEnum = JobBizStatusEnum.INTERRUPTED;  
				message = "获取执行sql的xml任务配置信息失败";
				logger.error(String.format("%s：【groupName：%s】【triggerName：%s】", message, groupName, taskName));
				
				addTaskLog(jobId, taskName, groupName, jobBizStatusEnum, message);
				
				SendMsg2AMQ.updateStatusAndSend(jobId, jobBizStatusEnum, jmsClusterMgr);
				RinseStatusAndLogCache.removeTaskByJobId(jobId);
				return;
			}
			
			Integer triggerNameIndex = getTriggerNameIndex(taskName);
			
			if (triggerNameIndex == null) { 
				
				Integer parentTaskConfig = taskConfig.getDependencyTaskIds() != null ? taskConfig
						.getDependencyTaskIds().get(0) : null;  
				if (parentTaskConfig == null) { 
					jobBizStatusEnum = JobBizStatusEnum.INTERRUPTED;  
					message = "获取执行sql的xml任务配置信息没有设置依赖id";
					logger.error(String.format("%s：【groupName：%s】【triggerName：%s】", message, groupName, taskName));
					
					addTaskLog(jobId, taskName, groupName, jobBizStatusEnum, message);
					
					SendMsg2AMQ.updateStatusAndSend(jobId, jobBizStatusEnum, jmsClusterMgr);
					RinseStatusAndLogCache.removeTaskByJobId(jobId);
					return;
				}				
			}
		
			jobBizStatusEnum = JobBizStatusEnum.RUNNING; 
			addTaskLog(jobId, taskName, groupName, jobBizStatusEnum, "运行中");
			SendMsg2AMQ.updateStatusAndSend(jobId, jobBizStatusEnum, jmsClusterMgr);
			
			if(taskConfig.getIsAddTemporaryUdf() == 1){
				//create TEMPORARY function  md5  as 'com.hugedata.hive.udf.codec.UDFMd5' USING JAR ${HiveUDFJarPath}
				/**
				 * create function strip_new as 'com.hadoopbook.hive.Strip' using jar 'hdfs:/user/cloudera/strip.jar'
				 * sql = "add jar /home/hadoop/cityudf.jar";  
         stmt.execute(sql);  
         //加入到classpath下  
         sql = "create temporary function cityudf as 'com.hive.utf.CityUDF'";  
				 */
				/*StringBuilder sbBuilderJar = new StringBuilder();
				sbBuilderJar.append("add jar ");
				sbBuilderJar.append("/home/hive/xiao.py/developudf.jar");
				
				String temporaryUdfPath = String.format("开始执行【groupName：%s】【triggerName：%s】添加临时udf路径：%s", groupName, taskName,sbBuilderJar.toString());
				logger.info(temporaryUdfPath);
				//HiveUtils.execute(taskConfig.getSourceDbEntity(),taskConfig,sbBuilder.toString(),Integer.parseInt(jdbcTimeout));
				HiveUtils.execute(taskConfig,sbBuilderJar.toString(),Integer.parseInt(jdbcTimeout));
				String temporaryUdfPathEnd = String.format("开始执行【groupName：%s】【triggerName：%s】添加临时udf路径成功", groupName, taskName);
				logger.info(temporaryUdfPathEnd);
				
				StringBuilder sbBuilder = new StringBuilder();
				sbBuilder.append("create TEMPORARY function ");
				sbBuilder.append(taskConfig.getTemporaryUdfName());
				sbBuilder.append(" as ");
				sbBuilder.append("'");
				sbBuilder.append(taskConfig.getTemporaryUdfClass().trim());
				sbBuilder.append("'");				
				String temporaryUdfString = String.format("开始执行【groupName：%s】【triggerName：%s】添加临时udf函数：%s", groupName, taskName,sbBuilder.toString());
				logger.info(temporaryUdfString);
				//HiveUtils.execute(taskConfig.getSourceDbEntity(),taskConfig,sbBuilder.toString(),Integer.parseInt(jdbcTimeout));
				HiveUtils.execute(taskConfig,sbBuilder.toString(),Integer.parseInt(jdbcTimeout));
				String temporaryUdfEnd = String.format("开始执行【groupName：%s】【triggerName：%s】添加临时udf函数成功", groupName, taskName);
				logger.info(temporaryUdfEnd);*/
				
				String dropUdf = "DROP FUNCTION IF EXISTS " + taskConfig.getTemporaryUdfName();
				String dropUdfName = String.format("开始执行【groupName：%s】【triggerName：%s】删除udf函数：%s", groupName,taskName, dropUdf);
				logger.info(dropUdfName);
				HiveUtils.execute(taskConfig.getSourceDbEntity(),taskConfig,dropUdf,Integer.parseInt(jdbcTimeout));
				//HiveUtils.execute(taskConfig,dropUdf,Integer.parseInt(jdbcTimeout));
				String dropUdfNameEnd = String.format("开始执行【groupName：%s】【triggerName：%s】删除udf函数成功", groupName, taskName);
				logger.info(dropUdfNameEnd);
				
				StringBuilder sbBuilderJar = new StringBuilder();
				sbBuilderJar.append("CREATE FUNCTION ");
				sbBuilderJar.append(taskConfig.getTemporaryUdfName());
				sbBuilderJar.append(" as '");
				sbBuilderJar.append(taskConfig.getTemporaryUdfClass().trim());
				sbBuilderJar.append("' ");
				sbBuilderJar.append("using jar ");
				sbBuilderJar.append("'");
				sbBuilderJar.append(taskConfig.getTemporaryUdfPath());
				sbBuilderJar.append("'");				
				
				String temporaryUdfPath = String.format("开始执行【groupName：%s】【triggerName：%s】添加临时udf函数：%s", groupName, taskName,sbBuilderJar.toString());
				logger.info(temporaryUdfPath);
				HiveUtils.execute(taskConfig.getSourceDbEntity(),taskConfig,sbBuilderJar.toString(),Integer.parseInt(jdbcTimeout));
				//HiveUtils.execute(taskConfig,sbBuilderJar.toString(),Integer.parseInt(jdbcTimeout));
				String temporaryUdfPathEnd = String.format("执行【groupName：%s】【triggerName：%s】添加临时udf函数成功", groupName, taskName);
				logger.info(temporaryUdfPathEnd);
				
			}
			
			
			String executeSql = ParseSQLXMLFileUtil.specialCharReplace(taskConfig.getExecuteSql().trim());
			if(taskConfig.getSchedulTimeparameter() != null && StringUtils.isNotBlank(taskConfig.getSchedulTimeparameter())){  //需要从调度系统传递时间参数
				String [] timeParameter = taskConfig.getSchedulTimeparameter().split(",");
				
				if(timeParameter != null && timeParameter.length == 1){  //只有一个时间参数   把开始时间替换第一个参数
					if (executeSql.contains(timeParameter[0])) {  //sql里面有第一个参数
						executeSql = executeSql.replace(timeParameter[0], startTimeStr);  //替换时间string类型
						//if(executeSql.toLowerCase().startsWith("create")){ //如果是create语句
							//executeSql = executeSql.replace(timeParameter[0], String.valueOf(createbeginTime)); //替换时间long类型
						//}else{ //不是create语句
							
						//}						
					}					
				}else if(timeParameter != null && timeParameter.length == 2){  //两个以上的时间参数  把开始时间替换第一个参数  ，结束时间替换第二个参数
					if (executeSql.contains(timeParameter[0])) {
						executeSql = executeSql.replace(timeParameter[0], startTimeStr);  //替换时间string类型						
					}
					
					if (executeSql.contains(timeParameter[1])) {
						executeSql = executeSql.replace(timeParameter[1], endTimeStr); //替换时间string类型
						//if(executeSql.toLowerCase().startsWith("create")){ //如果是create语句
							//executeSql = executeSql.replace(timeParameter[1], String.valueOf(createEndTime)); //替换时间long类型
						//}else{ //不是create语句							
						//}						
					}					
				}else if(timeParameter != null && timeParameter.length == 3){
					if (executeSql.contains(timeParameter[0])) {
						executeSql = executeSql.replace(timeParameter[0], startTimeStr);  //替换时间string类型						
					}
					if (executeSql.contains(timeParameter[1])) {
						executeSql = executeSql.replace(timeParameter[1], endTimeStr); //替换时间string类型										
					}
					if (executeSql.contains(timeParameter[2])) {
						executeSql = executeSql.replace(timeParameter[2], String.valueOf(createbeginTime)); //替换时间long类型										
					}
				}else if(timeParameter != null && timeParameter.length >= 4){
					if (executeSql.contains(timeParameter[0])) {
						executeSql = executeSql.replace(timeParameter[0], startTimeStr);  //替换时间string类型						
					}
					if (executeSql.contains(timeParameter[1])) {
						executeSql = executeSql.replace(timeParameter[1], endTimeStr); //替换时间string类型										
					}
					if (executeSql.contains(timeParameter[2])) {
						executeSql = executeSql.replace(timeParameter[2], String.valueOf(createbeginTime)); //替换时间long类型										
					}
					if (executeSql.contains(timeParameter[3])) {
						executeSql = executeSql.replace(timeParameter[3], String.valueOf(createEndTime)); //替换时间long类型										
					}
				}				
			}	
			String sqlString = String.format("开始执行【groupName：%s】【triggerName：%s】的sql语句：%s", groupName, taskName,executeSql);
			logger.info(sqlString);			
			logger.info(taskConfig.getSourceDbEntity() + executeSql + Integer.parseInt(jdbcTimeout));
			if(executeSql.toLowerCase().startsWith("insert") ||					
					executeSql.toLowerCase().startsWith("update") ||
					executeSql.toLowerCase().startsWith("delete") ||
					executeSql.startsWith("truncate")){
				HiveUtils.executeUpdate(taskConfig.getSourceDbEntity(),taskConfig,executeSql,Integer.parseInt(jdbcTimeout));
			}else{
				HiveUtils.execute(taskConfig.getSourceDbEntity(),taskConfig,executeSql,Integer.parseInt(jdbcTimeout));
			}
			String sqlStringEnd = String.format("执行【groupName：%s】【triggerName：%s】的sql语句成功", groupName, taskName);
			logger.info(sqlStringEnd);
			
			/*if(taskConfig.getIsDownload() == 1){
				//select INPUT__FILE__NAME from testTime;
				String downString = null ;
				if(taskConfig.getDownloadTableName().contains("/")){ //路径
					downString = taskConfig.getDownloadTableName();  
				}else{ //表名
					String downSqlString = "select INPUT__FILE__NAME from " + taskConfig.getDownloadTableName() + "  limit 1";
					ArrayList<Object []> resultlist = HiveUtils.executeQuery(taskConfig.getSourceDbEntity(),taskConfig,downSqlString,Integer.parseInt(jdbcTimeout));
					//ArrayList<Object []> resultlist = HiveUtils.executeQuery(taskConfig,downSqlString,Integer.parseInt(jdbcTimeout));
					if(resultlist != null){
						Object [] arr = resultlist.get(0);
						downString = arr[0].toString();
						logger.info("需要下载表的文件的路径为" + downString);
					}else{
						// 异常处理
						jobBizStatusEnum = JobBizStatusEnum.INTERRUPTED;
						String err = String.format("执行【groupName：%s】【triggerName：%s】的【表：%s 】的源路径的结果为null或空", groupName, taskName,taskConfig.getDownloadTableName());
						logger.error(err);
						addTaskLog(jobId, taskName, groupName, jobBizStatusEnum,err);
						RinseStatusAndLogCache.removeTaskByJobId(jobId); 
						SendMsg2AMQ.updateStatusAndSend(jobId, jobBizStatusEnum, jmsClusterMgr);
						return;
					}					
				}
				
				if(downString != null && StringUtils.isNotBlank(downString)){
					logger.info("需要下载文件的地址为" + downString);
					Configuration conf = new Configuration();
					conf.setBoolean("ipc.client.fallback-to-simple-auth-allowed", true);
					FileSystem fs = FileSystem.get(new URI(downString), conf);
					File file = new File(taskConfig.getDownloadPath());
					if(!file.exists()){
						file.mkdirs();
					  }
					file.setExecutable(true);
				    file.setReadable(true);
				    file.setWritable(true);
					logger.info("数据源文件的地址为" + downString + "*****下载到目的地为" + taskConfig.getDownloadPath());
					fs.copyToLocalFile(new Path(downString), new Path(taskConfig.getDownloadPath())); 
					logger.info("文件下载完成");
				}else{
					// 异常处理
					jobBizStatusEnum = JobBizStatusEnum.INTERRUPTED; //中断
					String err1 = String.format("执行【groupName：%s】【triggerName：%s】的下载路径为null或空", groupName, taskName);
					logger.error(err1);
					addTaskLog(jobId, taskName, groupName, jobBizStatusEnum,err1);
					RinseStatusAndLogCache.removeTaskByJobId(jobId); 
					SendMsg2AMQ.updateStatusAndSend(jobId, jobBizStatusEnum, jmsClusterMgr);
					return;
				}
				
			}*/
			
			String msgCdc = String.format("全部sql执行完成:【groupName：%s】【triggerName：%s】", groupName, taskName);
			logger.info(msgCdc);

			jobBizStatusEnum = JobBizStatusEnum.FINISHED; //完成
			addTaskLog(jobId, taskName, groupName, jobBizStatusEnum, "全部sql执行完成...");
			SendMsg2AMQ.updateStatusAndSend(jobId, jobBizStatusEnum, jmsClusterMgr);
			
		} catch (Exception e) {
			
			jobBizStatusEnum = JobBizStatusEnum.INTERRUPTED; 
			String errMsg = String.format("【groupName：%s】【triggerName：%s】导入出现异常：%s", groupName, taskName,
					e.getMessage());
			logger.error(errMsg, e);
			addTaskLog(jobId, taskName, groupName, jobBizStatusEnum,
					"执行ExecuteSQL:" + "中断:" + errMsg.substring(0, Math.min(errMsg.length(), 800)));
			RinseStatusAndLogCache.removeTaskByJobId(jobId); 
			SendMsg2AMQ.updateStatusAndSend(jobId, jobBizStatusEnum, jmsClusterMgr);
			return;
		}
	}
	/**
	 * 检查任务执行所需的必要条件
	 * @param groupName
	 * @param triggerName
	 * @param taskId
	 * @return  任务属性配置
	 */
	private TaskExeSQLPropertiesConfig checkTaskExecCondition(String groupName, String triggerName, String taskId) {
		TaskExeSQLPropertiesConfig taskConfig = ParseSQLXMLFileUtil.getTaskConfig(groupName, triggerName);  //通过组名和任务名获得任务属性配置
		if (taskConfig == null) {
			return null;
		}
		
		// 判断是ID 执行sql 数据源id是否为空
		if (taskConfig.getId() == null || taskConfig.getSourceDbId() == null || taskConfig.getExecuteSql() == null ) {
			return null;
		}
		
		if(taskConfig.getIsAddTemporaryUdf() == 1){
			if(taskConfig.getTemporaryUdfName() == null || taskConfig.getTemporaryUdfPath() == null){
				return null;
			}
		}
		
		/*if(taskConfig.getIsDownload() == 1){
			if(taskConfig.getDownloadTableName() == null || taskConfig.getDownloadPath() == null){
				return null;
			}
		}*/
		
		return taskConfig;
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
	
	
	public Integer getTriggerNameIndex(String triggerName){
		String [] spTriggerName= triggerName.trim().split("_");
		if(Integer.parseInt(spTriggerName[spTriggerName.length - 1]) == 1){
			return 1;
		}
		return null;
	}
	
	
	@ManagedOperation(description = "job simulator")
	@ManagedOperationParameters({ @ManagedOperationParameter(description = "dc_test", name = "shop_1"),
			@ManagedOperationParameter(description = "group name", name = "groupName") })
	public void simulateJob(String triggerName, String groupName) {
		logger.info("Test the job trigger from simulator.");
		RemoteJobInvokeParamsDto rD = new RemoteJobInvokeParamsDto();
		rD.addParam("startTime", "2017-02-08 00:00:00");
		rD.addParam("endTime", "2017-02-09 00:00:00");
		executeJobWithParams(GernerateUuidUtils.getUUID(), triggerName, groupName, rD);
	}
}
