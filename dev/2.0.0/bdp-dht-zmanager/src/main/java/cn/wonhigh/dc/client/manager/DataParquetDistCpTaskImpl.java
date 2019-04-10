package cn.wonhigh.dc.client.manager;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.annotation.Resource;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.tools.DistCpOptions;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.jmx.export.annotation.ManagedOperationParameter;
import org.springframework.jmx.export.annotation.ManagedOperationParameters;
import org.springframework.jmx.export.annotation.ManagedResource;
import org.springframework.stereotype.Service;

import com.yougou.logistics.base.common.enums.JobBizStatusEnum;
import com.yougou.logistics.base.common.exception.ManagerException;
import com.yougou.logistics.base.common.interfaces.RemoteJobServiceExtWithParams;
import com.yougou.logistics.base.common.vo.scheduler.RemoteJobInvokeParamsDto;

import cn.wonhigh.dc.client.common.constans.CommonEnumCollection.DateFormatStrEnum;
import cn.wonhigh.dc.client.common.constans.CommonEnumCollection.HiveDefinePartNameEnum;
import cn.wonhigh.dc.client.common.constans.MessageConstant;
import cn.wonhigh.dc.client.common.model.ClientTaskStatusLog;
import cn.wonhigh.dc.client.common.model.TaskPropertiesConfig;
import cn.wonhigh.dc.client.common.util.DateUtils;
import cn.wonhigh.dc.client.common.util.GernerateUuidUtils;
import cn.wonhigh.dc.client.common.util.HiveUtils;
import cn.wonhigh.dc.client.common.util.ParseXMLFileUtil;
import cn.wonhigh.dc.client.common.util.PropertyFile;
import cn.wonhigh.dc.client.manager.cache.RinseStatusAndLogCache;
import cn.wonhigh.dc.client.manager.jms.SendMsg2AMQ;
import cn.wonhigh.dc.client.service.ClientTaskStatusLogService;
import cn.wonhigh.dc.client.service.jms.JmsClusterMgr;

/**
 * 拷贝parquet存储格式的表
 * 
 * @author user
 *
 */
@Service
@ManagedResource(objectName = DataParquetDistCpTaskImpl.MBEAN_NAME, description = "数据迁移(数据存储格式为parquet)")
public class DataParquetDistCpTaskImpl implements RemoteJobServiceExtWithParams {

	// JMX注册名称
	public static final String MBEAN_NAME = "dc:client=DataParquetDistCpTaskImpl";

	private static final Logger logger = Logger.getLogger(DataParquetDistCpTaskImpl.class);

	private String dateFormat = DateFormatStrEnum.JAVA_YYYY_MM_DD.getValue();

	private String wmsFormat = DateFormatStrEnum.PARTITION_DATE_YYYY_MM.getValue();
	Map<String, ParentsJobId> repairByParentsId = new HashMap<String, ParentsJobId>();
	@Resource
	private ClientTaskStatusLogService clientTaskStatusLogService;

	@Autowired
	private JmsClusterMgr jmsClusterMgr;

	private static FileSystem fs = null;
	private static List specialTables = null;

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

	@Value("${bdp.fs.default}")
	private String bdpFsPath;

	@Value("${cdh.hive.path}")
	private String cdhHivePath;

	@Value("${bdp.hive.path}")
	private String bdpHivePath;

	@Override
	public void executeJobWithParams(String jobId, String taskName, String groupName,
			RemoteJobInvokeParamsDto remoteJobInvokeParamsDto) {
		validateJobParams(jobId, taskName, groupName, remoteJobInvokeParamsDto);// 验证Job的参数
		logger.info(String.format("【jobId为：%s】--->【taskName为：%s】参数验证通过...开始执行任务", jobId, groupName + "_" + taskName));
		logger.info(String.format("获取参数bdpHivePath--->%s;cdhHivePath--->%s;bdpHivePath--->%s", bdpFsPath, cdhHivePath,
				bdpHivePath));
		// 如果重复调用，则忽略本次请求
		// 获取kerberos验证文件
		// 默认：这里不设置的话，win默认会到 C盘下读取krb5.init
		Properties proprties = PropertyFile.getProprties(MessageConstant.PROP_FILE_NAME);
		String specialStr = proprties.getProperty("special.table");
		if (null != specialStr) {
			specialTables = Arrays.asList(specialStr.split(","));
			logger.info("加载特殊表成功");
		}
		Configuration conf = new Configuration();
		try {
			String startTimeStr = remoteJobInvokeParamsDto.getParam("startTime");
			String endTimeStr = remoteJobInvokeParamsDto.getParam("endTime");
			logger.info(String.format("获取参数startTime--->%s;endTime--->%s", startTimeStr, endTimeStr));
			executeDistcpJob(jobId, groupName, conf, taskName, startTimeStr, endTimeStr);
		} catch (Exception e) {
			logger.error(String.format("%s --->distcp job拷贝失败", ""), e);
		}
	}

	private void validateJobParams(String jobId, String taskName, String groupName,
			RemoteJobInvokeParamsDto remoteJobInvokeParamsDto) {
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
			if (repairByParentsId.containsKey(key)) {
				ParentsJobId paraentParam = repairByParentsId.get(key);
				paraentParam.setPrevJobId(parentJobIdStr);
				logger.info(String.format("has key then repairByParentsId[%s] = %s", key,
						repairByParentsId.get(key).getPrevJobId()));
			} else {
				ParentsJobId paraentParam = new ParentsJobId();
				paraentParam.setPrevJobId(parentJobIdStr);
				repairByParentsId.put(key, paraentParam);
				logger.info(String.format("new repairByParentsId[%s] = %s", key,
						repairByParentsId.get(key).getPrevJobId()));
			}
			SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
			if (startTimeStr != null && endTimeStr != null) {
				logger.info(String.format("【jobId为：%s】的任务被调用,", jobId) + "开始时间:" + startTimeStr + ";" + "结束时间:"
						+ endTimeStr + ".");
				try {
					syncBeginTime = sdf.parse(startTimeStr);
					syncEndTime = sdf.parse(endTimeStr);
				} catch (ParseException e) {
					logger.error(e.getMessage(), e);
					return;
				}
				// if (syncBeginTime.after(syncEndTime)) {
				// String message = String.format("【jobId为：%s】的任务被调用,", jobId) +
				// "开始时间:" + startTimeStr + "超过"
				// + "结束时间:" + endTimeStr + ",结束时间在开始时间在之前";
				// logger.error(message);
				// jobBizStatusEnum = JobBizStatusEnum.INTERRUPTED;
				// // 任务状态日志入库
				// addTaskLog(jobId, taskName, groupName, jobBizStatusEnum,
				// message);
				// SendMsg2AMQ.updateStatusAndSend(jobId, jobBizStatusEnum,
				// jmsClusterMgr);
				// RinseStatusAndLogCache.removeTaskByJobId(jobId);
				// return;
				// }
			} else {
				String message = String.format("【jobId为：%s】的任务被调用,", jobId) + "传入的开始和结束时间为空.";
				logger.error(message);
				jobBizStatusEnum = JobBizStatusEnum.INTERRUPTED;
				// 任务状态日志入库
				addTaskLog(jobId, taskName, groupName, jobBizStatusEnum, message);
				SendMsg2AMQ.updateStatusAndSend(jobId, jobBizStatusEnum, jmsClusterMgr);
				RinseStatusAndLogCache.removeTaskByJobId(jobId);
				return;
			}

		} else {
			String message = String.format("【jobId为：%s】的任务被调用,", jobId) + "传入的参数为空.";
			logger.error(message);
			jobBizStatusEnum = JobBizStatusEnum.INTERRUPTED;
			// 任务状态日志入库
			addTaskLog(jobId, taskName, groupName, jobBizStatusEnum, message);
			SendMsg2AMQ.updateStatusAndSend(jobId, jobBizStatusEnum, jmsClusterMgr);
			RinseStatusAndLogCache.removeTaskByJobId(jobId);
			return;
		}

	}

	/**
	 * 执行拷贝任务
	 * 
	 * @param jobId
	 * @param groupName
	 * @param conf
	 * @param taskName
	 * @param startTimeStr
	 * @param endTimeStr
	 * @throws ParseException
	 */
	private void executeDistcpJob(String jobId, String groupName, Configuration conf, String taskName,
			String startTimeStr, String endTimeStr) throws ParseException {
		String sourceInfo = "";
		JobBizStatusEnum jobBizStatusEnum;
		String isUpdate = "";
		String tableName = "";
		Map<String, String> distcpMap = new HashMap<String, String>();
		Map<String, String> specialMap = new HashMap<String, String>();
		String sourcePath = "";
		String targetPath = "";
		// 初始化
		try {
			// 添加CDH的hadoop系统的信息
			logger.info("加载CDH集群信息");
			conf.addResource(new Path(PropertyFile.getHdfsInfoPath()[2]));
			conf.addResource(new Path(PropertyFile.getHdfsInfoPath()[3]));
			logger.info(String.format("加载CDH集群信息成功..【core-site:%s;hdfs-site:%s】", PropertyFile.getHdfsInfoPath()[2],
					PropertyFile.getHdfsInfoPath()[3]));
			FileSystem tempFS = FileSystem.get(conf);
			InetSocketAddress active = HAUtil.getAddressOfActive(tempFS);
			InetAddress address = active.getAddress();
			// webhdfs://10.234.8.166:50070/user/hive/warehouse/gtp_tmp.db/
			logger.info(String.format("【获取CDH集群active节点IP===>%s】", address.getHostAddress()));
			String cdhPath = "webhdfs://" + address.getHostAddress() + ":50070" + cdhHivePath;
			TaskPropertiesConfig taskConfig = ParseXMLFileUtil.getTaskConfig(groupName, taskName);
			isUpdate = String.valueOf(taskConfig.getIsOverwrite()); // 是否增量拷贝
			tableName = taskName.replaceAll("_src_", "_src");// 任务名称
			List<String> dayList = new ArrayList<String>();
			List<String> days = new ArrayList<String>();
			if (isUpdate.equals("0")) {// ①如果增量导入,取到相应的日期构建路径
				SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
				Date startDate = sdf.parse(startTimeStr);
				Date endDate = sdf.parse(endTimeStr);
				// if (tableName.equals("gyl_wms_city_bill_im_import_src")
				// || tableName.equals("gyl_wms_city_bill_im_import_dtl_src")
				// || tableName.equals("blf1_bl_wms_check_dtl_src") ||
				// tableName.equals("blf1_bl_wms_check_src")
				// || tableName.equals("blf1_bl_wms_deliver_dtl_src")) {
				if (specialTables.contains(tableName.trim())) {
					dayList = DateUtils.getMonthsByBeginAndEnd(startDate, endDate, dateFormat);
					days = DateUtils.getDaysByBeginAndEnd(startDate, endDate, dateFormat);
					for (String dayPath : days) {
						sourceInfo = tableName + "/"
								+ HiveDefinePartNameEnum.PARTITION_DATE_NAME.getValue().toLowerCase() + "=" + dayPath;
						sourcePath = bdpHivePath + sourceInfo;
						targetPath = cdhPath + sourceInfo;
						specialMap.put(sourcePath, targetPath);
					}
					logger.info(String.format("【※※※%s※※※】增量导入区间【%s个】===>【%s】", tableName, specialMap.size(),
							days.toString().replaceAll("[\\[\\]]+", "")));
				} else {
					dayList = DateUtils.getDaysByBeginAndEnd(startDate, endDate, dateFormat);
					logger.info(String.format("【%s】增量导入区间【%s个】===>【%s】", tableName, distcpMap.size(),
							dayList.toString().replaceAll("[\\[\\]]+", "")));
				}
				for (String dayPath : dayList) {
					sourceInfo = tableName + "/" + HiveDefinePartNameEnum.PARTITION_DATE_NAME.getValue().toLowerCase()
							+ "=" + dayPath;
					sourcePath = bdpHivePath + sourceInfo;
					targetPath = cdhPath + sourceInfo;
					distcpMap.put(sourcePath, targetPath);
				}
			} else {// ②如果全量导入,获取表名称构建路径
				sourceInfo = tableName;
				sourcePath = bdpHivePath + sourceInfo;
				targetPath = cdhPath + sourceInfo;
				distcpMap.put(sourcePath, targetPath);
				// if (tableName.equals("gyl_wms_city_bill_im_import_src")
				// || tableName.equals("gyl_wms_city_bill_im_import_dtl_src")
				// || tableName.equals("blf1_bl_wms_check_dtl_src") ||
				// tableName.equals("blf1_bl_wms_check_src")
				// || tableName.equals("blf1_bl_wms_deliver_dtl_src")) {
				if (specialTables.contains(tableName.trim())) {
					logger.info(String.format("表【※※※%s※※※】全量导入,【%s】===>【%s】", tableName, sourcePath, targetPath));
				} else {
					logger.info(String.format("表【%s】全量导入,【%s】===>【%s】", tableName, sourcePath, targetPath));
				}
			}
			prepare4Distcp(jobId, groupName, conf, tableName, isUpdate, dayList, distcpMap, taskConfig, startTimeStr,
					endTimeStr, specialMap);
		} catch (Exception e) {
			String message = String.format("【传入参数taskName--->%s异常】", taskName);
			logger.error(message);
			jobBizStatusEnum = JobBizStatusEnum.INTERRUPTED;
			// 任务状态日志入库
			addTaskLog(jobId, taskName, groupName, jobBizStatusEnum, message);
			SendMsg2AMQ.updateStatusAndSend(jobId, jobBizStatusEnum, jmsClusterMgr);
			RinseStatusAndLogCache.removeTaskByJobId(jobId);
			return;
		}
	}

	/**
	 * 过滤符合条件的路径信息
	 * 
	 * @param listStatusPath
	 * @param distcpMap
	 * @param isSpecil
	 * @return
	 */

	private Map<String, String> filterSourcePaths(String listStatusPath, Map<String, String> distcpMap,
			String isUpdate) {
		final Map<String, String> newDistcpMap = new HashMap<String, String>();
		if (!isUpdate.equals("0")) {// 如果全量拷贝
			return distcpMap;
		}
		try {
			for (Map.Entry<String, String> entry : distcpMap.entrySet()) {
				final String key = entry.getKey();
				final String value = entry.getValue();
				fs.listStatus(new Path(listStatusPath), new PathFilter() {
					@Override
					public boolean accept(Path path) {
						try {// 文件存在并且存在数据
							if (key.equals(path.toString())) {
								FileStatus[] listStatus = fs.listStatus(new Path(key));
								if (listStatus.length > 0) {
									newDistcpMap.put(key, value);
								}
							}
						} catch (Exception e) {
							logger.error(String.format("过滤gtp库文件异常.."), e);
						}
						return true;
					}
				});
			}
		} catch (Exception e) {
			logger.error(String.format("【%s】文件系统路径错误..", listStatusPath), e);
		}
		return newDistcpMap;
	}

	/**
	 * 执行拷贝
	 * 
	 * @param conf
	 * @param sourcePath
	 */
	private void executeDistcp(String jobId, String groupName, Configuration conf, String tableName, String isUpdate,
			Map<String, String> distcpMap, TaskPropertiesConfig taskConfig) {
		List<Path> sourceList = null;
		JobBizStatusEnum jobBizStatusEnum = null;
		String runMsg = "";
		boolean isComplete = false;
		DistCp scp;
		try {
			logger.info("-----------------------------拷贝中.-----------------------------");
			runMsg = "运行中";
			jobBizStatusEnum = JobBizStatusEnum.RUNNING;
			addTaskLog(jobId, tableName, groupName, jobBizStatusEnum, runMsg);
			SendMsg2AMQ.updateStatusAndSend(jobId, jobBizStatusEnum, jmsClusterMgr);
			for (Map.Entry<String, String> entry : distcpMap.entrySet()) {
				sourceList = new ArrayList<Path>();
				String sourcePath = entry.getKey();
				sourceList.add(new Path(sourcePath));
				String targetPath = entry.getValue();
				DistCpOptions scpOptions = new DistCpOptions(sourceList, new Path(targetPath));
				// scpOptions.setIgnoreFailures(true);//忽略失败检测
				// scpOptions.setOverwrite(true);// 重写
				scpOptions.setSyncFolder(true);
				scpOptions.setDeleteMissing(true);
				scpOptions.setSkipCRC(true);
				if (tableName.equals("gyl_wms_city_bill_im_import_src")
						|| tableName.equals("gyl_wms_city_bill_im_import_dtl_src")
						|| tableName.equals("blf1_bl_wms_check_dtl_src") || tableName.equals("blf1_bl_wms_check_src")
						|| tableName.equals("blf1_bl_wms_deliver_dtl_src")) {
					logger.info(String.format("表【※※※%s※※※执行distcp开始】,【%s===>%s】", tableName, sourcePath, targetPath));
				} else {
					logger.info(String.format("表【%s执行distcp开始】,【%s===>%s】", tableName, sourcePath, targetPath));
				}
				scp = new DistCp(conf, scpOptions);
				Job job = scp.execute();
				isComplete = job.waitForCompletion(true);
			}
		} catch (Exception e) {
			logger.error(String.format("【执行拷贝distcp失败】"), e);
			runMsg = "中断";
			jobBizStatusEnum = JobBizStatusEnum.INTERRUPTED;
			addTaskLog(jobId, tableName, groupName, jobBizStatusEnum, runMsg);
			RinseStatusAndLogCache.removeTaskByJobId(jobId);
			SendMsg2AMQ.updateStatusAndSend(jobId, jobBizStatusEnum, jmsClusterMgr);
			return;
		} finally {
			if (isComplete) {
				logger.info(String.format("表【%s" + "拷贝完成】", tableName));
				logger.info(String.format("				↓↓↓↓↓↓拷贝信息↓↓↓↓↓↓				"));
				for (Map.Entry<String, String> res : distcpMap.entrySet()) {
					String key = res.getKey();
					String value = res.getValue();
					logger.info(String.format("【%s===>%s】", key, value));
				}
				jobBizStatusEnum = JobBizStatusEnum.FINISHED;
				addTaskLog(jobId, tableName, groupName, jobBizStatusEnum, String.format("表【%s" + "拷贝完成:】", tableName),
						new Date(), new Date());
				SendMsg2AMQ.updateStatusAndSend(jobId, jobBizStatusEnum, jmsClusterMgr);
				RinseStatusAndLogCache.removeTaskByJobId(jobId);
			}
		}

	}

	@ManagedOperation(description = "job simulator")
	@ManagedOperationParameters({ @ManagedOperationParameter(description = "trigger name", name = "triggerName"),
			@ManagedOperationParameter(description = "group name", name = "groupName") })
	public void simulateJob(String triggerName, String groupName) {
		logger.info("Test the job trigger from simulator.");
		RemoteJobInvokeParamsDto rD = new RemoteJobInvokeParamsDto();
		rD.addParam("startTime", "2018-05-25 00:00:00");
		rD.addParam("endTime", "2018-06-05 00:00:00");
		executeJobWithParams(GernerateUuidUtils.getUUID(), triggerName, groupName, rD);
	}

	@Override
	public JobBizStatusEnum getJobStatus(String arg0, String arg1, String arg2) {
		return null;
	}

	@Override
	public String getLogs(String arg0, String arg1, String arg2, long arg3) {
		return null;
	}

	@Override
	public void initializeJob(String arg0, String arg1, String arg2) {
	}

	@Override
	public void pauseJob(String arg0, String arg1, String arg2) {
	}

	@Override
	public void restartJob(String arg0, String arg1, String arg2) {
	}

	@Override
	public void resumeJob(String arg0, String arg1, String arg2) {
	}

	@Override
	public void stopJob(String arg0, String arg1, String arg2) {
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
	 * 拷贝准备
	 * 
	 * @param jobId
	 * @param groupName
	 * @param conf
	 * @param tableName
	 * @param isUpdate
	 * @param dayList
	 * @param distcpMap
	 * @param taskConfig
	 * @throws ManagerException
	 */
	private void prepare4Distcp(String jobId, String groupName, Configuration conf, String tableName, String isUpdate,
			List<String> dayList, Map<String, String> distcpMap, TaskPropertiesConfig taskConfig, String startTimeStr,
			String endTimeStr, Map<String, String> specialMap) throws ManagerException {
		String runMsg = "";
		JobBizStatusEnum jobBizStatusEnum = null;
		// ①清除表中数据
		StringBuilder sql = new StringBuilder();
		try {
			// 转移txt表的数据至parquet中
			sql.append("truncate table ");
			sql.append("gtp.");// 默认库创建
			sql.append(tableName);
			String message = String.format("清空表【gtp.%s】开始,表类型【parquet】执行sql:【%s】", tableName, sql.toString());
			logger.info(message);
			boolean isTruncate = false;
			if (null != taskConfig) {
				isTruncate = HiveUtils.execUpdate(taskConfig.getTargetDbEntity(), taskConfig, "", sql.toString(), 20);
			} else {
				runMsg = String.format("任务【%s】XML配置文件读取失败,请配置...", groupName + tableName);
				logger.error(runMsg, new Exception());
				// 任务状态日志入库
				jobBizStatusEnum = JobBizStatusEnum.INTERRUPTED;
				addTaskLog(jobId, tableName, groupName, jobBizStatusEnum, runMsg);
				SendMsg2AMQ.updateStatusAndSend(jobId, jobBizStatusEnum, jmsClusterMgr);
				RinseStatusAndLogCache.removeTaskByJobId(jobId);
				return;
			}
			if (isTruncate) { // 清空表成功
				jobBizStatusEnum = JobBizStatusEnum.RUNNING;
				runMsg = "运行中";
				addTaskLog(jobId, tableName, groupName, jobBizStatusEnum, runMsg);
				SendMsg2AMQ.updateStatusAndSend(jobId, jobBizStatusEnum, jmsClusterMgr);
				message = String.format("表【gtp.%s】已清空 ", tableName);
				logger.info(message);
				sql = new StringBuilder();
				// 转移txt表的数据至parquet中
				sql.append("insert into table ");
				sql.append("gtp.");// 默认库创建
				sql.append(tableName);
				sql.append(" partition(");
				sql.append(HiveDefinePartNameEnum.PARTITION_DATE_NAME.getValue());
				sql.append(") select ");
				sql.append(taskConfig.getSelectColumnsStr());
				logger.info("读取XML字段值==>【" + taskConfig.getSelectColumnsStr() + "】");
				// sql.append("*");
				// if (tableName.equals("gyl_wms_city_bill_im_import_src")
				// || tableName.equals("gyl_wms_city_bill_im_import_dtl_src")
				// || tableName.equals("blf1_bl_wms_check_dtl_src") ||
				// tableName.equals("blf1_bl_wms_check_src")
				// || tableName.equals("blf1_bl_wms_deliver_dtl_src")) {
				if (specialTables.contains(tableName.trim())) {
					sql.append(" from dc_ods.");// 默认库创建
					sql.append(tableName.replaceAll("_src", "_ods"));
				} else {
					sql.append(" from dc_src.");// 默认库创建
					sql.append(tableName);
				}
				if (isUpdate.equals("0")) {// 如果增量拼接日期,全量直接查询加载数据
					sql.append(" where ");
					sql.append(HiveDefinePartNameEnum.PARTITION_DATE_NAME.getValue());
					sql.append(" in (");
					StringBuilder builder = new StringBuilder();
					for (String dayPath : dayList) {
						builder.append(dayPath + ",");
					}
					sql.append(dayList.toString().replaceAll("\\]", "").replaceAll("\\[", ""));
					sql.append(")");
					// if (tableName.equals("gyl_wms_city_bill_im_import_src")
					// ||
					// tableName.equals("gyl_wms_city_bill_im_import_dtl_src"))
					// {
					if (specialTables.contains(tableName.trim())) {
						sql.append("and ");
						sql.append("edittm > '");
						sql.append(startTimeStr);
						sql.append("'");
						sql.append(" and edittm < '");
						sql.append(endTimeStr);
						sql.append("'");
					}
				}
				message = String.format("执行导入数据到【gtp.%s】,执行sql:【%s】", tableName, sql.toString());
				logger.info(message);
				logger.info("-----------------------------导入中-----------------------------");
				boolean isImport = HiveUtils.execUpdate(taskConfig.getTargetDbEntity(), taskConfig, "", sql.toString(),
						20);
				if (isImport) {
					message = String.format("导入【gtp.%s】数据成功", tableName, sql.toString());
					logger.info(message);
					jobBizStatusEnum = JobBizStatusEnum.RUNNING;
					addTaskLog(jobId, tableName, groupName, jobBizStatusEnum, runMsg);
					SendMsg2AMQ.updateStatusAndSend(jobId, jobBizStatusEnum, jmsClusterMgr);
					logger.info("-------------------------过滤路径-------------------------");
					String listStatusPath = bdpHivePath.replaceAll(bdpFsPath, "") + tableName;
					logger.info("-------------------------" + listStatusPath + "-------------------------");
					Map<String, String> newDistcpMap;
					conf.addResource(new Path(PropertyFile.getHdfsInfoPath()[0]));
					conf.addResource(new Path(PropertyFile.getHdfsInfoPath()[1]));
					try {
						fs = FileSystem.newInstance(conf);
					} catch (IOException e1) {
						logger.error("FileSystem initialize failed");
					}
					if (tableName.equals("gyl_wms_city_bill_im_import_src")
							|| tableName.equals("gyl_wms_city_bill_im_import_dtl_src")) {
						newDistcpMap = filterSourcePaths(listStatusPath, specialMap, isUpdate);
					} else {
						newDistcpMap = filterSourcePaths(listStatusPath, distcpMap, isUpdate);
					}
					if (!newDistcpMap.isEmpty()) {
						logger.info("【↓↓↓↓↓↓过滤结果↓↓↓↓↓↓】");
						for (Map.Entry<String, String> res : newDistcpMap.entrySet()) {
							String key = res.getKey();
							String value = res.getValue();
							logger.info(String.format("【%s===>%s】", key, value));
						}
					} else {
						runMsg = String.format("当前拷贝选项暂无数据，请校验所选时间段【%s】===>【%s】,反馈ETL该时间段否存在数据", startTimeStr,
								endTimeStr);
						logger.error(runMsg, new Exception());
						// 任务状态日志入库
						jobBizStatusEnum = JobBizStatusEnum.FINISHED;
						addTaskLog(jobId, tableName, groupName, jobBizStatusEnum, runMsg);
						SendMsg2AMQ.updateStatusAndSend(jobId, jobBizStatusEnum, jmsClusterMgr);
						RinseStatusAndLogCache.removeTaskByJobId(jobId);
						return;
					}
					executeDistcp(jobId, groupName, conf, tableName, isUpdate, newDistcpMap, taskConfig);
				}
			}
		} catch (Exception e) {
			String msg = String.format("hive execute操作失败【%s】sql:【%s】", tableName, sql.toString(), sql.toString());
			runMsg = "中断";
			jobBizStatusEnum = JobBizStatusEnum.INTERRUPTED;
			addTaskLog(jobId, tableName, groupName, jobBizStatusEnum, runMsg);
			RinseStatusAndLogCache.removeTaskByJobId(jobId);
			SendMsg2AMQ.updateStatusAndSend(jobId, jobBizStatusEnum, jmsClusterMgr);
			logger.error(msg, e);
			return;
		}
	}

	public static void main(String[] args) throws Exception {
		String start = "2018-05-20 00:00:00";
		String end = "2018-05-20 00:00:00";
		String dateFormat = DateFormatStrEnum.JAVA_YYYY_MM_DD.getValue();
		SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
		Date syncBeginTime = sdf.parse(start);
		Date syncEndTime = sdf.parse(end);
		if (syncBeginTime.after(syncEndTime)) {
			System.out.println("fail");
		}
	}
}
