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
import java.util.Set;
import java.util.TimeZone;

import javax.annotation.Resource;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jmx.export.annotation.ManagedResource;
import org.springframework.stereotype.Service;

import org.apache.commons.lang.StringUtils;
import cn.wonhigh.dc.client.common.constans.CommonEnumCollection;
import cn.wonhigh.dc.client.common.constans.CommonEnumCollection.DbTypeCollecEnum;
import cn.wonhigh.dc.client.common.constans.CommonEnumCollection.HiveDefinePartNameEnum;
import cn.wonhigh.dc.client.common.constans.CommonEnumCollection.ParamNameEnum;
import cn.wonhigh.dc.client.common.model.ClientTaskStatusLog;
import cn.wonhigh.dc.client.common.model.TaskDatabaseConfig;
import cn.wonhigh.dc.client.common.model.TaskPropertiesConfig;
import cn.wonhigh.dc.client.common.util.ExceptionHandleUtils;
import cn.wonhigh.dc.client.common.util.ParseXMLFileUtil;
import cn.wonhigh.dc.client.manager.cache.RinseStatusAndLogCache;
import cn.wonhigh.dc.client.manager.jms.SendMsg2AMQ;
import cn.wonhigh.dc.client.service.ClientTaskStatusLogService;
import cn.wonhigh.dc.client.service.jms.JmsClusterMgr;
import cn.wonhigh.dc.scheduler.common.api.dto.DataQACheckDto;
import cn.wonhigh.dc.scheduler.common.api.service.RemoteRepoJobServiceExtWithParams;

import com.yougou.logistics.base.common.enums.JobBizStatusEnum;
import com.yougou.logistics.base.common.vo.scheduler.RemoteJobInvokeParamsDto;

/**
 * 同步数据质量核查
 * @author zhang.rq
 * @since 2016-07-11
 */
@Service
@ManagedResource(objectName = DataSynVerificationTaskImpl.MBEAN_NAME, description = "Hive同步数据质量核查")
public class DataSynVerificationTaskImpl implements
		RemoteRepoJobServiceExtWithParams<DataQACheckDto> {
	
	public static final String MBEAN_NAME = "dc:client=DataSynVerificationTaskImpl";
	
	private String dateFormat = "yyyy-MM-dd HH:mm:ss";
	
	private final String START_TAB="ods_t";
	
	private static final Logger logger = Logger
			.getLogger(DataSynVerificationTaskImpl.class);

	@Resource
	private ClientTaskStatusLogService clientTaskStatusLogService;

	@Autowired
	private JmsClusterMgr jmsClusterMgr;

	@Value("${dc.data.synVerification.date}")
	private String synVerificationDate = "60";

	@Value("${dc.data.synVerification.trace}")
	private String synTraceDate = "1";

	/**
	 * 统计阀值  标准差的倍数，是可配置的
	 */
	@Value("${dc.data.synVerification.standard}")
	private String standardDeviation = "2";
	/**
	 * 记录当前算出来的数据质量，
	 * 并且覆盖现有的相同的数据
	 */
	private final String dataSyncTableName=CommonEnumCollection.HiveDefinePartNameEnum.DB_NAME_STAGING.getValue()
			+ "data_sync_check_results" +  CommonEnumCollection.HiveDefinePartNameEnum.CK_TABLE_NAME_SUBFIX.getValue();
	
	// 数据库和表名间的分隔符
	public final String SPLITONE = ".";
	public final String SPLITTWO = ";";	

	/**
	 * 求给定双精度数组中值的标准差
	 * 
	 * @param inputData
	 *            输入数据数组
	 * @return 运算结果  [平均值，阀值]
	 */
	public double[] getStandardDiviation(Integer[] inputData, int size) {
		int len = inputData.length;

		double sum = 0;
		for (int i = 0; i < len; i++) {
			sum = sum + inputData[i];
		}

		double average = sum / size;
		double result = 0.0;// / len-1;
		for (int i = 0; i < len; i++) {
			result = result + Math.pow((inputData[i] - average), 2);
		}

		// 如果hive和业务库都没有数据，那么这里设置为零占位
		int noDateSize = size - len;
		for (int i = 0; i < noDateSize; i++) {
			result = result + Math.pow((0 - average), 2);
		}

		result = result / size;

		double diffResult = Math.sqrt(Math.abs(result));
		
		double[] ret=new double[2];
		ret[0]=average;
		ret[1]=diffResult;
		return ret;

	}

	/**
	 * @param groupName
	 * @param triggerName
	 * @param mapVo
	 * @throws Exception
	 */
	private Map<String, DataQACheckDto> querySourceTable( String groupName, String triggerName,
						 TaskPropertiesConfig taskConfig, Map<String, DataQACheckDto> mapVo) throws  SQLException{

		Connection conSql = null;
		Statement state = null;
		ResultSet voRs = null;

		StringBuffer sqlStrBuff = new StringBuffer();
		TaskDatabaseConfig sourceDbEntity = taskConfig.getSourceDbEntity();
		// 数据库相关信息
		Integer dbType = sourceDbEntity.getDbType();
		String dbUrl = sourceDbEntity.getConnectionUrl();
		String dbUserName = sourceDbEntity.getUserName();
		String dbPwd = sourceDbEntity.getPassword();
		// 表相关信息
		String syncTimeColumn = taskConfig.getSyncTimeColumnStr();
		String sourceTable = taskConfig.getSourceTable();
		String[] sourceTabArrays=sourceTable.split(",");
		
		//_ck.xml里面添加属性 -----<filterConditions>
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
		String tmpVerificationDate = String.valueOf(Integer.valueOf(synVerificationDate) + Integer.valueOf(synTraceDate));
		logger.info(String.format("【tmpVerificationDate：%s = synVerificationDate：%s + synTraceDate：%s】"
				,tmpVerificationDate,tmpVerificationDate,synTraceDate));
		
		int synDate = Integer.parseInt(synVerificationDate);
		DateFormat format=new SimpleDateFormat("yyyy-MM-dd");
		format.setTimeZone(TimeZone.getTimeZone("GMT+8"));		
		synDate = synDate + Integer.valueOf(synTraceDate);
		Date date = new Date();
		// 统计 开始日期
		Calendar beginDate = Calendar.getInstance();
		beginDate.setTime(date);
		beginDate.set(Calendar.DATE, beginDate.get(Calendar.DATE) - synDate);
	    String beginTime=format.format(beginDate.getTime());
	    beginTime=beginTime+" 00:00:00";
	    
		// 统计 结束日期
		Calendar endDate = Calendar.getInstance();
		Date date2 = new Date();
		endDate.setTime(date2);
		endDate.set(Calendar.DATE, endDate.get(Calendar.DATE) - Integer.valueOf(synTraceDate));
		String endTime=format.format(endDate.getTime());
		endTime=endTime+" 23:59:59";
		

		if("".equals(filterConditions.trim()))filterConditions=" AND 1=1";

		if (dbType == DbTypeCollecEnum.ORACLE.getValue()) {
			sqlStrBuff.append( "select TO_CHAR(");
			sqlStrBuff.append( syncTimeColumn);
			sqlStrBuff.append(",'YYYY-MM-DD') ywDate,COUNT(1) couNum from ");
			sqlStrBuff.append(sourceTabArrays[0]+" "+START_TAB);
			sqlStrBuff.append( " WHERE 1=1 " +filterConditions+" AND ");
			sqlStrBuff.append( syncTimeColumn);
			sqlStrBuff.append(">=( sysdate -");
			sqlStrBuff.append(tmpVerificationDate);
			sqlStrBuff.append(")");
			sqlStrBuff.append(" AND ");
			sqlStrBuff.append( syncTimeColumn);
			sqlStrBuff.append("<=( sysdate -");
			sqlStrBuff.append(synTraceDate);
			sqlStrBuff.append(")");
			sqlStrBuff.append("GROUP BY TO_CHAR(");
			sqlStrBuff.append( syncTimeColumn);
			sqlStrBuff.append(",'YYYY-MM-DD')") ;
			
		} else if (dbType == DbTypeCollecEnum.MYSQL.getValue()) {
			sqlStrBuff.append("select DATE_FORMAT( ");
			sqlStrBuff.append( syncTimeColumn);
			sqlStrBuff.append(",'%Y-%m-%d') as ywDate,count(1) as couNum from ");
			sqlStrBuff.append(sourceTabArrays[0]+" AS "+START_TAB);
			sqlStrBuff.append(" where 1=1 "+filterConditions+" AND ");
			sqlStrBuff.append("'"+beginTime+"' <= ");
			sqlStrBuff.append( syncTimeColumn);

			sqlStrBuff.append(" AND ");
			sqlStrBuff.append("'"+endTime+"' >= ");
			sqlStrBuff.append( syncTimeColumn);
			sqlStrBuff.append( " GROUP BY DATE_FORMAT(");
			sqlStrBuff.append( syncTimeColumn);
			sqlStrBuff.append( ",'%Y-%m-%d')");
			
			if(sourceTabArrays.length>1){//历史表查询
				sqlStrBuff.append( " Union ALL ");
				sqlStrBuff.append("select DATE_FORMAT( ");
				sqlStrBuff.append( syncTimeColumn);
				sqlStrBuff.append(",'%Y-%m-%d') as ywDate,count(1) as couNum  from ");
				sqlStrBuff.append(sourceTabArrays[1]+" AS "+START_TAB);
				sqlStrBuff.append(" where 1=1 "+filterConditions+" AND ");
				sqlStrBuff.append("'"+beginTime+"' <= ");
				sqlStrBuff.append( syncTimeColumn);

				sqlStrBuff.append(" AND ");
				sqlStrBuff.append("'"+endTime+"' >= ");
				sqlStrBuff.append( syncTimeColumn);


				sqlStrBuff.append( " GROUP BY DATE_FORMAT(");
				sqlStrBuff.append( syncTimeColumn);
				sqlStrBuff.append( ",'%Y-%m-%d')");
			}
		}

		try {
			String sqlStr = sqlStrBuff.toString();
			if(sourceTabArrays.length>1){
				//统计历史表和临时表统计求和
				sqlStr="select ywDate,sum(couNum) from ("+sqlStr;//开头加上求和
				sqlStr=sqlStr+") temp  group by ywDate";
			}
			
			conSql = getConn(dbUrl, dbUserName, dbPwd, dbType);
			state = conSql.createStatement();
			voRs = state.executeQuery(sqlStr);
			logger.info(String.format("查询业务库数据质量监控完成！ 【%s】",sqlStr));

			SimpleDateFormat insertfm = new SimpleDateFormat("yyyy-MM-dd");
			insertfm.setTimeZone(TimeZone.getTimeZone("GMT+8"));


			while (voRs != null && voRs.next()) {
				String key = voRs.getString(1).trim();
				DataQACheckDto vo = new DataQACheckDto();
				vo.setSyncDate(insertfm.parse(key));
				vo.setBizCount(voRs.getInt(2));
				vo.setTriggerGroup(groupName);
				vo.setTriggerName(triggerName);
				vo.setHiveCount(0);//设置默认值

				mapVo.put(key, vo);
			}

		} catch (SQLException e) {
			throw new RuntimeException("同步数据质量核查DataSynVerificationTaskImpl：searchVoList()  table=["
					+ sourceTable + "]数据查询失败！！     "+e);
		} catch (ParseException e) {
			throw new RuntimeException("同步数据质量核查DataSynVerificationTaskImpl：searchVoList()  String 转换成 Date报错了    "+e);
		}finally {
			try {
				if (voRs != null)
					voRs.close();
				if (state != null)
					state.close();
				if (conSql != null)
					conSql.close();
			} catch (SQLException e) {
				throw new RuntimeException("同步数据质量核查DataSynVerificationTaskImpl：searchVoList()  table=["
						+ sourceTable + "]连接关闭失败！！！     " + e);
			}
		}
		logger.info(String.format("返回给调度系统的个数 【%s】",mapVo.size()));
		return mapVo;

	}
	/**
	 * @param groupName
	 * @param triggerName
	 * @param mapVo
	 * @throws Exception
	 */
	private Map<String, DataQACheckDto> queryHiveTable( String groupName, String triggerName,
				TaskPropertiesConfig taskConfig, Map<String, DataQACheckDto> mapVo) throws  SQLException{

		Connection conSql = null;
		Statement state = null;
		ResultSet voRs = null;

		StringBuffer sqlStrBuff = new StringBuffer();
		TaskDatabaseConfig taregetDbEntity = taskConfig.getTargetDbEntity();
		// 数据库相关信息
		Integer dbType = taregetDbEntity.getDbType();
		String dbUrl = taregetDbEntity.getConnectionUrl();
		String dbUserName = taregetDbEntity.getUserName();
		String dbPwd = taregetDbEntity.getPassword();
		// 表相关信息
//		String syncTimeColumn = taskConfig.getSyncTimeColumnStr();
		String targetTable = taskConfig.getTargetTable();

		int synDate = Integer.parseInt(synVerificationDate);

		logger.info(String.format("数据质量监控统计天数【%d】，【groupName：%s】【triggerName：%s】)", Integer.parseInt(synVerificationDate)
				,groupName, triggerName));
		String partiName=CommonEnumCollection.HiveDefinePartNameEnum.PARTITION_DATE_NAME.getValue();
		DateFormat format=new SimpleDateFormat("yyyyMMdd");
		format.setTimeZone(TimeZone.getTimeZone("GMT+8"));
		DateFormat saveForm=new SimpleDateFormat("yyyy-MM-dd");
		saveForm.setTimeZone(TimeZone.getTimeZone("GMT+8"));

		synDate = synDate + Integer.valueOf(synTraceDate);
		Date date = new Date();
		// 统计 开始日期
		Calendar beginDate = Calendar.getInstance();
		beginDate.setTime(date);
		beginDate.set(Calendar.DATE, beginDate.get(Calendar.DATE) - synDate);
	    String beginTime=format.format(beginDate.getTime());
		// 统计 结束日期
		Calendar endDate = Calendar.getInstance();
		Date date2 = new Date();
		endDate.setTime(date2);
		endDate.set(Calendar.DATE, endDate.get(Calendar.DATE) - Integer.valueOf(synTraceDate));
		String endTime=format.format(endDate.getTime());


		sqlStrBuff.append("select t."+partiName+",count(1) from ");
		sqlStrBuff.append( HiveDefinePartNameEnum.DB_NAME_ODS.getValue());
		sqlStrBuff.append(targetTable);
		sqlStrBuff.append(" t where t."+partiName+" >= ");
		sqlStrBuff.append(beginTime);
		sqlStrBuff.append(" AND t.");
		sqlStrBuff.append(partiName);
		sqlStrBuff.append(" <= ");
		sqlStrBuff.append(endTime);
		sqlStrBuff.append(" group by t."+partiName);
		try {
			String sqlStr = sqlStrBuff.toString();
			conSql = getConn(dbUrl, dbUserName, dbPwd, dbType);
			state = conSql.createStatement();
			voRs = state.executeQuery(sqlStr);
			logger.info(String.format("查询hive库  【%s】",sqlStr));

			

			// hive 条数读取
			targetTable = targetTable.substring(groupName.length() + 1,
					targetTable.length());
			
			while (voRs != null && voRs.next()) {
				String key = voRs.getInt(1)+"";
				Date keySynDate=format.parse(key);
				key=saveForm.format(keySynDate);
				
				int hiveTableCount = voRs.getInt(2);

				if (mapVo.get(key) == null) {
					DataQACheckDto vo = new DataQACheckDto();
					vo.setSyncDate(keySynDate);
					vo.setHiveCount(voRs.getInt(2));
					vo.setBizCount(0);
					vo.setTriggerGroup(groupName);
					vo.setTriggerName(triggerName);

					mapVo.put(key, vo);
				} else {
					mapVo.get(key).setHiveCount(hiveTableCount);
				}
			}

		} catch (SQLException e) {
			throw new RuntimeException("同步数据质量核查DataSynVerificationTaskImpl：searchVoList()  table=["
					+ targetTable + "]数据查询失败！！     "+e);
		} catch (ParseException e) {
			throw new RuntimeException("同步数据质量核查DataSynVerificationTaskImpl：searchVoList()  String 转换成 Date报错了    "+e);
		}finally {
			try {
				if (voRs != null)
					voRs.close();
				if (state != null)
					state.close();
				if (conSql != null)
					conSql.close();
			} catch (SQLException e) {
				throw new RuntimeException("同步数据质量核查DataSynVerificationTaskImpl：searchVoList()  table=["
						+ targetTable + "]连接关闭失败！！！     " + e);
			}
		}
		return mapVo;

	}
	/**
	 * hive 插入临时缓存数据
	 * @param taskConfig
	 * @param mapVo
	 * @throws SQLException
	 */
	private void insertHiveTempTable(TaskPropertiesConfig taskConfig ,Map<String, DataQACheckDto> mapVo) throws SQLException{
		Connection conSql = null;
		Statement st=null;
		ResultSet rt=null;

		TaskDatabaseConfig targetDbEntity = taskConfig.getTargetDbEntity();
		Integer dbType = targetDbEntity.getDbType();
		String dbUrl = targetDbEntity.getConnectionUrl();
		String dbUserName = targetDbEntity.getUserName();
		String dbPwd = targetDbEntity.getPassword();

		if( mapVo == null || mapVo.size() < 1 ) {
			logger.info("********更新hive 临时表过程中，发现没有要更新的内容！");
			return;
		}

		//拼接 insert hive的SQL语句 和 delete查询条件
		String conditionSql="";
		

		SimpleDateFormat sd = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		sd.setTimeZone(TimeZone.getTimeZone("GMT+8"));
		String createDate = sd.format(new Date());

		SimpleDateFormat insertfm = new SimpleDateFormat("yyyy-MM-dd");
		insertfm.setTimeZone(TimeZone.getTimeZone("GMT+8"));

		StringBuffer InsertSqlBuff = new StringBuffer();
		InsertSqlBuff.append("insert into table ");
		InsertSqlBuff.append(dataSyncTableName);
		InsertSqlBuff.append(" values");

		for (String key : mapVo.keySet()) {
			DataQACheckDto vo=mapVo.get(key);
			conditionSql+="'"+vo.getTriggerGroup()+"-"+vo.getTriggerName()+" "+key+"',";
			String synDate = insertfm.format(vo.getSyncDate());

			InsertSqlBuff.append("('"+vo.getTriggerGroup()+ "','"
					+	vo.getTriggerName() +"','"+synDate+"'" + ","
					+	vo.getBizCount() +"," + vo.getHiveCount() + ","
					+	vo.getDataDif() +",'" + vo.getThreshold()+"',"
					+	vo.getIsSyncDif()+",'"+createDate+"'),"
					+	System.getProperty("line.separator"));
		}
		
		
		//1.删除重复数据
		//-------------这里删除部分数据，不是删除全部是数据哦
		if(conditionSql.endsWith(","))
			conditionSql=conditionSql.substring(0,conditionSql.length()-1);
		String deleteSql="delete from "+dataSyncTableName+" where concat(group_name,'-',task_name,' ',syn_date) in ("+conditionSql+")";
		
		try{
			conSql = getConn(dbUrl, dbUserName, dbPwd, dbType);
			try {
				st = conSql.createStatement();
		        st.executeUpdate(deleteSql);
			} catch (SQLException e) {
				throw new RuntimeException("DataSynVerificationTaskImpl  insertHiveTempTable:删除数据hive临时表时,报错了"+e);
			}finally{
				try {
					if(rt!=null)rt.close();
					if(st!=null)st.close();
				} catch (SQLException e) {
					throw new RuntimeException("DataSynVerificationTaskImpl  insertHiveTempTable:关闭Statement 或者ResultSet ,报错了"+e);
				}
			}
			
	
			// 2 要插入临时表
			try {
				st = conSql.createStatement();
				String insertStr=InsertSqlBuff.toString();
//				logger.info(String.format("数据校验模块 dbUrl【%s】, dbUserName【%s】, dbPwd【%s】, dbType【%s】，sql【%s】",
//						dbUrl, dbUserName, dbPwd, dbType,insertStr));
	
				insertStr=insertStr.trim();
				insertStr=insertStr.substring(0, insertStr.length()-1);
	
		        st.executeUpdate(insertStr);
				String message = String.format("插入【%s】成功！",	dataSyncTableName	);
		        logger.info(message);
			} catch (SQLException e) {
				String message = String.format("插入【%s】失败！",	dataSyncTableName	);
				logger.info(message);
				throw e;
			}finally{
				try {
					if(rt!=null)rt.close();
					if(st!=null)st.close();
				} catch (SQLException e) {
					String message = String.format("关闭hive连接信息失败！");
					logger.info(message);
					throw e;
				}
			}
		}finally{
			try {
				if(conSql!=null)conSql.close();
			} catch (SQLException e) {
				String message = String.format("关闭hive连接信息失败！");
				logger.info(message);
				throw e;
			}
		}
	}
	/**
	 * 设值给 [数据差异量,阀值,是否同步差异]
	 */
	private Map<String, DataQACheckDto> setIsSynNumber(
			Map<String, DataQACheckDto> voList) {
		Integer[] diffMap = new Integer[voList.size()];
		Set<String> s = voList.keySet();// 获取KEY集合

		int i = 0;
		System.out.println("");
		for (String str : s) {
			DataQACheckDto vo = voList.get(str);
			int diffCount = vo.getHiveCount()-vo.getBizCount();// 数据差异量
			System.out.print(diffCount+",");
			voList.get(str).setDataDif(diffCount);
			voList.get(str).setIsSyncDif(diffCount==0?0:1);
			diffMap[i] = diffCount;
			i++;
		}


		if(synVerificationDate.isEmpty() || synVerificationDate.length()<= 1){
			synVerificationDate = "60";
		}

		Integer synVerDate =  Integer.valueOf(synVerificationDate) ;

		// 计算阀值
		double[] sd = getStandardDiviation(diffMap, synVerDate);
		
		if(StringUtils.isBlank(standardDeviation))standardDeviation="2";
		
		double dStandardDeviation=Double.parseDouble(standardDeviation);
		
		//阀值区间 sd[0]-2.00*sd[1];
		double threshold_s=sd[0]-dStandardDeviation*sd[1];
		double threshold_m=sd[0]+dStandardDeviation*sd[1];
		
		for (String str : s) {
			//标准差sd[1]
			
			DataQACheckDto dto=voList.get(str);
			
			//同步数据是否正常
			if(dto.getDataDif()>=threshold_s&&dto.getDataDif()<=threshold_m){
				//正常
				voList.get(str).setIsSyncDif(0);
			}else{
				//不正常
				voList.get(str).setIsSyncDif(1);
			}
			
			//阀值区间
			String areaSd=String.format("[%.2f , %.2f]",threshold_s,threshold_m);
			voList.get(str).setThreshold(areaSd);
//			logger.info("标准差："+sd[1]+"          平均值："+sd[0]+"         阀值："+areaSd+"        是否正常："+voList.get(str).getIsSyncDif()+"     差异值："+dto.getDataDif());
		}
		
		return voList;
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
			String passWord, Integer dbType)throws RuntimeException {

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
//			System.out.println("passWord: " + passWord);

			conn = DriverManager.getConnection(connectionUrl, userName,	passWord);
		} catch (SQLException e) {
			throw new RuntimeException("数据源连接失败！！！ "+connectionUrl+"   " + e);
		}
		return conn;
	}

	@Override
	public List<DataQACheckDto> executeCkJobsWithParams(String jobId,
			String triggerName, String groupName,
			RemoteJobInvokeParamsDto remoteJobInvokeParamsDto) {
		JobBizStatusEnum jobBizStatusEnum = null;
		List<DataQACheckDto> voList = new ArrayList<DataQACheckDto>();

		// 如果重复调用，则忽略本次请求
		JobBizStatusEnum jobStauts = RinseStatusAndLogCache
				.getTaskStatusByJobId(jobId);
		if (jobStauts != null&& !jobStauts.name().equals(JobBizStatusEnum.INTERRUPTED.name())) {
			logger.info(String.format("【jobId为：%s】的任务被重复调用", jobId));
			return voList;
		}

		//记录开始时间和结束时间
		Date syncBeginTime = null;
		Date syncEndTime = null;
		String startTimeStr = null;
		String endTimeStr = null;
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
				} catch (ParseException e) {
					logger.error(e);
					return voList;
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

			logger.info("--------获取任务配置信息------------");
			// 1、获取任务配置信息
			TaskPropertiesConfig taskConfig = ParseXMLFileUtil.getTaskConfig(groupName, triggerName);
			if (taskConfig == null) {
				logger.error(String.format("获取缓存中的xml失败：组名【%s】调度名【%s】",groupName, triggerName));
				SendMsg2AMQ.updateStatusAndSend(jobId,JobBizStatusEnum.INTERRUPTED, jmsClusterMgr);
				RinseStatusAndLogCache.removeTaskByJobId(jobId);
				return voList;
			}

			logger.info(String.format("获取任务配置信息成功！开始时间【%s】 结束时间：【%s】 组名【%s】 调度名【%s】",
					syncBeginTime, syncEndTime, groupName, triggerName));
			// 2、更新实例状态--初始化
			jobBizStatusEnum = JobBizStatusEnum.RUNNING;
			String message = "运行中";
			addTaskLog(jobId, triggerName, groupName, jobBizStatusEnum,	message, syncBeginTime, syncEndTime);
			SendMsg2AMQ.updateStatusAndSend(jobId, jobBizStatusEnum,jmsClusterMgr);

			Map<String, DataQACheckDto> mapVo = new HashMap<String, DataQACheckDto>();
			logger.info(String.format("【1】 开始查询业务表数据 ！组名【%s】调度名【%s】", groupName,triggerName));
			mapVo = querySourceTable( groupName, triggerName,taskConfig, mapVo);
			logger.info(String.format("【2】 开始查询hive 表数据 ！组名【%s】调度名【%s】", groupName,triggerName));
			mapVo = queryHiveTable( groupName, triggerName, taskConfig,mapVo);
			logger.info(String.format("【3】 开始计算标准差 ！组名【%s】调度名【%s】 ", groupName,triggerName));
			mapVo = setIsSynNumber(mapVo);
			logger.info(String.format("【4】 开始更新将标准差到hive数据库 ！组名【%s】调度名【%s】 ", groupName,	triggerName));
			insertHiveTempTable(taskConfig, mapVo);

			for (String key : mapVo.keySet()) {
				voList.add(mapVo.get(key));
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
			return voList;
		}
		
		jobBizStatusEnum = JobBizStatusEnum.FINISHED;
		String messageLocalTh = "执行同步数据质量核查查询成功:"
				+ String.format("【groupName=%s】【schedulerName=%s】执行成功！", groupName, triggerName);
		addTaskLog(jobId, triggerName, groupName, jobBizStatusEnum, messageLocalTh, syncBeginTime, syncEndTime);
		// 成功后，需要传mq并删除缓存
		SendMsg2AMQ.updateStatusAndSend(jobId, jobBizStatusEnum, jmsClusterMgr);
		RinseStatusAndLogCache.removeTaskByJobId(jobId);
		logger.info(String.format("完成调度任务：%s ===>结束时间：%s！组名【%s】调度名【%s】",
				syncBeginTime, syncEndTime, groupName, triggerName));
		logger.info(String.format("【5】 将数据校验结果返回调度！组名【%s】调度名【%s】", groupName,triggerName));
		return voList;
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
	public DataQACheckDto executeCkJobWithParams(String arg0,
			String arg1, String arg2, RemoteJobInvokeParamsDto arg3) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void executeJobWithParams(String arg0, String arg1, String arg2,
			RemoteJobInvokeParamsDto arg3) {
		// TODO Auto-generated method stub

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
		executeCkJobsWithParams("asdfegdfsa","pro_product_ck","ck_test",null);
	}
}
