package cn.wonhigh.dc.client.common.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import cn.wonhigh.dc.client.common.constans.CommonEnumCollection;
import cn.wonhigh.dc.client.common.constans.CommonEnumCollection.DbTypeCollecEnum;
import cn.wonhigh.dc.client.common.constans.MessageConstant;
import cn.wonhigh.dc.client.common.model.Column;
import cn.wonhigh.dc.client.common.model.TableRelation;
import cn.wonhigh.dc.client.common.model.TaskDatabaseConfig;
import cn.wonhigh.dc.client.common.model.TaskPropertiesConfig;
import cn.wonhigh.dc.client.common.util.excel.ExcelUtil;
import cn.wonhigh.dc.client.common.util.excel.model.SchedulerTriggers;
import cn.wonhigh.dc.client.common.util.excel.model.SchedulerTriggersKey;

import com.yougou.logistics.base.common.exception.ManagerException;
/**
 * 自动生成xml sql.txt excel
 * -i  新增表，增量或者全量
 * -u  新增现有表字段
 * 格式：必须如下：
 * orcl-employee_info-temp_update-0-u
 * @author user
 * 
 * 生成文件必备条件：必须要有物理删除轨迹表，且数据源id是999
 *
 */
public class CreateNewTableConfUtils {
	/**
	 * @param
	 * 使用时请将CreatePreImportXmlUtils预生成的本次所有新加表的xml(不要放入旧表的)
	 * 放入sqoop配置文件目录（先备份此目录文件），
	 * 同时在本机配置文件的db-config.xml中加入新表所在的库ID（如果库已经存在，不必新加）
	 * 因有jdbc连接，请在maven中加入jar包依赖，但不要提交。
	 */
	private static Integer dbType = 0;
	
	// 数据库和表名间的分隔符
	public static final String SPLITONE = ".";
	public static final String SPLITTWO = ";";
	private static String historyHiveLoadCDCTable =" seq_no bigint COMMENT '本表自增ID'," + System.lineSeparator()
			+" capture_event_uuid string COMMENT '事件的uuid'," + System.lineSeparator()
			+ " dml_type int COMMENT '数据捕获类型，0-删除，1-插入，2-更新'," + System.lineSeparator()
			+" db_name string COMMENT '库名称(如retail_pos),用于说明该数据来自哪个系统'," + System.lineSeparator()
			+" db_source_name string COMMENT '源库名称(如retail_pos_db2),用于说明该数据来自哪个节点'," + System.lineSeparator()
			+" table_name string COMMENT '表名称'," + System.lineSeparator()
			+" id_column_name string COMMENT '捕获数据主键字段名'," + System.lineSeparator()
			+ " id_column_value string COMMENT '捕获数据主键值'," + System.lineSeparator()
			+" update_timestamp_column_name string COMMENT '捕获数据更新字段名'," + System.lineSeparator()
			+" update_timestamp_column_value timestamp COMMENT '捕获数据更新字段对应的时间'," + System.lineSeparator()
			+" capture_time TIMESTAMP COMMENT '数据抓取时间'," + System.lineSeparator();
	
	/**
	 * 物理轨迹表的插入SQL
	 */
	private static final String physicDelRecord="insert into t_physic_del_record(id,sys_name,table_name,seq_no,update_time) " +
			"values({id},'%s','%s',0,now());";
	/**
	 * 物理轨迹表的查询最大Id的SQL
	 */
	private static final String physicDelSql="select max(id) from t_physic_del_record";
	
	/**
	 * 对应数据源配置文件是数据源ID  999
	 */
	private static final Integer pysicDbSourceId=999;
	
	/**
	 * 物理删除轨迹表最大的id值
	 */
	private static Integer maxId=0;
//			+" src_update_time timestamp COMMENT '装载区入库时间'" + System.lineSeparator()
//			+") COMMENT '总部成本维护' PARTITIONED BY (partition_date int) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS TEXTFILE;" + System.lineSeparator();



//	private static String transcatinHLogPG = " seq_no int8 NOT NULL,"+System.lineSeparator()
//			+" capture_event_uuid text ,"+System.lineSeparator()
//			+" dml_type int2 NOT NULL,"+System.lineSeparator()
//			+" db_name text ,"+System.lineSeparator()
//			+" db_source_name text ,"+System.lineSeparator()
//			+" table_name text ,"+System.lineSeparator()
//			+" id_column_name text ,"+System.lineSeparator()
//			+" id_column_value text ,"+System.lineSeparator()
//			+" update_timestamp_column_name text ,"+System.lineSeparator()
//			+" update_timestamp_column_value timestamp,"+System.lineSeparator()
//			+" capture_time timestamp,"+System.lineSeparator()
//			+" yw_update_time timestamp,"+System.lineSeparator()
//			+" PRIMARY KEY (seq_no)"+System.lineSeparator()
//			+");"+System.lineSeparator();

//	private static String transcatinHLogCommon =" COMMENT ON TABLE usr_dc_ods.retail_gms_transaction_history_log IS '记录删除同步表（用于dc同步删除数据）';"+ System.getProperty("line.separator")
//			+" COMMENT ON COLUMN usr_dc_ods.retail_gms_transaction_history_log.seq_no IS '生产的序列号';"+ System.getProperty("line.separator")
//			+" COMMENT ON COLUMN usr_dc_ods.retail_gms_transaction_history_log.capture_event_uuid IS '事件的uuid';"+ System.getProperty("line.separator")
//			+" COMMENT ON COLUMN usr_dc_ods.retail_gms_transaction_history_log.dml_type IS '0删除，1插入，2更新';"+ System.getProperty("line.separator")
//			+" COMMENT ON COLUMN usr_dc_ods.retail_gms_transaction_history_log.db_name IS '库名称(如retail_pos),用于说明该数据来自哪个系统';"+ System.getProperty("line.separator")
//			+" COMMENT ON COLUMN usr_dc_ods.retail_gms_transaction_history_log.db_source_name IS '源库名称(如retail_pos_db2),用于说明该数据来自哪个节点';"+ System.getProperty("line.separator")
//			+" COMMENT ON COLUMN usr_dc_ods.retail_gms_transaction_history_log.table_name IS '表名称';"+ System.getProperty("line.separator")
//			+" COMMENT ON COLUMN usr_dc_ods.retail_gms_transaction_history_log.id_column_name IS '主键字段名称';"+ System.getProperty("line.separator")
//			+" COMMENT ON COLUMN usr_dc_ods.retail_gms_transaction_history_log.id_column_value IS '被删除记录的主键记录值';"+ System.getProperty("line.separator")
//			+" COMMENT ON COLUMN usr_dc_ods.retail_gms_transaction_history_log.update_timestamp_column_name IS '同步时间戳字段名';"+ System.getProperty("line.separator")
//			+" COMMENT ON COLUMN usr_dc_ods.retail_gms_transaction_history_log.update_timestamp_column_value IS '同步时间戳字段值';"+ System.getProperty("line.separator")
//			+" COMMENT ON COLUMN usr_dc_ods.retail_gms_transaction_history_log.capture_time IS '数据抓取时间';"+ System.getProperty("line.separator")
//			+" COMMENT ON COLUMN usr_dc_ods.retail_gms_transaction_history_log.yw_update_time IS 'dc装载区入库时间';"+ System.getProperty("line.separator");

	// 从业务库到装载区
	private static final String commSchedulerTaskCofSrc = "4505," + "0 0 1 * * ?," + "dc:client=DataLoadingTaskImpl,"
			+ "service:jmx:rmi://172.17.210.119/jndi/rmi://172.17.210.119:6088/eltJob," + "5," + "3600," + "3,"
			+ "0," + "0," + "`?," + "1D";
	// 分发 到cdc表
	private static final String commSchedulerTaskCofSrcSlave = "4505," + "0 0 1 * * ?," + "dc:client=DataLoadingTaskImpl,"
			+ "service:jmx:rmi://172.17.210.119/jndi/rmi://172.17.210.119:6088/eltJob," + "5," + "3600," + "3,"
			+ "0," + "1," + "`?," + "1D";
	// thl
	private static final String commSchedulerTaskCofThl = "4506," + "0 0 1 * * ?," + "dc:client=CDCDataMoveTaskImpl,"
			+ "service:jmx:rmi://172.17.210.119/jndi/rmi://172.17.210.119:6088/eltJob," + "5," + "3600," + "3,"
			+ "0," + "1," + "`?," + "1D";
	// ods
	private static final String commSchedulerTaskCofOds = "4507," + "0 0 1 * * ?," + "dc:client=DataCleaningTaskImpl,"
			+ "service:jmx:rmi://172.17.210.119/jndi/rmi://172.17.210.119:6088/eltJob," + "5," + "3600," + "3,"
			+ "0," + "1," + "`?," + "1D";
	// 导出
	private static final String commSchedulerTaskCofAllExp = "4507," + "0 0 1 * * ?," + "dc:client=DataOutputTaskImpl,"
			+ "service:jmx:rmi://172.17.210.119/jndi/rmi://172.17.210.119:6088/eltJob," + "5," + "3600," + "3,"
			+ "0," + "1," + "`?," + "1D";
	private static final String commSchedulerTaskCofSportExp = "4507," + "0 0 1 * * ?," + "dc:client=DataOutputTaskImpl,"
			+ "service:jmx:rmi://172.17.210.119/jndi/rmi://172.17.210.119:6088/eltJob," + "5," + "3600," + "3,"
			+ "0," + "1," + "`?," + "1D";
	private static final String commSchedulerTaskCofRetailExp = "4507," + "0 0 1 * * ?," + "dc:client=DataOutputTaskImpl,"
			+ "service:jmx:rmi://172.17.210.119/jndi/rmi://172.17.210.119:6088/eltJob," + "5," + "3600," + "3,"
			+ "0," + "1," + "`?," + "1D";
	
	// _ck
	private static final String commSchedulerTaskCofCkSlave = "4505," + "0 0 1 * * ?," + "dc:client=DataSynVerificationTaskImpl,"
			+ "service:jmx:rmi://172.17.210.119/jndi/rmi://172.17.210.119:6088/eltJob," + "5," + "3600," + "3,"
			+ "0," + "0," + "`?," + "1D";

	/**
	 * 重命名追加当前时间
	 */
	public static String appendName=null;
	
	//add by Richy 2016-06-16 start
	/**
	 * 路径：最新所有的表配置xml文件所在路径
	 */
	private static String latestXmlDirPath=null;
	/**
	 * 路径：目标xml路径
	 */
	private static String targetXMLDirPath=null;
	/**
	 * 路径：目标excel路径
	 */
	private static String targetExcelDirPath=null;
	/**
	 * 路径：目标sql路径
	 */
	private static String targetSqlDirPath=null;	
	
	//add by Richy 2016-06-16 end
	
	public static String tableNameSpase = "usr_dc_ods.";
	
	/**
	 * sp.xml和rt.xml
	 * 过滤添加条件 列sharding_flag
	 */
	private static String shardingFlag="sharding_flag";
	
	// 装载区HistoryLog表
//	public static String sHiveHistoryLogTable ="";
//	public static String sPgHistoryLogTable = "";
//	public static String 	transcationHistorytaskSrc = "";
//	public static String 	transcationHistorytaskRetailExport = "";
//	public static String 	transcationHistorytaskSportExport = "";
//
//	public static String 	 depHistoryLogImport ="";
//	public static String 	 depHistoryLog_Sport_Export = "";
//	public static String 	 depHistoryLog_Retail_Export = "";
	public static String 	 g_groupName = "";
	private static final Logger logger = Logger.getLogger(CreateNewTableConfUtils.class);
	
	/**
	 * 创建xml和SQL分为两种------------i:新增表  增量和全量       ======= u:新增列，且新增列位于最后
	 * @param getPreXml 获取预准备多个xml的信息内容
	 * @throws SQLException
	 * @throws IOException
	 * @throws ManagerException 
	 */
	public static void createSQL(Map<String,String> getPreXml) throws SQLException, IOException, ManagerException {
		logger.info("==========>开始生成新表脚本");
		
		//初始化临时文件
		int countNum=Integer.parseInt(getPreXml.get("countNum"));
		int initLySrc=0;//记录生成第几个_ly_src.xml
		String[] tempXmlContent=new String[countNum*7];
		for(int i=0;i<countNum;i++){
			tempXmlContent[i*6+0]=getPreXml.get("sRetaiExport"+i);
			tempXmlContent[i*6+1]=getPreXml.get("sSportExport"+i);
			tempXmlContent[i*6+2]=getPreXml.get("sSrc"+i);
			tempXmlContent[i*6+3]=getPreXml.get("sThl"+i);
			tempXmlContent[i*6+4]=getPreXml.get("sOds"+i);
			tempXmlContent[i*6+5]=getPreXml.get("sAllExport"+i);
			tempXmlContent[i*6+6]=getPreXml.get("sCk"+i);
		}
		ParseXMLFileUtil.initTempTargetXmlFile(tempXmlContent);
		
		//*****************u:新增列，且新增列位于最后*************************//
		
		// begin
		ConcurrentMap<String, TaskPropertiesConfig> taskmap = ParseXMLFileUtil.getTaskList();
		HashSet<String> groupNameSet = new HashSet<String>();
		ArrayList<TableRelation> tableRelationList = new ArrayList<TableRelation>();
		//废弃的，暂时没有用到的
//		String[] tableRelationStrArray = readFile("D:/Richy/other/gyl_table_relation.txt").split(
//				System.getProperty("line.separator"));
//		for (int i = 0; i < tableRelationStrArray.length; i++) {
//			TableRelation trl = new TableRelation();
//			if (StringUtils.isNotBlank(tableRelationStrArray[i])) {
//				String[] nameList = tableRelationStrArray[i].split("@");
//				trl.setParentTableName(nameList[0].trim());
//				trl.setParentRelationColmName(nameList[1].trim());
//				trl.setSlaveTableName(nameList[2].trim());
//				trl.setSlaveRelationColmName(nameList[3].trim());
//				tableRelationList.add(trl);
//			}
//		}
		for (Entry<String, TaskPropertiesConfig> task : taskmap.entrySet()) {
			//if (!task.getValue().getGroupName().contains("_hive") && !task.getValue().getTriggerName().contains("_cln")) {
			if (task.getValue().getTriggerName().contains("src")
					&& !task.getValue().getTriggerName().contains("thl")
					&& !task.getValue().getTriggerName().contains("clnd")) {
				groupNameSet.add(task.getValue().getGroupName());
			}
		}

		// Set taskset=taskmap.entrySet();
		String taskSrc = "";
		String taskThl = "";
		String taskOds = "";
		String taskExp = "";

		String dependence = "";
		String indexSql = "";
		
		//存储物理轨迹表的SQL集合
		Map<String,String> physicDelMap=new HashMap<String,String>();


		for (String groupN : groupNameSet) {
			g_groupName = groupN;
			String sHiveLoadBaseTable = ""; // 装载区基础表
			String sHiveLoadCDCTable = "";  // 装载区基础CDC表
			String sHiveODSTable = "";		// 装载区ODS表
			String sPgTable = "";

			String sPg = "";
			String sYw = "";
			String sMysql = "";


				Connection connMysql = null;
				//数据库连接配置信息
				Map<String,String> conConf=new HashMap<String,String>();
				
				for (Entry<String, TaskPropertiesConfig> task : taskmap.entrySet()) {
					
				if (task.getValue().getTriggerName().contains("_src")
						&&!task.getValue().getGroupName().contains("_thl")
						&& !task.getValue().getTriggerName().contains("_clnd")
						&& task.getValue().getGroupName().equalsIgnoreCase(groupN)) {
//						&& !task.getValue().getTriggerName().contains(MessageConstant.TRANSCATION_HISTORY_LOG)
					
					String selectColumnsStr = "";
					System.out.println("task.getValue().getTriggerName():"+task.getValue().getTriggerName());
					// 获取目标表名
					String groupName = task.getValue().getGroupName(); 
					String targetTable = task.getValue().getTargetTable().replace("_src","");


					// 装载区基础表
					sHiveLoadBaseTable += "DROP TABLE IF EXISTS " + CommonEnumCollection.HiveDefinePartNameEnum.DB_NAME_SRC.getValue()
							+ targetTable + CommonEnumCollection.HiveDefinePartNameEnum.SRC_TABLE_NAME_SUBFIX.getValue() + SPLITTWO + System.getProperty("line.separator");
					sHiveLoadBaseTable += "CREATE TABLE IF NOT EXISTS " +  CommonEnumCollection.HiveDefinePartNameEnum.DB_NAME_SRC.getValue()
							+ targetTable +  CommonEnumCollection.HiveDefinePartNameEnum.SRC_TABLE_NAME_SUBFIX.getValue()+ " (" + System.getProperty("line.separator");

					//判断是不是日志表(不生成clnd和thl)
					if(!getTypeTLog(targetTable)){
						
						if(!targetTable.contains(MessageConstant.VERIFY)){
							
							// 装载区cdc基础表
							sHiveLoadCDCTable += "DROP TABLE IF EXISTS " + CommonEnumCollection.HiveDefinePartNameEnum.DB_NAME_SRC.getValue()
									+ targetTable +CommonEnumCollection.HiveDefinePartNameEnum.THL_TABLE_NAME_SUBFIX.getValue() + SPLITTWO + System.getProperty("line.separator");
							sHiveLoadCDCTable += "CREATE TABLE IF NOT EXISTS " + CommonEnumCollection.HiveDefinePartNameEnum.DB_NAME_SRC.getValue()
									+ targetTable + CommonEnumCollection.HiveDefinePartNameEnum.THL_TABLE_NAME_SUBFIX.getValue()+ " (" + System.getProperty("line.separator");
						}
						// ODS层 基础表
						sHiveODSTable += "DROP TABLE IF EXISTS " + CommonEnumCollection.HiveDefinePartNameEnum.DB_NAME_ODS.getValue()
								+ targetTable + CommonEnumCollection.HiveDefinePartNameEnum.ODS_TABLE_NAME_SUBFIX.getValue() + SPLITTWO + System.getProperty("line.separator");
						sHiveODSTable += "CREATE TABLE IF NOT EXISTS " +  CommonEnumCollection.HiveDefinePartNameEnum.DB_NAME_ODS.getValue()
								+ targetTable + CommonEnumCollection.HiveDefinePartNameEnum.ODS_TABLE_NAME_SUBFIX.getValue()+ " (" + System.getProperty("line.separator");
					}
					
					sPgTable += "DROP TABLE IF EXISTS " + tableNameSpase + groupName.replace("dc_","") +"_"
							+ targetTable  + SPLITTWO + System.getProperty("line.separator");
					sPgTable += "CREATE TABLE IF NOT EXISTS " +  tableNameSpase + groupName.replace("dc_","") +"_"
							+ targetTable + " (" + System.getProperty("line.separator");

					// pg 数据表
					sPg += "DROP TABLE IF EXISTS " + tableNameSpase + targetTable + ";"
							+ System.getProperty("line.separator");
					sPg += "CREATE TABLE IF NOT EXISTS " + tableNameSpase + targetTable + " ("
							+ System.getProperty("line.separator");
					
					String ywTable=targetTable;
					if(targetTable.contains(MessageConstant.VERIFY)){
						ywTable=targetTable.substring(0,ywTable.length()-MessageConstant.VERIFY.length());
					}
					
					// yw 源数据表
					sYw += "DROP TABLE IF EXISTS " + tableNameSpase + ywTable + ";"
							+ System.getProperty("line.separator");
					sYw += "CREATE TABLE IF NOT EXISTS " + tableNameSpase + ywTable + " ("
							+ System.getProperty("line.separator");

						
					connMysql = getConn(task.getValue().getSourceDbEntity().getConnectionUrl(), task
							.getValue().getSourceDbEntity().getUserName(), task.getValue().getSourceDbEntity()
							.getPassword(),connMysql,conConf);
				
					
					
					//为了防止有的数据库一次查不到元数据信息，所以要多尝试几次
					List<Column> columnList = new ArrayList<Column>();
					ResultSet columnRs = null;
					PreparedStatement psColumn = null;

					String mysqlSql = null;
					if (dbType == DbTypeCollecEnum.ORACLE.getValue()) {
						mysqlSql = "SELECT DBA_TAB_COLS.COLUMN_NAME,DBA_TAB_COLS.DATA_TYPE,DBA_col_comments.comments,DBA_TAB_COLS.NULLABLE FROM DBA_TAB_COLS INNER JOIN DBA_col_comments ON DBA_col_comments.TABLE_NAME = DBA_TAB_COLS.TABLE_NAME AND DBA_col_comments.COLUMN_NAME = DBA_TAB_COLS.COLUMN_NAME and DBA_TAB_COLS.TABLE_NAME =? ORDER BY DBA_TAB_COLS.COLUMN_ID";
					}else{
						mysqlSql = "/*#mycat sql=SELECT 1 from "
								+ task.getValue().getSourceTable()
								+ " */SELECT distinct column_name,column_type,column_comment,is_nullable FROM information_schema.COLUMNS WHERE table_name =?";
						
					}
				
					psColumn = connMysql.prepareStatement(mysqlSql);
					String sourceTable = task.getValue().getSourceTable().trim();
					
					if(sourceTable.contains(MessageConstant.VERIFY)){
						sourceTable=sourceTable.substring(0,sourceTable.length()-MessageConstant.VERIFY.length());
					}
					
					if (dbType == DbTypeCollecEnum.ORACLE.getValue()) {
						psColumn.setString(1,sourceTable.toUpperCase());
					}else{
						psColumn.setString(1,sourceTable);
					}

					try {
						columnRs = psColumn.executeQuery();
					} catch (SQLException e) {
						System.out.println("get comluns error: "+e);
					}
					
					if (columnRs == null){
						System.out.println("columnRs is null!");
					}
					
					
					//记录新增的列
					List<Column> addColumn=new ArrayList<Column>();//找出新增的列
					String tableName=task.getValue().getTriggerName().replace("_src","");
					String nameArray=getPreXml.get("importXmlNameArray"+"-"+groupName.toLowerCase()+"-"+tableName.toLowerCase());
					List<String> addColumnName=new ArrayList<String>();//找出新增的列名
					List<String> u_list=new ArrayList<String>();
					if(StringUtils.isNotBlank(nameArray)){
						String[] nameArrays=nameArray.split(",");
						for(String item:nameArrays){
							u_list.add(item.trim());
						}
					}
					
				//	System.out.println("columnRs.next():" + columnRs.next());
					while (columnRs!=null&&columnRs.next()) {
						Column col = new Column();
						String colName=columnRs.getString(1).trim();
						col.setColumnName(colName);
						//System.out.println("column: "+  columnRs.getString(1).trim());
						col.setColumnType(columnRs.getString(2).trim());
						col.setColumnComment(StringUtils.isNotBlank(columnRs.getString(3)) ? columnRs.getString(3)
								.replaceAll("'", " ") : columnRs.getString(3));
						col.setIsNullable(columnRs.getString(4).trim());

						if (!addColumnName.contains(colName.toUpperCase())) {
							addColumnName.add(colName.toUpperCase());
							if(u_list.size()>0&&!u_list.contains(colName)){
								addColumn.add(col);//记录新增的列
							}else{
								columnList.add(col);
							}
							
						}
					}
					
					columnList.addAll(addColumn);//新增排序，新增的列放在最后面
				
					if (columnRs != null) {
						try {
							columnRs.close();
						} catch (SQLException e) {
							e.printStackTrace();
						}
					}
					if (psColumn != null) {
						try {
							psColumn.close();
						} catch (SQLException e) {
							e.printStackTrace();
						}
					}
					//获得表注释
					String tableComment = "";
					String tableCommentSql;
					
					if (dbType == DbTypeCollecEnum.ORACLE.getValue()) {
						tableCommentSql = "select COMMENTS from DBA_TAB_COMMENTS where Table_Name =?";
						tableCommentSql.toUpperCase();
					}else{
						tableCommentSql = "/*#mycat sql=SELECT 1 from " + task.getValue().getSourceTable()
							+ " */select DISTINCT table_comment from information_schema.tables  where  table_name =?";
					}
					PreparedStatement psTableCommentSql = connMysql.prepareStatement(tableCommentSql);
					
					
					psTableCommentSql.setString(1, sourceTable.toUpperCase());
					ResultSet tableCommentSqlRs = null;
					try {
						tableCommentSqlRs = psTableCommentSql.executeQuery();
					} catch (SQLException e) {
						logger.error(e);
					}
					while (tableCommentSqlRs!=null&&tableCommentSqlRs.next()) {
						tableComment = tableCommentSqlRs.getString(1);
					}
					if (tableCommentSqlRs != null) {
						try {
							tableCommentSqlRs.close();
						} catch (SQLException e) {
							e.printStackTrace();
						}
					}
					if (psTableCommentSql != null) {
						try {
							psTableCommentSql.close();
						} catch (SQLException e) {
							e.printStackTrace();
						}
					}

					//获得主键字段
					String primaryKey = "";
					String primaryKeySql;
					if (dbType == DbTypeCollecEnum.ORACLE.getValue()) {
						primaryKeySql = "select col.column_name from DBA_constraints con,  DBA_cons_columns col where con.constraint_name = col.constraint_name and con.constraint_type='P' and col.table_name = ?";
					}else{
						primaryKeySql = "/*#mycat sql=SELECT 1 from "
							+ task.getValue().getSourceTable()
							+ " */SELECT DISTINCT k.column_name FROM information_schema.table_constraints t JOIN information_schema.key_column_usage k USING (constraint_name,table_schema,table_name) WHERE t.constraint_type='PRIMARY KEY' AND t.table_name=?";
					}
					PreparedStatement psPrimaryKeySql = connMysql.prepareStatement(primaryKeySql);
										
					
					if (dbType == DbTypeCollecEnum.ORACLE.getValue()) {
						psPrimaryKeySql.setString(1,sourceTable.toUpperCase());
					}else{
						psPrimaryKeySql.setString(1,sourceTable );
					}
					ResultSet primaryKeySqlRs = null;
					try {
						primaryKeySqlRs = psPrimaryKeySql.executeQuery();
					} catch (SQLException e) {
						logger.error(e);
					}
					List<String> primaryKeys = new ArrayList<String>();
					while (primaryKeySqlRs!=null&&primaryKeySqlRs.next()) {
						if (!primaryKeys.contains(primaryKeySqlRs.getString(1))) {
							primaryKeys.add(primaryKeySqlRs.getString(1));
						}
					}
					for (int i = 0; i < primaryKeys.size(); i++) {
						if (i == primaryKeys.size() - 1) {
							primaryKey += primaryKeys.get(i);
						} else {
							primaryKey += primaryKeys.get(i) + ",";
						}
					}
					if (primaryKeySqlRs != null) {
						try {
							primaryKeySqlRs.close();
						} catch (SQLException e) {
							e.printStackTrace();
						}
					}
					if (psPrimaryKeySql != null) {
						try {
							psPrimaryKeySql.close();
						} catch (SQLException e) {
							e.printStackTrace();
						}
					}
					String pgsqlAlt = "";
					String pgsqlCom = "";
					String ywsqlAlt = "";
					String ywsqlCom = "";
					for (Column com : columnList) {
						
						//新增列位置置后
						//1.u:新增列，src_update_time,ods_update_time,partition_date位置调整
						//2.i:新增列，位置不需要调整
						if(addColumn.size()>0&&addColumn.get(0).equals(com)){
							//---------------------src.txt
							//补充ods_update_time 和 partition_date
							sHiveLoadBaseTable += "  " + "src_update_time timestamp COMMENT '装载区入库时间',"
									+ System.getProperty("line.separator");
							
							//判断是不是日志表(不生成clnd和thl)
							if(!getTypeTLog(targetTable)){
								//---------------------clnd.txt
								//补充yw_update_time,ods_update_time 和 partition_date
								sHiveODSTable += "  " + MessageConstant.ODS_UPDATE_TIME_MSG
										+ System.getProperty("line.separator");
								
								sHiveODSTable += "  " + "partition_date int COMMENT 'ODS 层分区字段',"
										+ System.getProperty("line.separator");
							}
							//---------------------sPg.txt
							pgsqlAlt += "  " + MessageConstant.YW_UPDATE_TIME +" timestamp," + System.getProperty("line.separator");
							pgsqlCom += "COMMENT ON COLUMN " + tableNameSpase + targetTable+"."
									+  MessageConstant.PG_UPDATE_TIME_MSG + System.getProperty("line.separator");
						}
						
						//hive脚本部分
						selectColumnsStr += com.getColumnName() + ",";
						String hivesql = "";
						String columnComment = com.getColumnComment();
						if (columnComment != null && columnComment.contains(";")) {
							columnComment = columnComment.replace(";", " ");
						}
						if (dbType == DbTypeCollecEnum.ORACLE.getValue()) {
							hivesql += "  " + com.getColumnName().toLowerCase() + " " + oracleToHive(com.getColumnType()) + " "
								+ "COMMENT '" + columnComment + "'," + System.getProperty("line.separator");
						}else{
							hivesql += "  " + com.getColumnName().toLowerCase() + " " + mysqlToHive(com.getColumnType()) + " "
								+ "COMMENT '" + columnComment + "'," + System.getProperty("line.separator");
						}

						// 装载区和ods层表的结构是一致的（从业务源查出来）
						sHiveLoadBaseTable  += hivesql;
						
						//判断是不是日志表(不生成clnd和thl)
						if(!getTypeTLog(targetTable)){
							sHiveODSTable  += hivesql;
						}


						//pg脚本部分
						String isNullable = "";
						if (com.getIsNullable().equalsIgnoreCase("no")) {
							isNullable = " NOT NULL";
						}
						
						if (dbType == DbTypeCollecEnum.ORACLE.getValue()) {
							pgsqlAlt += "  " + com.getColumnName().toLowerCase() + " " + oraclToPg(com.getColumnType()) + isNullable
									+ "," + System.getProperty("line.separator");
						}else{
							pgsqlAlt += "  " + com.getColumnName().toLowerCase() + " " + mysqlToPg(com.getColumnType()) + isNullable
									+ "," + System.getProperty("line.separator");
						}

						ywsqlAlt += "  " + com.getColumnName().toLowerCase() + " " + mysqlToYw(com.getColumnType()) + isNullable
								+ "," + System.getProperty("line.separator");

						pgsqlCom += "COMMENT ON COLUMN " + tableNameSpase + targetTable + "."
								+ com.getColumnName() + " IS '" + com.getColumnComment() + "'" + ";"
								+ System.getProperty("line.separator");

						//生成业务数据库表结构
//						ywsqlAlt += "  " + com.getColumnName().toLowerCase() + " " + com.getColumnType() + isNullable
//								+ "," + System.getProperty("line.separator");
						ywsqlCom += "COMMENT ON COLUMN " + tableNameSpase + ywTable + "."
								+ com.getColumnName() + " IS '" + com.getColumnComment() + "'" + ";"
								+ System.getProperty("line.separator");
					}
					
					//判断是不是日志表(不生成clnd和thl)
					if(!getTypeTLog(targetTable)){
						
						// 装载区CDC表
						if(!targetTable.contains(MessageConstant.VERIFY)){
							sHiveLoadCDCTable  += historyHiveLoadCDCTable;
						}
					}

					//新增列位置置后
					//1.u:新增列，src_update_time位置调整
					//2.i:新增列，src_update_time位置不需要调整
					if(addColumn.size()==0){
						//补充ods_update_time 和 partition_date
						sHiveLoadBaseTable += "  " + "src_update_time timestamp COMMENT '装载区入库时间'"
								+ System.getProperty("line.separator");
					}else{//去除末尾的逗号
						sHiveLoadBaseTable=sHiveLoadBaseTable.trim();
						sHiveLoadBaseTable=sHiveLoadBaseTable.endsWith(",")?sHiveLoadBaseTable.substring(0,sHiveLoadBaseTable.length()-1):sHiveLoadBaseTable;
						sHiveLoadBaseTable=sHiveLoadBaseTable+ System.getProperty("line.separator");
					}
					
					
					

					sHiveLoadBaseTable += ") COMMENT '"
							+ tableComment
							+ "' PARTITIONED BY (partition_date int) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\001' STORED AS TEXTFILE;"
							+ System.getProperty("line.separator");
					// 在下一个表后多一个空行
					sHiveLoadBaseTable += System.getProperty("line.separator");
					
					
					//判断是不是日志表(不生成clnd和thl)
					if(!getTypeTLog(targetTable)){
						
						if(!targetTable.contains(MessageConstant.VERIFY)){
							// CDC 基础表
							sHiveLoadCDCTable += "  " + MessageConstant.SRC_UPDATE_TIME + " timestamp COMMENT '装载区入库时间'"
									+ System.getProperty("line.separator");
							sHiveLoadCDCTable += ") COMMENT '"
								//	+ historyLogTableComment
									+ "thl 表"
									+ "' PARTITIONED BY (partition_date int) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\001' STORED AS TEXTFILE;"
									+ System.getProperty("line.separator");
							// 在下一个表后多一个空行
							sHiveLoadCDCTable += System.getProperty("line.separator");
						}
	
	
						//新增列位置置后
						//1.u:新增列，ods_update_time,partition_date位置调整
						//2.i:新增列，位置不需要调整
						if(addColumn.size()==0){
							//补充yw_update_time,ods_update_time 和 partition_date
							sHiveODSTable += "  " + MessageConstant.ODS_UPDATE_TIME_MSG
									+ System.getProperty("line.separator");
							
							sHiveODSTable += "  " + "partition_date int COMMENT 'ODS 层分区字段'"
									+ System.getProperty("line.separator");
						}else{
							sHiveODSTable=sHiveODSTable.trim();
							sHiveODSTable=sHiveODSTable.endsWith(",")?sHiveODSTable.substring(0,sHiveODSTable.length()-1):sHiveODSTable;
							sHiveODSTable=sHiveODSTable+ System.getProperty("line.separator");
						}
	
	
						sHiveODSTable += ") COMMENT '"
								+ tableComment
								+ "' ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\001'"
								+ " STORED AS ORC "
								+ System.getProperty("line.separator")
								+ MessageConstant.TBLPROPERTIES
	//							+ " CLUSTERED BY("	+ primaryKey + ") INTO "
	//							+ MessageConstant.BUCKETS_SIZE
	//							+ " BUCKETS"
								+ System.getProperty("line.separator");
	
	
						// 创建ods 表后，修改  orc 存储格式信息，目前只支持alter table 方式
						sHiveODSTable += "ALTER TABLE "	+   CommonEnumCollection.HiveDefinePartNameEnum.DB_NAME_ODS.getValue()
								+ targetTable + CommonEnumCollection.HiveDefinePartNameEnum.ODS_TABLE_NAME_SUBFIX.getValue()
								+ " CLUSTERED BY("	+ primaryKey + ") INTO "
								+ MessageConstant.BUCKETS_SIZE
								+ " BUCKETS;"
								+ System.getProperty("line.separator");
						// 在下一个表后多一个空行
						sHiveODSTable += System.getProperty("line.separator");
						
					}

					//新增列位置置后
					//1.u:新增列，ods_update_time,partition_date位置调整
					//2.i:新增列，位置不需要调整
					if(addColumn.size()==0){
						pgsqlAlt += "  " + MessageConstant.YW_UPDATE_TIME +" timestamp," + System.getProperty("line.separator");
					}
					
					if (StringUtils.isNotBlank(primaryKey)) {
						pgsqlAlt += "  " + "PRIMARY KEY (" + primaryKey + ")" + System.getProperty("line.separator");
					}else{
						pgsqlAlt=pgsqlAlt.trim();
						pgsqlAlt=pgsqlAlt.endsWith(",")?pgsqlAlt.substring(0,pgsqlAlt.length()-1):pgsqlAlt;
						pgsqlAlt="  " +pgsqlAlt+ System.getProperty("line.separator");
					}

					sPg += pgsqlAlt + ")" +  ";" + System.getProperty("line.separator");
					sPg += pgsqlCom;
					
					//新增列位置置后
					//1.u:新增列，ods_update_time,partition_date位置调整
					//2.i:新增列，位置不需要调整
					if(addColumn.size()==0){
						sPg += "COMMENT ON COLUMN " + tableNameSpase + targetTable+"."
								+  MessageConstant.PG_UPDATE_TIME_MSG + System.getProperty("line.separator");
					
					}
					
					sPg += "COMMENT ON TABLE " + tableNameSpase +targetTable + " IS '"
							+ tableComment + "';" + System.getProperty("line.separator");
					sPg += System.getProperty("line.separator");

					ywsqlAlt=ywsqlAlt.trim();
					ywsqlAlt=ywsqlAlt.endsWith(",")?ywsqlAlt.substring(0,ywsqlAlt.length()-1):ywsqlAlt;
					ywsqlAlt="  " +ywsqlAlt+ System.getProperty("line.separator");
					
					// 业务数据库脚本
					sYw += ywsqlAlt + ")"  +  ";" + System.getProperty("line.separator");
					sYw += ywsqlCom;
					sYw += "COMMENT ON TABLE " + tableNameSpase + ywTable + " IS '"
							+ tableComment + "';" + System.getProperty("line.separator");
					sYw += System.getProperty("line.separator");


					/*以下部分开始生成最终xml*/
					int tableType = 2;//0-主表，1-从表，2单表，默认单表
					//首先判断当前表的类型
					tableType = createTableType(tableRelationList, targetTable);
					String relationColmStr = getRelatitionColmStr(tableRelationList, targetTable);
					String parentTableName = "";
					TaskPropertiesConfig parentImpTaskConfig = null;
					if (tableType == 1) {
						parentTableName = getParentTableName(tableRelationList, targetTable);
						parentImpTaskConfig = getParentTaskConfig(parentTableName);
					}

					//替换导入xml配置信息
					String schedulerName = task.getValue().getTriggerName().replace("_src","");
					String depImport = groupName + "-" + schedulerName + CommonEnumCollection.HiveDefinePartNameEnum.SRC_TABLE_NAME_SUBFIX.getValue();
					String depThl = groupName + "-" + schedulerName + CommonEnumCollection.HiveDefinePartNameEnum.THL_TABLE_NAME_SUBFIX.getValue();
					String depClnd = groupName + "-" + schedulerName + CommonEnumCollection.HiveDefinePartNameEnum.CLEANED_TABLE_NAME_SUBFIX.getValue();
					String depAllExport = groupName  +  "-" + schedulerName + CommonEnumCollection.HiveDefinePartNameEnum.EXPORT_ALL_TABLE_NAME_SUBFIX.getValue();
					String depSportExport = groupName  +  "-" + schedulerName + CommonEnumCollection.HiveDefinePartNameEnum.EXPORT_SPORT_PERFIX.getValue();
					String depRetailExport = groupName  +  "-" + schedulerName + CommonEnumCollection.HiveDefinePartNameEnum.EXPORT_RETAIL_PERFIX.getValue();
					String depCk = groupName  +  "-" + schedulerName + CommonEnumCollection.HiveDefinePartNameEnum.CK_TABLE_NAME_SUBFIX.getValue();
//					// history_log 导入、导出xml
//					depHistoryLogImport = groupName + "-" + MessageConstant.TRANSCATION_HISTORY_LOG  +
//							CommonEnumCollection.HiveDefinePartNameEnum.SRC_TABLE_NAME_SUBFIX.getValue();
//
//					depHistoryLog_Sport_Export = groupName + "-" + MessageConstant.TRANSCATION_HISTORY_LOG  +
//							CommonEnumCollection.HiveDefinePartNameEnum.EXPORT_RETAIL_PERFIX.getValue();
//					depHistoryLog_Retail_Export = groupName + "-" + MessageConstant.TRANSCATION_HISTORY_LOG  +
//							CommonEnumCollection.HiveDefinePartNameEnum.EXPORT_SPORT_PERFIX.getValue();


					//判断是不是日志表,不生成clnd和thl
					if(!getTypeTLog(sourceTable)){
						if (getOptType(groupName) == 1 ) {
							if(!schedulerName.contains(MessageConstant.VERIFY)){
								dependence += depThl + "," + depImport + System.getProperty("line.separator")
										+ depClnd + "," + depThl + System.getProperty("line.separator")
										+ depAllExport + "," + depClnd + System.getProperty("line.separator");
							}else{
								dependence += depClnd + "," + depImport + System.getProperty("line.separator")
										+ depAllExport + "," + depClnd + System.getProperty("line.separator");
							}
	
						} else if(getOptType(groupName) == 2){
							if(!schedulerName.contains(MessageConstant.VERIFY)){
								dependence += depThl + "," + depImport + System.getProperty("line.separator")
										+ depClnd + "," + depThl + System.getProperty("line.separator")
										+ depRetailExport + "," + depClnd + System.getProperty("line.separator");
							}else{
								dependence += depClnd + "," + depImport + System.getProperty("line.separator")
										+ depRetailExport + "," + depClnd + System.getProperty("line.separator");
							}
						}
						else  if(getOptType(groupName) == 3)  {
							if(!schedulerName.contains(MessageConstant.VERIFY)){
								dependence += depThl + "," + depImport + System.getProperty("line.separator")
										+ depClnd + "," + depThl + System.getProperty("line.separator")
										+ depRetailExport + "," + depClnd + System.getProperty("line.separator")
										+ depSportExport + "," + depClnd + System.getProperty("line.separator");
							}else{
								dependence += depClnd + "," + depImport + System.getProperty("line.separator")
										+ depRetailExport + "," + depClnd + System.getProperty("line.separator")
										+ depSportExport + "," + depClnd + System.getProperty("line.separator");
							}
						}
						if (tableType == 1) {
							dependence += depImport + ","
									+ depImport.replace(schedulerName, parentImpTaskConfig.getSourceTable())
									+ System.getProperty("line.separator");
						}
						
						// 同步数据质量核查，依赖生成
						String sCk = getPreXml.get(getPreXml.get("sCk"+"-"+groupName.toUpperCase()+"-"+schedulerName.toUpperCase()));
						if(StringUtils.isNotBlank(sCk)){
							dependence += depCk + "," + depClnd + System.getProperty("line.separator");
						}
						
					}else{
						// history_log 导入、导出xml
						// cvs dependency
						if (getOptType(g_groupName) == 1 ) {
							dependence += depAllExport + "," + depImport + System.getProperty("line.separator");
						}else if(getOptType(g_groupName) == 2){
							dependence +=  depRetailExport+ "," +  depImport + System.getProperty("line.separator");
							
						}else  if(getOptType(g_groupName) == 3) {
							dependence += depSportExport + "," + depImport + System.getProperty("line.separator")
									+ depRetailExport + "," + depImport + System.getProperty("line.separator");
						}
						
					}


					String parentTaskId = "";
					if (tableType == 1) {
						parentTaskId = parentImpTaskConfig.getId().toString();
					}
					/*
					 * 以下开始生成索引脚本
					 * */
					if (tableType == 1) {
						//主表关联字段索引
						String[] relationColm = relationColmStr.split(",");
						for (int i = 0; i < relationColm.length; i++) {
							String indexSqlLocal = "CREATE INDEX idx_" + parentImpTaskConfig.getTargetTable()
									+ "_"+ relationColm[i] + " ON " + tableNameSpase + parentImpTaskConfig.getTargetTable()
									+ " USING btree (" + relationColm[i].toLowerCase() + ") "  + ";"
									+ System.getProperty("line.separator");
							if (!indexSql.contains(indexSqlLocal)) {
								indexSql += indexSqlLocal;
							}
						}
						//从表关联字段索引
						for (int i = 0; i < relationColm.length; i++) {
							String indexSqlLocal = "CREATE INDEX idx_" + parentImpTaskConfig.getTargetTable()
									+  "_"+ relationColm[i] + " ON " + tableNameSpase + parentImpTaskConfig.getTargetTable()
									+ " USING btree (" + relationColm[i].toLowerCase() + ")  " +";"
											+ System.getProperty("line.separator");
							if (!indexSql.contains(indexSqlLocal)) {
								indexSql += indexSqlLocal;
							}
						}
					}
					
					//创建目标xml
					

					
					//------------u:新增列，且新增列位于最后       ======= 
					if(StringUtils.isNotBlank(nameArray)){
						createTargetXMLForU(schedulerName, groupName, addColumn);
					}else{
						//------------i:新增表  增量和全量       ======= 
						taskSrc = createTargetXML(getPreXml,addColumnName, taskSrc,
								selectColumnsStr, groupName, targetTable,
								primaryKey, tableType, schedulerName, parentTaskId,initLySrc);
						initLySrc++;
						
						//生成需要的hiveCountSql.txt
						createHiveCountSQL(CommonEnumCollection.HiveDefinePartNameEnum.DB_NAME_ODS.getValue()
								+ targetTable + CommonEnumCollection.HiveDefinePartNameEnum.ODS_TABLE_NAME_SUBFIX.getValue(), getPreXml.get("syncTime"+"-"+groupName+"-"+schedulerName));
						
						
						//存档当前物理删除轨迹表的表名和组名(只对-i的现在表，过滤-u)
						//physicDelRecord
						physicDelMap.put(groupName+"-"+schedulerName, String.format(physicDelRecord, groupName,schedulerName));
						
					}
					
				}
				
			}
				
			if (connMysql != null) {
				try {
					connMysql.close();
					
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}


			// 装载区 基础表
//			sHiveHistoryLogTable += "  " + "src_update_time timestamp COMMENT '装载区入库时间'"
//					+ System.getProperty("line.separator");
//
//			sHiveHistoryLogTable += ") COMMENT '"
//					+ "transaction_history_log_src"
//					+ "' PARTITIONED BY (partition_date int) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\001' STORED AS TEXTFILE;"
//					+ System.getProperty("line.separator");
//			// 在下一个表后多一个空行
//			sHiveHistoryLogTable += System.getProperty("line.separator");
//			sHiveLoadBaseTable += sHiveHistoryLogTable;
			
			
			//记录物理删除表SQL
			StringBuffer physicDelBuf=new StringBuffer("");
			//设置物理轨迹表id
			for(Map.Entry<String,String> m:physicDelMap.entrySet())
			{
				physicDelBuf.append(m.getValue().replace("{id}", maxId+"")+System.getProperty("line.separator"));
				maxId++;
			}
			physicDelMap.clear();
			if(physicDelBuf!=null&&StringUtils.isNotBlank(physicDelBuf.toString())){
				OutputStreamWriter outPhysic = new OutputStreamWriter(new FileOutputStream(targetSqlDirPath+"mysql/"
						+ "t_physic_del_record.sql",appendName), "UTF-8");
				outPhysic.write(physicDelBuf.toString().toCharArray());
				outPhysic.flush();
				outPhysic.close();
			}
			
			
			
			
			OutputStreamWriter outLoadBaseHive = new OutputStreamWriter(new FileOutputStream(targetSqlDirPath+"hivesql/"
					+ groupN
					+ CommonEnumCollection.HiveDefinePartNameEnum.SRC_TABLE_NAME_SUBFIX.getValue()
					+".sql",appendName), "UTF-8");
			outLoadBaseHive.write(sHiveLoadBaseTable.toCharArray());
			outLoadBaseHive.flush();
			outLoadBaseHive.close();

			//创建目标cvs
			//1.u:新增列，不需要生成thl.sql
			//2.i:新增列，需要生成thl.sql
			if(getPreXml.get("is_i")!=null&&StringUtils.isNotBlank(sHiveLoadCDCTable)){
				// 装载区 CDC基础表
				OutputStreamWriter outLoadCDCBaseHive = new OutputStreamWriter(new FileOutputStream(targetSqlDirPath+"hivesql/"
						+ groupN
						+ CommonEnumCollection.HiveDefinePartNameEnum.THL_TABLE_NAME_SUBFIX.getValue()
						+".sql",appendName), "UTF-8");
				outLoadCDCBaseHive.write(sHiveLoadCDCTable.toCharArray());
				outLoadCDCBaseHive.flush();
				outLoadCDCBaseHive.close();
			}
			
			if(StringUtils.isNotBlank(sHiveODSTable)){
				// ODS层 基础表
				OutputStreamWriter outODSBaseHive = new OutputStreamWriter(new FileOutputStream(targetSqlDirPath+"hivesql/"
						+ groupN
						+ CommonEnumCollection.HiveDefinePartNameEnum.CLEANED_TABLE_NAME_SUBFIX.getValue()
						+".sql",appendName), "UTF-8");
				outODSBaseHive.write(sHiveODSTable.toCharArray());
				outODSBaseHive.flush();
				outODSBaseHive.close();
			}

			// pg 表

//			sPg += sPgHistoryLogTable;
			OutputStreamWriter outPg = new OutputStreamWriter(new FileOutputStream(targetSqlDirPath+"pgsql/"
					+ groupN + "-pgsql.sql",appendName), "UTF-8");
			outPg.write(sPg.toCharArray());
			outPg.flush();
			outPg.close();
			// pg 表的索引
			OutputStreamWriter outPgIndex = new OutputStreamWriter(new FileOutputStream(targetSqlDirPath+"pgsql/"
					+ "create_index.sql",appendName), "UTF-8");
			outPgIndex.write(indexSql.toCharArray());
			outPgIndex.flush();
			outPgIndex.close();
			// yw 表
			OutputStreamWriter outYw = new OutputStreamWriter(new FileOutputStream(targetSqlDirPath+"ywsql/"
					+ groupN + "-ywsql.sql",appendName), "UTF-8");
			outYw.write(sYw.toCharArray());
			outYw.flush();
			outYw.close();
		}
		
		//创建目标cvs
		//1.u:新增列，不需要生成cvs
		//2.i:新增列，需要生成cvs
		if(getPreXml.get("is_i")!=null){
			createTargetCVS(taskSrc, dependence);
		}

		logger.info("==========>结束生成新表脚本");
	}

	/**
	 * 清理静态值
	 */
	private static void clearStatic(){
		// 装载区HistoryLog表
//		sHiveHistoryLogTable ="";
//		sPgHistoryLogTable = "";
//		transcationHistorytaskSrc = "";
//		transcationHistorytaskRetailExport = "";
//		transcationHistorytaskSportExport = "";
//
//		depHistoryLogImport ="";
//		depHistoryLog_Sport_Export = "";
//		depHistoryLog_Retail_Export = "";
		appendName=null;
		
		//输入参数
		latestXmlDirPath=null;
		targetXMLDirPath=null;
		targetExcelDirPath=null;
		targetSqlDirPath=null;
		maxId=0;
	}
	/**
	 * 查询半年，每天入库数量
	 * @param tableName 表名
	 * @param updateTimeName 新增的列名
	 * @throws IOException
	 */
	private static void createHiveCountSQL(String tableName, String updateTimeName) throws IOException {
		//一天：86400  一年365天，那么半年是：182.5天 
		// 86400*182.5=15768000
		String halfYear="15768000";
		String content = "SELECT * FROM ( SELECT from_unixtime(unix_timestamp("+updateTimeName+"),'yyyy-MM-dd') as dateTemp,count(1) FROM "+tableName +
				" WHERE unix_timestamp()- unix_timestamp("+updateTimeName+")<="+halfYear+" " +
				" GROUP BY from_unixtime(unix_timestamp("+updateTimeName+"),'yyyy-MM-dd') ) as tab order by tab.dateTemp desc";
		OutputStreamWriter outHiveSql = new OutputStreamWriter(new FileOutputStream(targetSqlDirPath + "hivecount/"+tableName, appendName), "UTF-8");
		outHiveSql.write(content);
		outHiveSql.flush();
		outHiveSql.close();
		//统计hive,按天统计每天新增条数，同是只查询半年的数据
	}
	
	/**
	 * 创建目标xml
	 *-----------u:新增表的列       ======= 
	 * @throws IOException
	 */
	private static void createTargetXMLForU(String schedulerName,String groupName,List<Column> addColumn) throws IOException{
		// 生成具体的xml 导入，去重，导出信息
		String clnd=groupName + "-"	+ schedulerName	+ CommonEnumCollection.HiveDefinePartNameEnum.CLEANED_TABLE_NAME_SUBFIX.getValue()+ ".xml";
		String rt=groupName.toLowerCase() + "-"+ schedulerName.toLowerCase() + CommonEnumCollection.HiveDefinePartNameEnum.EXPORT_RETAIL_PERFIX.getValue() + ".xml";
		String sp=groupName.toLowerCase() + "-"	+ schedulerName.toLowerCase() + CommonEnumCollection.HiveDefinePartNameEnum.EXPORT_SPORT_PERFIX.getValue() + ".xml";
		String src=groupName + "-"+ schedulerName+ CommonEnumCollection.HiveDefinePartNameEnum.SRC_TABLE_NAME_SUBFIX.getValue()+ ".xml";
		String all=groupName + "-"+ schedulerName+ CommonEnumCollection.HiveDefinePartNameEnum.EXPORT_ALL_TABLE_NAME_SUBFIX.getValue()+ ".xml";
		
		
		//替换导出xml配置信息
		String clndContent = CreatePreImportXmlUtils.readFile(latestXmlDirPath+clnd);// 基础表导入（sSrc和sCDCImport都存储在这里）
		String rtContent = CreatePreImportXmlUtils.readFile(latestXmlDirPath+rt);// 基础表cdc分发
		String spContent = CreatePreImportXmlUtils.readFile(latestXmlDirPath+sp);// 基础表去重
		String srcContent = CreatePreImportXmlUtils.readFile(latestXmlDirPath+src);// 基础表导出
		String allContent = CreatePreImportXmlUtils.readFile(latestXmlDirPath+all);// _all
		
		String addcols="";
		String clndCols="";
		
		for(Column str:addColumn) {
			clndCols += " src_t."+str.getColumnName()+",";
			addcols += " "+str.getColumnName()+",";
		}
		
		if(addcols.endsWith(","))addcols=addcols.substring(0,addcols.length()-1);
		if(clndCols.endsWith(","))clndCols=clndCols.substring(0,clndCols.length()-1);
		
		clndContent=covertUpdateColU(clndContent, clndCols);
		rtContent=covertUpdateColU(rtContent, addcols);
		spContent=covertUpdateColU(spContent, addcols);
		srcContent=covertUpdateColU(srcContent, addcols);
		allContent=covertUpdateColU(allContent, addcols);
		


		//_src
		OutputStreamWriter outImport = new OutputStreamWriter(new FileOutputStream(targetXMLDirPath+ src,appendName), "UTF-8");
		outImport.write(srcContent.toCharArray());
		outImport.flush();
		outImport.close();
		
		//判断是不是日志表(不生成clnd和thl)
		if(!getTypeTLog(schedulerName)){
			//_clnd
			OutputStreamWriter outOds = new OutputStreamWriter(new FileOutputStream(targetXMLDirPath+ clnd,appendName), "UTF-8");
			outOds.write(clndContent.toCharArray());
			outOds.flush();
			outOds.close();
		}
		OutputStreamWriter outExport=null;

		if(StringUtils.isNotBlank(rtContent)){
			
			//_rt
			outExport = new OutputStreamWriter(new FileOutputStream(targetXMLDirPath + rt,appendName), "UTF-8");
			outExport.write(rtContent.toCharArray());
			outExport.flush();
			outExport.close();
		}
		
		if(StringUtils.isNotBlank(spContent)){
			
			//sp
			outExport = new OutputStreamWriter(new FileOutputStream(targetXMLDirPath + sp,appendName), "UTF-8");
			outExport.write(spContent.toCharArray());
			outExport.flush();
			outExport.close();
		}
		
		if(StringUtils.isNotBlank(allContent)){
			
			//all
			outExport = new OutputStreamWriter(new FileOutputStream(targetXMLDirPath + all,appendName), "UTF-8");
			outExport.write(allContent.toCharArray());
			outExport.flush();
			outExport.close();
		}
		
	}	

	/**
	 * 创建目标CVS
	 *-----------i:新增表  增量和全量       ======= 
	 */
	private static void createTargetCVS(String taskSrc, String dependence)
			throws UnsupportedEncodingException, FileNotFoundException,
			IOException {
		
		List<SchedulerTriggersKey> setVoDependencyImportPre=setVoDependencyImportPre(dependence);
		if(setVoDependencyImportPre!=null&&setVoDependencyImportPre.size()>0){
			String outPath=targetExcelDirPath+ "schedule_task_dependency_import_pre.xlsx";
			ExcelUtil.getInstance().exportExcelByTemplateObject("/model-config/schedule_task_dependency_import_pre.xlsx", outPath, setVoDependencyImportPre,SchedulerTriggersKey.class);
		}
		
		
		
		// excel import task
//		taskSrc += transcationHistorytaskSrc;
//		taskSrc += transcationHistorytaskRetailExport;
//		taskSrc += transcationHistorytaskSportExport;
		List<SchedulerTriggers> setVoImportPre=setVoImportPre(taskSrc);
		if(setVoImportPre!=null&&setVoImportPre.size()>0){
			String outPath=targetExcelDirPath+ "schedule_task_import_pre.xlsx";
			ExcelUtil.getInstance().exportExcelByTemplateObject("/model-config/schedule_task_import_pre.xlsx", outPath, setVoImportPre,SchedulerTriggers.class);
		}
		
		
		
//		OutputStreamWriter outDep = new OutputStreamWriter(new FileOutputStream(targetExcelDirPath
//				+ "schedule_task_dependency_import_pre.xlsx",appendName), "UTF-8");
//		outDep.write(dependence.toCharArray());
//		outDep.flush();
//		outDep.close();
//		// cvs import task
//		taskSrc += transcationHistorytaskSrc;
//		taskSrc += transcationHistorytaskRetailExport;
//		taskSrc += transcationHistorytaskSportExport;
//		OutputStreamWriter outTaskImp = new OutputStreamWriter(new FileOutputStream(targetExcelDirPath
//				+ "schedule_task_import_pre.xlsx",appendName), "UTF-8");
//		outTaskImp.write(taskSrc.toCharArray());
//		outTaskImp.flush();
//		outTaskImp.close();
	}
	
	
	/**
	 * 把字段字符串集合整进VO(schedule_task_import_pre.xlsx)
	 * @param Str  需要填入vo的字段集合
	 * @return
	 */
	private static List<SchedulerTriggersKey> setVoDependencyImportPre(String str){
		List<SchedulerTriggersKey> list=new ArrayList<SchedulerTriggersKey>();
		if(StringUtils.isBlank(str))return null;
		String[] rows=str.split(System.getProperty("line.separator"));
		for(int i=0;i<rows.length;i++){
			if(StringUtils.isBlank(rows[i]))return null;
			String[] strArray=rows[i].split(",");
			SchedulerTriggersKey vo=new SchedulerTriggersKey();
			vo.setTriggerGroup(null);
		    vo.setTriggerName(null);
		    vo.setTask(strArray[0]);
		    vo.setParentTask(strArray[1]);
		    list.add(vo);
		}
		return list;
	}		
	
	/**
	 * 把字段字符串集合整进VO(schedule_task_import_pre.xlsx)
	 * @param Str  需要填入vo的字段集合
	 * @return
	 */
	private static List<SchedulerTriggers> setVoImportPre(String str){
		List<SchedulerTriggers> list=new ArrayList<SchedulerTriggers>();
		if(StringUtils.isBlank(str))return null;
		String[] rows=str.split(System.getProperty("line.separator"));
		for(int i=0;i<rows.length;i++){
			if(StringUtils.isBlank(rows[i]))return null;
			String[] strArray=rows[i].split(",");
			SchedulerTriggers vo=new SchedulerTriggers();
			vo.setTriggerGroup(strArray[0]);
		    vo.setTriggerName(strArray[1]);
		    vo.setJobNumber(strArray[2]);
		    vo.setCronException(strArray[3]);
		    vo.setMbeanurl(strArray[4]);
		    vo.setUrl(strArray[5]);
		    vo.setTimeout(strArray[6]);
		    vo.setCheckInTimeOut(Integer.parseInt(strArray[7]));
		    vo.setTryTime(Integer.parseInt(strArray[8]));
		    vo.setIsCanParallel(Integer.parseInt(strArray[9]));
		    vo.setJobType(Integer.parseInt(strArray[10]));
		    vo.setRemark(strArray[11]);
		    vo.setRange(strArray[12]);
		    list.add(vo);
		}
		return list;
	}	

	/**
	 * 创建目标xml
	 *-----------i:新增表  增量和全量       =======  
	 */
	private static String createTargetXML(Map<String, String> getPreXml,List<String> addColumnName,
			String taskSrc, String selectColumnsStr, String groupName,
			String targetTable, String primaryKey, int tableType,
			String schedulerName, String parentTaskId,int initLySrc)
			throws UnsupportedEncodingException, FileNotFoundException,
			IOException {
		// 生成具体的xml 导入，去重，导出信息
		String bigGroupName=groupName.toUpperCase();
		String bigSchedulerName=schedulerName.toUpperCase();
		String sSrc = getPreXml.get(getPreXml.get("sSrc"+"-"+bigGroupName+"-"+bigSchedulerName));// 基础表导入（sSrc和sCDCImport都存储在这里）
		String sThl = getPreXml.get(getPreXml.get("sThl"+"-"+bigGroupName+"-"+bigSchedulerName));// 基础表cdc分发
		String sOds = getPreXml.get(getPreXml.get("sOds"+"-"+bigGroupName+"-"+bigSchedulerName));// 基础表去重
		String sAllExport = getPreXml.get(getPreXml.get("sAllExport"+"-"+bigGroupName+"-"+bigSchedulerName));// 基础表导出
		String sSportExport = getPreXml.get(getPreXml.get("sSportExport"+"-"+bigGroupName+"-"+bigSchedulerName));//体育
		String sRetailExport = getPreXml.get(getPreXml.get("sRetaiExport"+"-"+bigGroupName+"-"+bigSchedulerName));// 基础表导出, history_log 只有导入，导出，没有去重
		String sCk = getPreXml.get(getPreXml.get("sCk"+"-"+bigGroupName+"-"+bigSchedulerName));// 同步数据质量核查
		String sSrc_ly="";

		String isSlave = "0";
		sSrc = covertFinalImport(sSrc, primaryKey, selectColumnsStr, parentTaskId, primaryKey,
				isSlave);

		//替换thl xml配置信息
		String thlColumnsStr = "";
		String[] thlColumns = MessageConstant.sTranscation_History_log.split(",");
		for (String column : thlColumns ) {
			thlColumnsStr += " src_t.";
			thlColumnsStr += column.trim();
			thlColumnsStr += ",";
		}
		sThl = covertFinalThl(sThl, primaryKey, primaryKey,thlColumnsStr, isSlave);

		//替换ods xml配置信息,去重按照主键
		String odsColumnsStr = "";
		String[] odsColumns = selectColumnsStr.split(",");

		for (String column : odsColumns ) {
			odsColumnsStr += " src_t.";
			odsColumnsStr += column;
			odsColumnsStr += ",";
		}
		sOds = covertFinalOds(sOds, primaryKey, primaryKey,odsColumnsStr, isSlave);
		
		//替换导出xml配置信息
		boolean addFlag=false;
		if (addColumnName.contains(shardingFlag.toUpperCase())) {
			addFlag=true;
		}
		
		if (getOptType(groupName) == 1 ) {
			String uFlag=addFlag?CommonEnumCollection.YW_Type_Enum.ALL_TYPE.getValue():"";
			sAllExport = covertFinalExport(sAllExport, primaryKey, selectColumnsStr, primaryKey, isSlave, uFlag );
		}else  if(getOptType(groupName) == 2)  {
			String uFlag=addFlag? CommonEnumCollection.YW_Type_Enum.RETAIL_TYPE.getValue():"";
			sRetailExport = covertFinalExport(sRetailExport, primaryKey, selectColumnsStr, primaryKey, isSlave, uFlag);
		}
		else  if(getOptType(groupName) == 3)  {
			String uFlag=addFlag? CommonEnumCollection.YW_Type_Enum.SPORT_TYPE.getValue():"";
			sSportExport = covertFinalExport(sSportExport, primaryKey, selectColumnsStr, primaryKey, isSlave,  uFlag);
			uFlag=addFlag? CommonEnumCollection.YW_Type_Enum.RETAIL_TYPE.getValue():"";
			sRetailExport = covertFinalExport(sRetailExport, primaryKey, selectColumnsStr, primaryKey, isSlave, uFlag);
		}
		

		OutputStreamWriter outImport = new OutputStreamWriter(new FileOutputStream(targetXMLDirPath+ groupName + "-"
				+ schedulerName
				+ CommonEnumCollection.HiveDefinePartNameEnum.SRC_TABLE_NAME_SUBFIX.getValue()
				+ ".xml",appendName), "UTF-8");
		outImport.write(sSrc.toCharArray());
		outImport.flush();
		outImport.close();
		
		//_ly_src.xml新增xml  只对分组gly_wms_city生成  add by zhang.rq 2016-09-20 start
		if(groupName.toLowerCase().startsWith("gyl_wms_city")){
			sSrc_ly=sSrc;
			sSrc_ly=covertLySrc(sSrc_ly, getPreXml.get("ly_src_id_"+initLySrc), "1");
			OutputStreamWriter outLyImport = new OutputStreamWriter(new FileOutputStream(targetXMLDirPath+ groupName + "-"
					+ schedulerName+"_ly"
					+ CommonEnumCollection.HiveDefinePartNameEnum.SRC_TABLE_NAME_SUBFIX.getValue()
					+ ".xml",appendName), "UTF-8");
			outLyImport.write(sSrc_ly.toCharArray());
			outLyImport.flush();
			outLyImport.close();
		}
		//add by zhang.rq 2016-09-20 end
		
		
		
		//判断是不是日志表(不生成clnd和thl)
		if(!getTypeTLog(targetTable)){
		
			//判断是不是日志表(不生成clnd和thl)
			if(!schedulerName.contains(MessageConstant.VERIFY)) {
				
				OutputStreamWriter outThl = new OutputStreamWriter(new FileOutputStream(targetXMLDirPath + groupName + "-"
						+ schedulerName
						+ CommonEnumCollection.HiveDefinePartNameEnum.THL_TABLE_NAME_SUBFIX.getValue()
						+ ".xml",appendName), "UTF-8");
				outThl.write(sThl.toCharArray());
				outThl.flush();
				outThl.close();
			}
			OutputStreamWriter outOds = new OutputStreamWriter(new FileOutputStream(targetXMLDirPath+ groupName + "-"
					+ schedulerName
					+ CommonEnumCollection.HiveDefinePartNameEnum.CLEANED_TABLE_NAME_SUBFIX.getValue()
					+ ".xml",appendName), "UTF-8");
			outOds.write(sOds.toCharArray());
			outOds.flush();
			outOds.close();
		}

		if(1 == getOptType(groupName)&&StringUtils.isNotBlank(sAllExport)) {
			OutputStreamWriter outExport = new OutputStreamWriter(new FileOutputStream(targetXMLDirPath + groupName.toLowerCase() + "-"
					+ schedulerName.toLowerCase() + CommonEnumCollection.HiveDefinePartNameEnum.EXPORT_ALL_TABLE_NAME_SUBFIX.getValue() + ".xml",appendName), "UTF-8");
			outExport.write(sAllExport.toCharArray());
			outExport.flush();
			outExport.close();
		}else if(2 == getOptType(groupName)) {
			OutputStreamWriter outExport = new OutputStreamWriter(new FileOutputStream(targetXMLDirPath + groupName.toLowerCase() + "-"
					+ schedulerName.toLowerCase() + CommonEnumCollection.HiveDefinePartNameEnum.EXPORT_RETAIL_PERFIX.getValue() + ".xml",appendName), "UTF-8");
			outExport.write(sRetailExport.toCharArray());
			outExport.flush();
			outExport.close();
		}if(3 == getOptType(groupName)) {
			OutputStreamWriter outExport = new OutputStreamWriter(new FileOutputStream(targetXMLDirPath + groupName.toLowerCase() + "-"
					+ schedulerName.toLowerCase() + CommonEnumCollection.HiveDefinePartNameEnum.EXPORT_RETAIL_PERFIX.getValue() + ".xml",appendName), "UTF-8");
			outExport.write(sRetailExport.toCharArray());
			outExport.flush();
			outExport.close();
			outExport = new OutputStreamWriter(new FileOutputStream(targetXMLDirPath + groupName.toLowerCase() + "-"
					+ schedulerName.toLowerCase() + CommonEnumCollection.HiveDefinePartNameEnum.EXPORT_SPORT_PERFIX.getValue() + ".xml",appendName), "UTF-8");
			outExport.write(sSportExport.toCharArray());
			outExport.flush();
			outExport.close();
		}
		
		// 同步数据质量核查生成_ck.xml
		if(StringUtils.isNotBlank(sCk)){
			OutputStreamWriter outCK = new OutputStreamWriter(new FileOutputStream(targetXMLDirPath + groupName.toLowerCase() + "-"
					+ schedulerName.toLowerCase() + CommonEnumCollection.HiveDefinePartNameEnum.CK_TABLE_NAME_SUBFIX.getValue() + ".xml",appendName), "UTF-8");
			outCK.write(sCk.toCharArray());
			outCK.flush();
			outCK.close();
		}
		
		/*以下部分生成最终scheduler_task*/
		if (tableType == 1) {
			taskSrc += groupName
					+ ","
					+ schedulerName + schedulerName+CommonEnumCollection.HiveDefinePartNameEnum.SRC_TABLE_NAME_SUBFIX.getValue()
					+ ","
					+ commSchedulerTaskCofSrcSlave.replace("`?", targetTable
							+ " import") + System.getProperty("line.separator");

//			transcationHistorytaskSrc = groupName
//					+ ","
//					+ MessageConstant.TRANSCATION_HISTORY_LOG +CommonEnumCollection.HiveDefinePartNameEnum.SRC_TABLE_NAME_SUBFIX.getValue()
//					+ ","
//					+ commSchedulerTaskCofSrcSlave.replace("`?", groupName+"_"+ MessageConstant.TRANSCATION_HISTORY_LOG
//					+CommonEnumCollection.HiveDefinePartNameEnum.SRC_TABLE_NAME_SUBFIX.getValue()) + System.getProperty("line.separator");
		} else {
			taskSrc += groupName
					+ ","
					+ schedulerName+CommonEnumCollection.HiveDefinePartNameEnum.SRC_TABLE_NAME_SUBFIX.getValue()
					+ ","
					+ commSchedulerTaskCofSrc
							.replace("`?",targetTable +CommonEnumCollection.HiveDefinePartNameEnum.SRC_TABLE_NAME_SUBFIX.getValue())
					+ System.getProperty("line.separator");

//			transcationHistorytaskSrc = groupName
//					+ ","
//					+ MessageConstant.TRANSCATION_HISTORY_LOG +CommonEnumCollection.HiveDefinePartNameEnum.SRC_TABLE_NAME_SUBFIX.getValue()
//					+ ","
//					+ commSchedulerTaskCofSrcSlave.replace("`?", groupName+"_"+ MessageConstant.TRANSCATION_HISTORY_LOG
//					+CommonEnumCollection.HiveDefinePartNameEnum.SRC_TABLE_NAME_SUBFIX.getValue()) + System.getProperty("line.separator");
		}
		
		//判断是不是日志表(不生成clnd和thl)
		if(!getTypeTLog(schedulerName)){
			// thl
			if(!schedulerName.contains(MessageConstant.VERIFY)) {
				taskSrc += groupName + "," + schedulerName + CommonEnumCollection.HiveDefinePartNameEnum.THL_TABLE_NAME_SUBFIX.getValue() + ","
						+ commSchedulerTaskCofThl.replace("`?", targetTable  + CommonEnumCollection.HiveDefinePartNameEnum.THL_TABLE_NAME_SUBFIX.getValue())
						+ System.getProperty("line.separator");
			}
			// 去重
			taskSrc += groupName + "," + schedulerName+CommonEnumCollection.HiveDefinePartNameEnum.CLEANED_TABLE_NAME_SUBFIX.getValue() + ","
					+ commSchedulerTaskCofOds.replace("`?", targetTable + CommonEnumCollection.HiveDefinePartNameEnum.CLEANED_TABLE_NAME_SUBFIX.getValue() )
					+ System.getProperty("line.separator");
		}
		// 导出
		if(1 == getOptType(groupName)&&!StringUtils.isBlank(sAllExport)) {
			taskSrc += groupName + ","  + schedulerName + CommonEnumCollection.HiveDefinePartNameEnum.EXPORT_ALL_TABLE_NAME_SUBFIX.getValue() + ","
					+ commSchedulerTaskCofAllExp.replace("`?", targetTable + CommonEnumCollection.HiveDefinePartNameEnum.EXPORT_ALL_TABLE_NAME_SUBFIX.getValue())
					+ System.getProperty("line.separator");


//			transcationHistorytaskRetailExport= groupName + ","  + MessageConstant.TRANSCATION_HISTORY_LOG + CommonEnumCollection.HiveDefinePartNameEnum.EXPORT_ALL_TABLE_NAME_SUBFIX.getValue() + ","
//					+ commSchedulerTaskCofAllExp.replace("`?", MessageConstant.TRANSCATION_HISTORY_LOG + CommonEnumCollection.HiveDefinePartNameEnum.EXPORT_ALL_TABLE_NAME_SUBFIX.getValue())
//					+ System.getProperty("line.separator");

		}else if(2 == getOptType(groupName)) {
			taskSrc += groupName + ","  + schedulerName + CommonEnumCollection.HiveDefinePartNameEnum.EXPORT_RETAIL_PERFIX.getValue() + ","
					+ commSchedulerTaskCofRetailExp.replace("`?", targetTable +  CommonEnumCollection.HiveDefinePartNameEnum.EXPORT_RETAIL_PERFIX.getValue())
					+ System.getProperty("line.separator");
//			transcationHistorytaskRetailExport= groupName + ","  + MessageConstant.TRANSCATION_HISTORY_LOG + CommonEnumCollection.HiveDefinePartNameEnum.EXPORT_ALL_TABLE_NAME_SUBFIX.getValue() + ","
//					+ commSchedulerTaskCofRetailExp.replace("`?", MessageConstant.TRANSCATION_HISTORY_LOG + CommonEnumCollection.HiveDefinePartNameEnum.EXPORT_ALL_TABLE_NAME_SUBFIX.getValue())
//					+ System.getProperty("line.separator");
		}else if(3 == getOptType(groupName)) {
			taskSrc += groupName + ","  + schedulerName + CommonEnumCollection.HiveDefinePartNameEnum.EXPORT_RETAIL_PERFIX.getValue() + ","
					+ commSchedulerTaskCofRetailExp.replace("`?", targetTable + CommonEnumCollection.HiveDefinePartNameEnum.EXPORT_RETAIL_PERFIX.getValue())
					+ System.getProperty("line.separator");

			taskSrc += groupName + ","  + schedulerName + CommonEnumCollection.HiveDefinePartNameEnum.EXPORT_SPORT_PERFIX.getValue() + ","
					+ commSchedulerTaskCofSportExp.replace("`?", targetTable + CommonEnumCollection.HiveDefinePartNameEnum.EXPORT_SPORT_PERFIX.getValue())
					+ System.getProperty("line.separator");

//			transcationHistorytaskRetailExport= groupName + ","  + MessageConstant.TRANSCATION_HISTORY_LOG + CommonEnumCollection.HiveDefinePartNameEnum.EXPORT_RETAIL_PERFIX.getValue() + ","
//					+ commSchedulerTaskCofRetailExp.replace("`?", groupName+"_"+ MessageConstant.TRANSCATION_HISTORY_LOG + CommonEnumCollection.HiveDefinePartNameEnum.EXPORT_RETAIL_PERFIX.getValue())
//					+ System.getProperty("line.separator");
//			transcationHistorytaskSportExport= groupName + ","  + MessageConstant.TRANSCATION_HISTORY_LOG + CommonEnumCollection.HiveDefinePartNameEnum.EXPORT_SPORT_PERFIX.getValue() + ","
//					+ commSchedulerTaskCofSportExp.replace("`?", groupName+"_"+ MessageConstant.TRANSCATION_HISTORY_LOG + CommonEnumCollection.HiveDefinePartNameEnum.EXPORT_SPORT_PERFIX.getValue())
//					+ System.getProperty("line.separator");
		}
		
		// 同步数据质量核查生成_ck.xml
		if(StringUtils.isNotBlank(sCk)){
			taskSrc += groupName
					+ ","
					+ schedulerName+CommonEnumCollection.HiveDefinePartNameEnum.CK_TABLE_NAME_SUBFIX.getValue()
					+ ","
					+ commSchedulerTaskCofCkSlave.replace("`?", targetTable
							+ CommonEnumCollection.HiveDefinePartNameEnum.CK_TABLE_NAME_SUBFIX.getValue()) + System.getProperty("line.separator");
		}
		
		return taskSrc;
	}

	private static boolean hasCol(List<Column> columnList, Column col) {
		Boolean hasCol = false;
		for (Column colu : columnList) {
			if (colu.getColumnName().equalsIgnoreCase(col.getColumnName())) {
				hasCol = true;
			}
		}
		return hasCol;
	}

	private static TaskPropertiesConfig getParentTaskConfig(String parentTableName) {
		TaskPropertiesConfig tpc = null;
		ConcurrentMap<String, TaskPropertiesConfig> taskmap = ParseXMLFileUtil.getTaskList();
		for (Entry<String, TaskPropertiesConfig> task : taskmap.entrySet()) {
			if (task.getValue().getGroupName().contains("_src")
					&& !task.getValue().getTriggerName().contains("_thl")
					&& !task.getValue().getTriggerName().contains("_ods")
					&& !task.getValue().getTriggerName().contains("_exp")
					&& task.getValue().getTargetTable().equalsIgnoreCase(parentTableName)) {
				tpc = task.getValue();
			}
		}
		return tpc;
	}

	private static String getParentTableName(ArrayList<TableRelation> tableRelationList, String table) {
		String parentTableName = "";
		for (TableRelation tbr : tableRelationList) {
			if (tbr.getSlaveTableName().equalsIgnoreCase(table.trim())) {
				parentTableName = tbr.getParentTableName();
			}
		}
		return parentTableName;
	}

	private static String getRelatitionColmStr(ArrayList<TableRelation> tableRelationList, String table) {
		String relatitionColmStr = "";
		for (TableRelation tbr : tableRelationList) {
			if (tbr.getParentTableName().equalsIgnoreCase(table.trim())
					&& tbr.getParentRelationColmName().length() > relatitionColmStr.length()) {
				relatitionColmStr = tbr.getParentRelationColmName();
			} else if (tbr.getSlaveTableName().equalsIgnoreCase(table.trim())
					&& tbr.getSlaveRelationColmName().length() > relatitionColmStr.length()) {
				relatitionColmStr = tbr.getSlaveRelationColmName();
			}
		}
		return relatitionColmStr;
	}

	private static int createTableType(ArrayList<TableRelation> tableRelationList, String table) {
		int tableType = 2;
		for (TableRelation tbr : tableRelationList) {
			if (tbr.getSlaveTableName().equalsIgnoreCase(table.trim())) {
				tableType = 1;
				break;
			} else if (tbr.getParentTableName().equalsIgnoreCase(table.trim())) {
				tableType = 0;
				break;
			}
		}
		return tableType;
	}

	public static Connection getConn(String connectionUrl, String userName, String passWord
			,Connection connMysql,Map<String,String> conConf) {
		
		String driver="";
		if (dbType == DbTypeCollecEnum.ORACLE.getValue()) {
			driver="oracle.jdbc.driver.OracleDriver";
		}else{
			driver="com.mysql.jdbc.Driver";
		}
		
		//判断是是否和上一个连接一样，如果一样直接返回
		if(conConf!=null&&conConf.size()>0&&connMysql!=null){
			if(driver.equals(conConf.get("driver"))&&connectionUrl.equals(conConf.get("connectionUrl"))&&
					userName.equals(conConf.get("userName"))&&passWord.equals(conConf.get("passWord"))){
				return connMysql;
			}
		}else{
			conConf.put("driver",driver);
			conConf.put("connectionUrl",connectionUrl);
			conConf.put("userName",userName);
			conConf.put("passWord",passWord);
			//关闭老连接
			if (connMysql != null) {
				try {
					connMysql.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
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

			conn = DriverManager.getConnection(connectionUrl, userName, passWord);
		} catch (SQLException e) {
			logger.error(e);
			;
		}
		return conn;
	}

	public static String mysqlToHive(String mysqlColType) {
		String hiveColType = null;
		String mysqlColTypeLow = mysqlColType.toLowerCase();
		if (mysqlColTypeLow.contains("char") || mysqlColTypeLow.contains("text")) {
			hiveColType = "string";
		} else if (mysqlColTypeLow.equalsIgnoreCase("date")) {
			hiveColType = "date";
		} else if (mysqlColTypeLow.contains("bigint")) {
			hiveColType = "bigint";
		}else if (mysqlColTypeLow.contains("time")) {
//			hiveColType = "timestamp";
			hiveColType = "string";
		} else if (mysqlColTypeLow.contains("blob")) {
			hiveColType = "binary";
		} else if (mysqlColTypeLow.contains("binary")) {
			hiveColType = "string";
		}  else if (mysqlColTypeLow.contains("double")) {
			hiveColType = "decimal(17,4)";
		} else if (mysqlColTypeLow.contains("float")) {
			hiveColType = "decimal(17,4)";
		} else if (mysqlColTypeLow.contains("smallint")) {
			hiveColType = "int";
		} else if (mysqlColTypeLow.contains("tinyint")) {
			hiveColType = "int";
		} else if (mysqlColTypeLow.contains("int")) {
			hiveColType = "int";
		} else if (mysqlColTypeLow.contains("decimal")) {
			hiveColType = mysqlColTypeLow;
		}else if (mysqlColTypeLow.contains("varbinary")) {
			hiveColType ="string";
		}
		return hiveColType;
	}

	public static String oracleToHive(String mysqlColType) {
		String hiveColType = null;
		String mysqlColTypeLow = mysqlColType.toLowerCase();
		if (mysqlColTypeLow.contains("char") || mysqlColTypeLow.contains("text")) {
			hiveColType = "string";
		} else if (mysqlColTypeLow.equalsIgnoreCase("date")) {
			hiveColType = "timestamp";
		} else if (mysqlColTypeLow.contains("time")) {
//			hiveColType = "timestamp";
			hiveColType = "string";
		} else if (mysqlColTypeLow.contains("blob")) {
			hiveColType = "binary";
		} else if (mysqlColTypeLow.contains("binary")) {
			hiveColType = "string";
		}  else if (mysqlColTypeLow.contains("double")) {
			hiveColType = "decimal(17,4)";
		} else if (mysqlColTypeLow.contains("float")) {
			hiveColType = "decimal(17,4)";
		} else if (mysqlColTypeLow.contains("smallint")) {
			hiveColType = "int";
		} else if (mysqlColTypeLow.contains("tinyint")) {
			hiveColType = "int";
		} else if (mysqlColTypeLow.contains("bigint")) {
			hiveColType = "bigint";
		}else if (mysqlColTypeLow.contains("int")) {
			hiveColType = "int";
		} else if (mysqlColTypeLow.contains("decimal")) {
			hiveColType = mysqlColTypeLow;
		} else if(mysqlColTypeLow.contains("number")){
			hiveColType = "decimal";
		}else if (mysqlColTypeLow.contains("varbinary")) {
			hiveColType ="string";
		}

		return hiveColType;
	}

	public static String mysqlToYw(String mysqlColType) {

		String ywColTypeLow = mysqlColType.toLowerCase();

		return ywColTypeLow;
	}
	public static String mysqlToPg(String mysqlColType) {
		String pgColType = null;
		String pgColTypeLow = mysqlColType.toLowerCase();
		if (pgColTypeLow.contains("char") || pgColTypeLow.contains("text")) {
			pgColType = "text";
		} else if (pgColTypeLow.contains("blob")) {
			pgColType = "bytea";
		} else if (pgColTypeLow.contains("datetime")) {
			pgColType = "timestamp";
		} else if (pgColTypeLow.contains("timestamp")) {
			pgColType = "timestamp";
		} else if (pgColTypeLow.contains("date")) {
			pgColType = "date";
		} else if (pgColTypeLow.contains("double")) {
			pgColType = "numeric(15,2)";
		} else if (pgColTypeLow.contains("float")) {
			pgColType = "numeric"+pgColTypeLow.substring(pgColTypeLow.indexOf("("), pgColTypeLow.indexOf(")")+1);
		} else if (pgColTypeLow.contains("time")) {
//			pgColType = "timestamp";
			pgColType = "time";
		} else if (pgColTypeLow.contains("smallint") || pgColTypeLow.contains("tinyint")) {
			pgColType = "int2";
		} else if (pgColTypeLow.contains("bigint")) {
			pgColType = "int8";
		} else if (pgColTypeLow.contains("int") ) {
			pgColType = "int4";
		}else if (pgColTypeLow.contains("decimal")) {
			pgColType = pgColTypeLow.replace("decimal", "numeric");
		}else if (pgColTypeLow.contains("varbinary")) {
			pgColType ="text";
		}
		return pgColType;
	}
	public static String oraclToPg(String mysqlColType) {
		String pgColType = null;
		String pgColTypeLow = mysqlColType.toLowerCase();
		if (pgColTypeLow.contains("char") || pgColTypeLow.contains("text")) {
			pgColType = "text";
		} else if (pgColTypeLow.contains("blob")) {
			pgColType = "bytea";
		} else if (pgColTypeLow.contains("datetime")) {
			pgColType = "timestamp";
		} else if (pgColTypeLow.contains("timestamp")) {
			pgColType = "timestamp";
		}  else if (pgColTypeLow.contains("date")) {
			pgColType = "timestamp";
		} else if (pgColTypeLow.contains("double")) {
			pgColType = "numeric(15,2)";
		} else if (pgColTypeLow.contains("float")) {
			pgColType = "numeric"+pgColTypeLow.substring(pgColTypeLow.indexOf("("), pgColTypeLow.indexOf(")")+1);
		} else if (pgColTypeLow.contains("time")) {
//			pgColType = "timestamp";
			pgColType = "time";
		} else if (pgColTypeLow.contains("smallint") || pgColTypeLow.contains("tinyint")) {
			pgColType = "int2";
		} else if (pgColTypeLow.contains("bigint")) {
			pgColType = "int8";
		}else if (pgColTypeLow.contains("int") ) {
			pgColType = "int4";
		} else if (pgColTypeLow.contains("decimal")) {
			pgColType = pgColTypeLow.replace("decimal", "numeric");
		}else if(pgColTypeLow.contains("number")){
			pgColType = "decimal";
		}else if (pgColTypeLow.contains("varbinary")) {
			pgColType ="text";
		}
		return pgColType;
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

	public static int getOptType(String groupName) {
		int groupType = 0;
		if( groupName.toLowerCase().contains("miu") || groupName.toLowerCase().contains("blf1")){ // 集团主数据 _all
			groupType = 1;
		} else if(groupName.toLowerCase().contains("retail_mdm") || groupName.toLowerCase().startsWith("gyl")){//_rt
			groupType = 2;
		}else{//_rt 和_sp
			groupType = 3;
		}
		return  groupType;
	}
	
	/**
	 * 判断是否是物理删除日志表
	 * @return false 不是，true 是
	 */
	public static boolean getTypeTLog(String tableName){
		boolean bool=false;
		if(tableName.trim().toLowerCase().contains(MessageConstant.TRANSCATION_HISTORY_LOG)){
			bool=true;
		}
		return bool;
	}

	public Connection getConnById(int id, String dbtype) {
		Connection conn = null;
		String driver = "";
		if (dbtype == "postgressql") {
			driver = "org.postgresql.Driver";
		} else {
			// the default driver is mysql
			driver = "com.mysql.jdbc.Driver";
		}
		TaskDatabaseConfig dbconf = ParseXMLFileUtil.getDbById(id);
		dbType = dbconf.getDbType();
		try {
			Class.forName(driver);
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
		DriverManager.setLoginTimeout(30);
		try {
			conn = DriverManager.getConnection(dbconf.getConnectionUrl(), dbconf.getUserName(), dbconf.getPassword());
		} catch (SQLException e) {
			logger.error(e);
			;
		}
		return conn;
	}

	public static String readFile(String filePath) throws IOException {
		InputStreamReader isr = new InputStreamReader(new FileInputStream(filePath), "UTF-8");
		StringBuffer sbread = new StringBuffer();
		while (isr.ready()) {
			sbread.append((char) isr.read());
		}
		isr.close();
		return sbread.toString();
	}

	private static String covertFinalImport(String sImport, String primaryKey, String selectColumnsStr,
											String parentTaskId, String relationColmStr, String isSlave) {
		if (StringUtils.isNotBlank(parentTaskId)) {
			sImport = sImport.replace("<sourceParentTableId/>", "<sourceParentTableId>" + parentTaskId
					+ "</sourceParentTableId>");
			sImport = sImport.replace("<dependencyTaskIds/>", "<dependencyTaskIds>" + parentTaskId
					+ "</dependencyTaskIds>");
		}
		sImport = sImport.replaceAll("<isSlaveTable>[\\s|\\S]*</isSlaveTable>", "<isSlaveTable>" + isSlave
				+ "</isSlaveTable>");//如果xml已经存在，仅对普通字段修改，请注释此行
		sImport = sImport.replaceAll("<relationColumns>[\\s|\\S]*</relationColumns>", "<relationColumns>"
				+ relationColmStr + "</relationColumns>");//如果xml已经存在，仅对普通字段修改，请注释此行
		sImport = sImport.replaceAll("<primaryKeys>[\\s|\\S]*</primaryKeys>", "<primaryKeys>" + primaryKey
				+ "</primaryKeys>");//如果xml已经存在，仅对普通字段修改，请注释此行
		
		if (dbType == DbTypeCollecEnum.ORACLE.getValue()) {
			sImport = sImport.replaceAll("<selectColumns>[\\s|\\S]*</selectColumns>", "<selectColumns>" + selectColumnsStr
					 +"sysdate as "+ MessageConstant.SRC_UPDATE_TIME + "</selectColumns>");
		}else{
			sImport = sImport.replaceAll("<selectColumns>[\\s|\\S]*</selectColumns>", "<selectColumns>" + selectColumnsStr
					 +"now() as "+ MessageConstant.SRC_UPDATE_TIME + "</selectColumns>");
		}
		
		
		return sImport;
	}

	private static String covertFinalThl(String sThl, String primaryKey, String relationKey,String selectColumnsStr, String isSlave) {
		if(sThl==null)sThl="";
		//新需求注释 relationColumns 和 primaryKeys zhang.sl提出 2016-06-27
//		sThl = sThl.replaceAll("<relationColumns>[\\s|\\S]*</relationColumns>", "<relationColumns>" + relationKey
//				+ "</relationColumns>");//如果xml已经存在，仅对普通字段修改，请注释此行
		sThl = sThl.replaceAll("<isSlaveTable>[\\s|\\S]*</isSlaveTable>", "<isSlaveTable>" + isSlave
				+ "</isSlaveTable>");//如果xml已经存在，仅对普通字段修改，请注释此行
//		sThl = sThl.replaceAll("<primaryKeys>[\\s|\\S]*</primaryKeys>", "<primaryKeys>" + primaryKey + "</primaryKeys>");//如果xml已经存在，仅对普通字段修改，请注释此行
		sThl = sThl.replaceAll("<selectColumns>[\\s|\\S]*</selectColumns>", "<selectColumns>" + selectColumnsStr
				+ MessageConstant.HIVE_UPDATE_TIME +  " as "+ MessageConstant.SRC_UPDATE_TIME +",src_t."+MessageConstant.PARTITION_DATE+ "</selectColumns>");
		return sThl;
	}

	private static String covertFinalOds(String sCln, String primaryKey, String relationKey,String selectColumnsStr, String isSlave) {
		if(sCln==null)sCln="";
		
		sCln = sCln.replaceAll("<relationColumns>[\\s|\\S]*</relationColumns>", "<relationColumns>" + relationKey
				+ "</relationColumns>");//如果xml已经存在，仅对普通字段修改，请注释此行
		sCln = sCln.replaceAll("<isSlaveTable>[\\s|\\S]*</isSlaveTable>", "<isSlaveTable>" + isSlave
				+ "</isSlaveTable>");//如果xml已经存在，仅对普通字段修改，请注释此行
		sCln = sCln.replaceAll("<primaryKeys>[\\s|\\S]*</primaryKeys>", "<primaryKeys>" + primaryKey + "</primaryKeys>");//如果xml已经存在，仅对普通字段修改，请注释此行
		sCln = sCln.replaceAll("<selectColumns>[\\s|\\S]*</selectColumns>", "<selectColumns>" + selectColumnsStr
				+ MessageConstant.HIVE_UPDATE_TIME +  " as "+ MessageConstant.ODS_UPDATE_TIME  +",src_t."+MessageConstant.PARTITION_DATE+ "</selectColumns>");
		return sCln;
	}

	private static String covertFinalExport(String sExport, String primaryKey, String selectColumnsStr,
			String relationColmStr, String isSlave,String uFlag) {
		if(sExport==null)sExport="";
		
		sExport = sExport.replaceAll("<relationColumns>[\\s|\\S]*</relationColumns>", "<relationColumns>"
				+ relationColmStr + "</relationColumns>");//如果xml已经存在，仅对普通字段修改，请注释此行
		sExport = sExport.replaceAll("<primaryKeys>[\\s|\\S]*</primaryKeys>", "<primaryKeys>" + primaryKey
				+ "</primaryKeys>");//如果xml已经存在，仅对普通字段修改，请注释此行
		sExport = sExport.replaceAll("<selectColumns>[\\s|\\S]*</selectColumns>", "<selectColumns>" + selectColumnsStr + " "
				+ MessageConstant.HIVE_UPDATE_TIME +" as "+ MessageConstant.YW_UPDATE_TIME + "</selectColumns>");
		sExport = sExport.replaceAll("<isSlaveTable>[\\s|\\S]*</isSlaveTable>", "<isSlaveTable>" + isSlave
				+ "</isSlaveTable>");//如果xml已经存在，仅对普通字段修改，请注释此行
		sExport = sExport.replaceAll("<filterConditions>[\\s|\\S]*</filterConditions>", "<filterConditions>" + uFlag
				+ "</filterConditions>");
		sExport = sExport.replaceAll("<isSlaveTable>[\\s|\\S]*</isSlaveTable>", "<isSlaveTable>" + isSlave
				+ "</isSlaveTable>");//如果xml已经存在，仅对普通字段修改，请注释此行
		sExport = sExport.replaceAll("<filterConditions>[\\s|\\S]*</filterConditions>", "<filterConditions>" + uFlag
				+ "</filterConditions>");
		return sExport;
	}
	
	/**
	 * 对插入列进行修改
	 * u:新增列，且新增列位于最后
	 */
	private static String covertUpdateColU(String sExport, String selectColumnsStr) {
		if(StringUtils.isNotBlank(selectColumnsStr)){
			selectColumnsStr=","+selectColumnsStr;
		}else{
			return sExport;
		}
		sExport = sExport.replaceAll("</selectColumns>", selectColumnsStr + "</selectColumns>");
		//+ MessageConstant.HIVE_UPDATE_TIME +" as "+ MessageConstant.YW_UPDATE_TIME + 
		return sExport;
	}
	
	/**
	 * _ly_src.xml生成替换_src.xml部分内容
	 * @param sImport
	 * @param id
	 * @param dependencyTaskIds
	 * @param isSlave
	 * @return
	 */
	private static String covertLySrc(String sImport,String id, String isSlave) {
		String dependencyTaskIds="0";
		String triggerName="";
		
		//获取当前src.xml的id
        Pattern p = Pattern.compile("<id>[\\s|\\S]*</id>");
        Matcher m = p.matcher(sImport);
        while (m.find()) {
        	dependencyTaskIds=m.group(0).replace("<id>", "").replace("</id>", "");
        	break;
        } 
        
        //获取当前的triggerName
        p = Pattern.compile("<triggerName>[\\s|\\S]*</triggerName>");
        m = p.matcher(sImport);
        while (m.find()) {
        	triggerName=m.group(0).replace("<triggerName>", "").replace("</triggerName>", "");
        	triggerName=triggerName.substring(0,triggerName.length()-4);
        	triggerName=triggerName+"_ly"+"_src";
        	break;
        }         
        
        
		sImport = sImport.replaceAll("<isSlaveTable>[\\s|\\S]*</isSlaveTable>",
				"<isSlaveTable>" + isSlave + "</isSlaveTable>");
		sImport = sImport.replaceAll(
				"<id>[\\s|\\S]*</id>",
				"<id>" + id + "</id>");
		sImport = sImport.replaceAll("<dependencyTaskIds/>",
				"<dependencyTaskIds>" + dependencyTaskIds + "</dependencyTaskIds>");
		sImport = sImport.replaceAll(
				"<triggerName>[\\s|\\S]*</triggerName>",
				"<triggerName>" + triggerName + "</triggerName>");		
		return sImport;
	}
	
	/**
	 * 查询轨迹表当前最大的id
	 */
	private static void getPhysicID(){
		TaskDatabaseConfig conf=ParseXMLFileUtil.getDbById(pysicDbSourceId);
		if(conf==null)return;
		
		Connection connMysql = getConn(conf.getConnectionUrl(), conf.getUserName(),conf.getPassword(),null,new HashMap<String,String>());
		if(connMysql==null)return;
		
		//查询历史轨迹表的最大id,并且赋值给插入sql语句中
		try {
			ResultSet maxRs = null;
			PreparedStatement maxIdSql = connMysql.prepareStatement(physicDelSql);
			maxRs = maxIdSql.executeQuery();
			if(maxRs!=null&&maxRs.next()) {
				maxId=maxRs.getInt(1);
			}
			maxId++;
		} catch (SQLException e) {
			logger.error(e);
		}finally{
			if(connMysql!=null)
				try {
					connMysql.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
		}

	}
	
	/**
	 * 根据需要创建不存在的目录
	 * @author zhang.rq
	 * @since 2016-06-15
	 * @param dirs
	 */
	private static void createDir(String...dirs){
		if(dirs!=null){
			for(String dirPath:dirs){
				if(StringUtils.isBlank(dirPath))continue;
				
				File dirFile=new File(dirPath);
				if(!dirFile.exists())dirFile.mkdirs();
			}
		}
	}

	/**
	 * 
	 * @param args
	 * 	serialNum   起始编号值
	 *  sorceDbConfPath 路径：数据源配置信息文件所在路径
	 *  latestXmlDirPath 路径：最新所有的表配置xml文件所在路径(同导入模板xml所在路径一个路径)
	 *  targetXMLDirPath 路径：目标xml路径
	 *  targetExcelDirPath 路径：目标excel路径
	 *  targetSqlDirPath 路径：目标sql路径
	 *  sourceDbId 导入的数据源id
	 *  biMdm  路径：新增或者修改表内容配置的.txt路径
	 * @author zhang.rq
	 * @since 2016-06-17
	 * @return String 弹出错误信息
	 */
	public static synchronized String createTargetFile(Map<String,String> args) {
		
		if(args==null&&args.size()>6) return "";
		//已经有程序运行中
		if(StringUtils.isNotBlank(targetXMLDirPath))return "";
		
		String error="";
		
		//-------------------------modify by zhang.rq 2016-06-15  修改路径可配置 start
		//起始编号值
		Integer serialNum=StringUtils.isNotBlank(args.get("serialNum"))?Integer.parseInt(args.get("serialNum")):-1;
		//1.路径：数据源配置信息文件所在路径
		String sorceDbConfPath=StringUtils.isNotBlank(args.get("sorceDbConfPath"))?args.get("sorceDbConfPath"):"";
		//2.路径：最新所有的表配置xml文件所在路径(同导入模板xml所在路径一个路径)
		latestXmlDirPath=StringUtils.isNotBlank(args.get("latestXmlDirPath"))?args.get("latestXmlDirPath"):"";
		latestXmlDirPath="".equals(latestXmlDirPath)||latestXmlDirPath.endsWith("/")?latestXmlDirPath:latestXmlDirPath+"/";
		//3.路径：目标xml路径
		targetXMLDirPath=StringUtils.isNotBlank(args.get("targetXMLDirPath"))?args.get("targetXMLDirPath"):"";
		targetXMLDirPath=targetXMLDirPath.endsWith("/")?targetXMLDirPath:targetXMLDirPath+"/";
		//4.路径：目标excel路径
		targetExcelDirPath=StringUtils.isNotBlank(args.get("targetExcelDirPath"))?args.get("targetExcelDirPath"):"";
		targetExcelDirPath=targetExcelDirPath.endsWith("/")?targetExcelDirPath:targetExcelDirPath+"/";
		//5.路径：目标sql路径
		targetSqlDirPath=StringUtils.isNotBlank(args.get("targetSqlDirPath"))?args.get("targetSqlDirPath"):"";
		targetSqlDirPath=targetSqlDirPath.endsWith("/")?targetSqlDirPath:targetSqlDirPath+"/";
		//6.导入的数据源id
		String sourceDbId=StringUtils.isNotBlank(args.get("sourceDbId"))?args.get("sourceDbId"):"";
		//7.路径：新增或者修改表内容配置的.txt路径
		String biMdm=StringUtils.isNotBlank(args.get("biMdm"))?args.get("biMdm"):"";	
		
		
		//记录当前时间，用于重命名同名文件
		Date dt = new Date();  
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh_mm_ss"); 
		appendName=sdf.format(dt);		
		
		//建立不存在的必须目录
		String[] dirs={latestXmlDirPath,targetXMLDirPath,targetExcelDirPath,targetSqlDirPath
				,targetSqlDirPath+"hivesql",targetSqlDirPath+"pgsql",targetSqlDirPath+"ywsql",targetSqlDirPath+"hivecount",targetSqlDirPath+"mysql"};
		
		try {
			createDir(dirs);
			String[] dbCon={sorceDbConfPath,latestXmlDirPath};
			ParseXMLFileUtil.initTask(dbCon);
			
			//获取第一次生成的临时xml
			Map<String,String> getPreXml=CreatePreImportXmlUtils.createPreXml(biMdm,sourceDbId,latestXmlDirPath,serialNum);
			error=getPreXml.get("error");
			
			//getPhysicID() 查询轨迹数据源是否存在，存在是否正确
			getPhysicID();
			if(getPreXml.get("is_i")!=null&maxId==0){
				error="请设置轨迹删除表的数据源，且数据源id必须为999！！！";
				return error;
			}
			
			if(StringUtils.isBlank(error)){
				//记录当前数据库类型
				TaskDatabaseConfig dbconf = ParseXMLFileUtil.getDbById(Integer.parseInt(sourceDbId));
				dbType = dbconf.getDbType();
				
				createSQL(getPreXml);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (ManagerException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}finally{
			//清理静态变量
			clearStatic();
		}
		return error;

		//------------------------modify by zhang.rq 2016-06-15  end
	}
	
	/**
	 * 获取：起始编号值
	 * @param latestXmlDirPath 最新所有的表配置xml文件所在路径(同导入模板xml所在路径一个路径)
	 * @author zhang.rq
	 * @since 2016-06-17
	 * @return 起始编号值
	 */
	public static Integer getSerialNum(String dirPath){
		Integer serialNum=null;
		File file = new File(dirPath);
		
		List<Integer> ids = new ArrayList<Integer>();
		if (file != null) {
			try {
				// 解析
				for (File f : file.listFiles()) {
					if (!f.getName().endsWith(".xml")) {
						continue;
					}
					String[] searchIds={"id"};
					ids.add(Integer.parseInt(ParseXMLFileUtil.parseTaskXMLReadId(f,searchIds)));
				}
			} catch (ManagerException e) {
				e.printStackTrace();
			}
			for (int i = 55770; i < Integer.MAX_VALUE ; i++) {
				if (!ids.contains(i)) {
					serialNum=i;
					break;
				}
			}			
		}
		return serialNum;
	}
	
	/**
	 * 查询业务系统列表【数据源列表】
	 * @param sorceDbConfPath 路径：数据源配置信息文件所在路径
	 * @return key业务显示名  value数据源对应id
	 */
	public static Map<String,String> getSystemDBConf(String sorceDbConfPath){
		Map<String,String> dbConf=new HashMap<String,String>();
		//读取配置文件
		try {
			dbConf=ParseXMLFileUtil.parseDatabaseXMLToMap(new File(sorceDbConfPath));
		} catch (ManagerException e) {
			e.printStackTrace();
		}
		return dbConf;
	}
	
	
	public static void main(String[] args) {
		
//		//1.生成按钮的调用
		Map<String,String> confFile=new HashMap<String,String>();
		String temp="/t";
		confFile.put("sorceDbConfPath", "C:/Users/user/Desktop/oracl/db-config(d).xml");//D:/Richy/db/db-config.xml D:/data/wonhigh/dc/client/db
//		confFile.put("latestXmlDirPath", "D:/data/diff/wonhigh/dc/client/target/xml");//D:/data/wonhigh/dc/client/task/sqoop
		confFile.put("targetXMLDirPath", "D:/data"+temp+"/wonhigh/dc/client/target/xml");
		confFile.put("targetExcelDirPath", "D:/data"+temp+"/wonhigh/dc/client/target");
		confFile.put("targetSqlDirPath", "D:/data"+temp+"/wonhigh/dc/client/target");
		confFile.put("sourceDbId", "300");
//		confFile.put("serialNum", "80000");
		
		//格式例子：retail_pos-order_payway-update_Richy--u
		//[0]-组名  
		//[1]-表名
		//[2]-update_Time 
		//[3]-增量或全量 
		//[4]-(i:增量和全量；u:新增字段)
		confFile.put("biMdm", "C:/Users/user/Desktop/oracl/gyl_wms_city - 2.txt");//D:/conf/static/retail_pos.txt
		
		System.out.println(createTargetFile(confFile));
		
		
		

//		
//		//2.初始化业务系统列表
//		System.out.println(getSystemDBConf("D:/data/wonhigh/dc/client/db/db-config.xml"));
		
		//3.生成最多的起始值
//		System.out.println(getSerialNum("C:/Users/user/Desktop/xml/xml")); //55775
		
		
//		//4.公用加密方法解密方法
//		String helloName="belle@r_2015";
//		String key="123@testOk";
//		System.out.println("没加密的："+helloName);
//		
//		//加密
//		try {
//			String diffHellName=EncryptionUtils.blowfishEncode(key, helloName);
//			System.out.println("加密后的："+diffHellName);
//			String oldHellName=EncryptionUtils.blowfishDecode(key, diffHellName);
//			System.out.println("解密后的："+oldHellName);
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
		
		
	}
}
