package cn.wonhigh.dc.client.common.util;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import cn.wonhigh.dc.client.common.constans.MessageConstant;
import cn.wonhigh.dc.client.common.model.TaskDatabaseConfig;
import cn.wonhigh.dc.client.common.model.TaskExeSQLPropertiesConfig;
import cn.wonhigh.dc.client.common.model.TaskProPropertiesConfig;
import cn.wonhigh.dc.client.common.model.TaskPropertiesConfig;

import com.yougou.logistics.base.common.exception.ManagerException;

/**
 * 解析执行sql XML文件
 * 
 * @author xiao.py
 * 
 */
public class ParseSQLXMLFileUtil {

	private static final Logger logger = Logger.getLogger(ParseSQLXMLFileUtil.class);
	//缓存任务基本信息
	private static ConcurrentMap<String, TaskExeSQLPropertiesConfig> cacheSQLTaskEntities = new ConcurrentHashMap<String, TaskExeSQLPropertiesConfig>();

	//通过id来映射任务 key：id，value：组名@调度名
	private static ConcurrentMap<Integer, String> cacheSQLTaskEntitiesById = new ConcurrentHashMap<Integer, String>();

	//private static ConcurrentMap<Integer, TaskDatabaseConfig> cacheDbEntities = new ConcurrentHashMap<Integer, TaskDatabaseConfig>();  //数据库map key是ID

	public static final String[] SPECIAL_CHAR_STR = new String[] { "&lt;", "&gt;", "&eq;" };

	public static final String SPLITE_CHAR_1 = ",";
	public static final String SPLITE_CHAR_2 = ":";
	public static final String SPLITE_CHAR_3 = "=";
	public static final String SPLITE_CHAR_4 = "@";

	/**
	 * 通过组名、调度名来获取任务配置信息
	 * 
	 * @param groupName
	 * @param triggerName
	 * @return
	 */
	public static TaskExeSQLPropertiesConfig getTaskConfig(String groupName, String triggerName) {
		return getTaskConfig(groupName + SPLITE_CHAR_4 + triggerName);
	}

	/**
	 * 通过组名、调度名来获取任务配置信息
	 * 
	 * @param groupName
	 * @param triggerName
	 * @return
	 */
	public static TaskExeSQLPropertiesConfig getTaskConfigByKey(String key) {
		if (StringUtils.isNotBlank(key) && key.contains(SPLITE_CHAR_4)) {
			if (cacheSQLTaskEntities.containsKey(key)) {
				return cacheSQLTaskEntities.get(key);
			}
		}
		return null;
	}

	/**
	 * 通过id来获取任务配置信息
	 * 
	 * @param id
	 * @return
	 */
	public static TaskExeSQLPropertiesConfig getTaskConfig(Integer id) {
		if (cacheSQLTaskEntitiesById.containsKey(id)) {
			return getTaskConfigByKey(cacheSQLTaskEntitiesById.get(id));
		}
		return null;
	}

	
	/**
	 * 通过组名、调度名来获取任务配置信息
	 * 
	 * @param key
	 * @return
	 */
	public static TaskExeSQLPropertiesConfig getTaskConfig(String key) {
		
		if (StringUtils.isNotBlank(key) && key.contains(SPLITE_CHAR_4)) {
			if (cacheSQLTaskEntities.containsKey(key)) { 
				TaskExeSQLPropertiesConfig taskConfig= cacheSQLTaskEntities.get(key);
				TaskExeSQLPropertiesConfig dependencyTask = taskConfig.getDependencyTaskList() != null ? taskConfig  
						.getDependencyTaskList().get(0) : null;  
				if(dependencyTask == null){  
					if (taskConfig.getDependencyTaskIds() != null) {  
						List<TaskExeSQLPropertiesConfig> tList = new ArrayList<TaskExeSQLPropertiesConfig>(); 
						for (Integer id : taskConfig.getDependencyTaskIds()) {
							TaskExeSQLPropertiesConfig tmp = getTaskConfig(id); 
							if(tmp==null){
								logger.error("获取依赖的id出错  没有取到id  id:"+id); 
							}else{ 
								tList.add(tmp);
							}
							
						}
						taskConfig.setDependencyTaskList(tList);  
						cacheSQLTaskEntities.put(key, taskConfig);
			    	}
			 }
			 return taskConfig;
		}
	  }
	  return null;
	}
	
	/**
	 * 获取导入的任务
	 */
	public static List<TaskExeSQLPropertiesConfig> getCacheTaskList() {
		List<TaskExeSQLPropertiesConfig> taskList = new ArrayList<TaskExeSQLPropertiesConfig>();
		if (cacheSQLTaskEntities != null && cacheSQLTaskEntities.size() > 0) {
			for (Entry<String, TaskExeSQLPropertiesConfig> entry : cacheSQLTaskEntities.entrySet()) {
				taskList.add(entry.getValue());
			}
		}
		return taskList;
	}

	/**
	 * 通过id来获取任务所需要的数据库信息
	 * 
	 * @param id
	 * @return
	 */
	public static TaskDatabaseConfig getDbById(Integer id) {

		return ParseXMLFileUtil.getDbById(id);
	}

	/**
	 * 初始化解析执行 sql任务的xml
	 */
	public static void initTask() throws ManagerException {
		File file = getTaskDir();  //配置sql xm 所在的文件目录路径
		if (file != null) {
			// 解析
			for (File f : file.listFiles()) {
				if (!f.getName().endsWith(".xml")) {
					continue;
				}
				parseTaskXML(f);
			}

			// 保证一致性
			syncTaskCache();
		}
	}

	/**
	 * 这是把cacheSQLTaskEntitiesById与cacheSQLTaskEntities里面的信息同步更新，因为监控文件是更新cacheSQLTaskEntities的
	 */
	private static void syncTaskCache() {
		for (Entry<String, TaskExeSQLPropertiesConfig> entity : cacheSQLTaskEntities.entrySet()) {
			cacheSQLTaskEntitiesById.put(entity.getValue().getId(), entity.getValue().getGroupName() + SPLITE_CHAR_4
					+ entity.getValue().getTriggerName());
		}
	}

	/**
	 * 初始化解析数据库的xml
	 */
	/*private static void initDb() throws ManagerException {
		File file = getDbDir();  
		if (file != null) { 
			for (File f : file.listFiles()) { 
				if (!f.getName().endsWith(".xml")) { 
					continue;
				}
				parseDatabaseXML(f);
			}
		}
	}*/

	/**
	 * 解析任务类型的xml
	 */
	private static void parseTaskXML(File xmlFile) throws ManagerException {
		SAXReader reader = new SAXReader();
		try {
			// 读入文档流
			Document document = reader.read(xmlFile);
			// 获取根节点
			Element root = document.getRootElement();
			TaskExeSQLPropertiesConfig taskConfig = new TaskExeSQLPropertiesConfig();
			taskConfig
					.setId(root.element("id") != null && StringUtils.isNotBlank(root.element("id").getText()) ? Integer
							.parseInt(root.element("id").getText()) : null);			
			taskConfig.setGroupName(root.element("groupName") != null
					&& StringUtils.isNotBlank(root.element("groupName").getText()) ? root.element("groupName")
					.getText() : null);
			taskConfig.setTriggerName(root.element("triggerName") != null
					&& StringUtils.isNotBlank(root.element("triggerName").getText()) ? root.element("triggerName")
					.getText() : null);
			taskConfig.setSourceDbId(root.element("sourceDbId") != null
					&& StringUtils.isNotBlank(root.element("sourceDbId").getText()) ? Integer.parseInt(root.element(
					"sourceDbId").getText()) : null);
			
			taskConfig.setSourceDbEntity(ParseXMLFileUtil.getDbById(taskConfig.getSourceDbId()) != null ? ParseXMLFileUtil.getDbById
					(taskConfig.getSourceDbId()) : null);
		 
			taskConfig.setIsAddTemporaryUdf(root.element("isAddTemporaryUdf") != null && StringUtils.isNotBlank(root.element(
					"isAddTemporaryUdf").getText()) ? Integer
					.parseInt(root.element("isAddTemporaryUdf").getText()) : null);
			
			taskConfig.setTemporaryUdfName(root.element("temporaryUdfName") != null
					&& StringUtils.isNotBlank(root.element("temporaryUdfName").getText()) ? root.element("temporaryUdfName").getText() : null);
			
			taskConfig.setTemporaryUdfPath(root.element("temporaryUdfPath") != null
					&& StringUtils.isNotBlank(root.element("temporaryUdfPath").getText()) ? root.element("temporaryUdfPath").getText() : null);
			
			taskConfig.setTemporaryUdfClass(root.element("temporaryUdfClass") != null
					&& StringUtils.isNotBlank(root.element("temporaryUdfClass").getText()) ? root.element("temporaryUdfClass").getText() : null);
			
			if (root.element("dependencyTaskIds") != null) {
				List<Integer> subTableIdList = new ArrayList<Integer>();
				// 拆分成多个从表
				String dependencyTaskIds = root.element("dependencyTaskIds").getText();
				if (StringUtils.isNotBlank(dependencyTaskIds)) {
					String[] ids = dependencyTaskIds.split(SPLITE_CHAR_1);
					for (String id : ids) {
						subTableIdList.add(Integer.parseInt(id));
					}
					taskConfig.setDependencyTaskIds(subTableIdList);
				}
			}
			
			taskConfig.setSchedulTimeparameter(root.element("schedulTimeparameter") != null
					&& StringUtils.isNotBlank(root.element("schedulTimeparameter").getText()) ? root.element("schedulTimeparameter").getText() : null);
			
			taskConfig.setExecuteSql(root.element("executeSql") != null
					&& StringUtils.isNotBlank(root.element("executeSql").getText()) ? root.element("executeSql").getText() : null);
					
			/*taskConfig.setIsDownload(root.element("isDownload") != null 
					&& StringUtils.isNotBlank(root.element("isDownload").getText()) ?Integer.parseInt(root.element("isDownload").getText()) : null);
			
			
			taskConfig.setDownloadTableName(root.element("downloadTableName") != null
					&& StringUtils.isNotBlank(root.element("downloadTableName").getText()) ? root.element("downloadTableName").getText() : null);
			
			taskConfig.setDownloadPath(root.element("downloadPath") != null
					&& StringUtils.isNotBlank(root.element("downloadPath").getText()) ? root.element("downloadPath").getText() : null) ;
			*/
			taskConfig.setVersion(root.element("version") != null
					&& StringUtils.isNotBlank(root.element("version").getText()) ? root.element("version").getText() : null) ;
		 
			cacheSQLTaskEntities.put(taskConfig.getGroupName() + SPLITE_CHAR_4 + taskConfig.getTriggerName(), taskConfig);
			
		} catch (Exception e) {
			throw new ManagerException("加载task的xml配置出错：", e);
		}
	}

	/**
	 * 解析数据库类型的xml，监控也是会更新parseDatabaseXML的代码
	 */
	/*@SuppressWarnings("rawtypes")
	private static void parseDatabaseXML(File xmlFile) throws ManagerException {
		SAXReader reader = new SAXReader();

		try {
			// 读入文档流
			Document document = reader.read(xmlFile);
			// 获取根节点
			Element root = document.getRootElement();
			// 遍历多个数据库配置
			for (Iterator iterator = root.elementIterator("database"); iterator.hasNext();) {
				TaskDatabaseConfig dbConfig = new TaskDatabaseConfig();
				Element element = (Element) iterator.next();
				// 获取属性值
				dbConfig.setId(element.attributeValue("id") != null ? Integer.parseInt(element.attributeValue("id"))
						: null);
				dbConfig.setDbType(element.element("dbType") != null ? Integer.parseInt(element.element("dbType")
						.getText()) : null);
				dbConfig.setVersion(element.attributeValue("version") != null ? element.attributeValue("version")
						: null);
				// 获取子元素属性值
				dbConfig.setIpAddr(element.element("ip") != null ? element.element("ip").getText() : null);
				dbConfig.setPort(element.element("port") != null ? element.element("port").getText() : null);
				dbConfig.setUserName(element.element("userName") != null ? element.element("userName").getText() : null);
				dbConfig.setPassword(element.element("password") != null ? element.element("password").getText() : null);
				dbConfig.setDbName(element.element("dbName") != null ? element.element("dbName").getText() : null);
				dbConfig.setCharset(element.element("charset") != null ? element.element("charset").getText() : null);
				// 加入缓存
				cacheDbEntities.put(dbConfig.getId(), dbConfig);
			}

		} catch (Exception e) {
			e.printStackTrace();
			throw new ManagerException("加载db的xml配置文件出错", e);
		}
	}*/

	/*private static File getDbDir() {
		File file = new File(MessageConstant.WINDOW_DB_XML_PATH);
		if (!file.exists()) {
			file = new File(MessageConstant.LINUX_DB_XML_PATH);
			if (file.exists()) {
				return file;
			} else {
				return null;
			}
		} else {
			return file;
		}
	}
*/
	/**
	 * 配置sql xm 所在的文件目录路径
	 * @return
	 */
	private static File getTaskDir() {
		File file = new File(MessageConstant.WINDOW_EXECUTE_SQL_TASK_XML_PATH);
		if (!file.exists()) {
			file = new File(MessageConstant.LINUX_EXECUTE_SQL_TASK_XML_PATH);
			if (file.exists()) {
				return file;
			} else {
				return null;
			}
		} else {
			return file;
		}
	}
	
	
	public static void saveOrUpdate(File xmlFile) throws ManagerException {
	 	ParseSQLXMLFileUtil.parseTaskXML(xmlFile);
		// 保证一致性
		syncTaskCache();
		
	}
	/**
	 * 转义xml中的特殊字符
	 */
	public static String specialCharReplace(String sqlStr) {
		if (StringUtils.isNotBlank(sqlStr)) {
			for (String str : SPECIAL_CHAR_STR) {
				if (sqlStr.indexOf("&lt;") != -1) {
					sqlStr = sqlStr.replace(str, "<");
				} else if (sqlStr.indexOf("&gt;") != -1) {
					sqlStr = sqlStr.replace(str, ">");
				} else if (sqlStr.indexOf("&eq;") != -1) {
					sqlStr = sqlStr.replace(str, "=");
				}
			}
		}
		return sqlStr;
	}

	public static void main(String[] args) throws Exception {
		initTask();
		TaskExeSQLPropertiesConfig config=ParseSQLXMLFileUtil.getTaskConfig("db_rest_client", "shop_pepole_flow_data_2");
		System.out.println(config.getSourceDbId());
	}

}
