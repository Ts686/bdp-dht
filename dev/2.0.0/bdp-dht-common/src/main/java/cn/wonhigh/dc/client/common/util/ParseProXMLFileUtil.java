package cn.wonhigh.dc.client.common.util;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.lang.StringUtils;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import cn.wonhigh.dc.client.common.constans.MessageConstant;
import cn.wonhigh.dc.client.common.model.TaskDatabaseConfig;
import cn.wonhigh.dc.client.common.model.TaskProPropertiesConfig;

import com.yougou.logistics.base.common.exception.ManagerException;

/**
 * 解析存储过程XML文件
 * 
 * @author wang.w
 * 
 */
public class ParseProXMLFileUtil {

	//缓存任务基本信息
	private static ConcurrentMap<String, TaskProPropertiesConfig> cacheproTaskEntities = new ConcurrentHashMap<String, TaskProPropertiesConfig>();

	//通过id来映射任务 key：id，value：组名@调度名
	private static ConcurrentMap<Integer, String> cacheproTaskEntitiesById = new ConcurrentHashMap<Integer, String>();

	private static ConcurrentMap<Integer, TaskDatabaseConfig> cacheDbEntities = new ConcurrentHashMap<Integer, TaskDatabaseConfig>();

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
	public static TaskProPropertiesConfig getTaskConfig(String groupName, String triggerName) {
		return getTaskConfig(groupName + SPLITE_CHAR_4 + triggerName);
	}

	/**
	 * 通过组名、调度名来获取任务配置信息
	 * 
	 * @param groupName
	 * @param triggerName
	 * @return
	 */
	public static TaskProPropertiesConfig getTaskConfig(String key) {
		if (StringUtils.isNotBlank(key) && key.contains(SPLITE_CHAR_4)) {
			if (cacheproTaskEntities.containsKey(key)) {
				return cacheproTaskEntities.get(key);
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
	public static TaskProPropertiesConfig getTaskConfig(Integer id) {
		if (cacheproTaskEntitiesById.containsKey(id)) {
			return getTaskConfig(cacheproTaskEntitiesById.get(id));
		}
		return null;
	}

	/**
	 * 获取导入的任务
	 */
	public static List<TaskProPropertiesConfig> getCacheTaskList() {
		List<TaskProPropertiesConfig> taskList = new ArrayList<TaskProPropertiesConfig>();
		if (cacheproTaskEntities != null && cacheproTaskEntities.size() > 0) {
			for (Entry<String, TaskProPropertiesConfig> entry : cacheproTaskEntities.entrySet()) {
				if (entry.getValue().getTaskType() == MessageConstant.SQOOP_IMPORT) {
					taskList.add(entry.getValue());
				}
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
		if (cacheDbEntities.containsKey(id)) {
			return cacheDbEntities.get(id);
		}
		return null;
	}

	/**
	 * 初始化解析任务的xml
	 */
	public static void initTask() throws ManagerException {
		initDb();
		File file = getTaskDir();
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

	private static void syncTaskCache() {
		for (Entry<String, TaskProPropertiesConfig> entity : cacheproTaskEntities.entrySet()) {
			cacheproTaskEntitiesById.put(entity.getValue().getId(), entity.getValue().getGroupName() + SPLITE_CHAR_4
					+ entity.getValue().getTriggerName());
		}
	}

	/**
	 * 初始化解析数据库的xml
	 */
	private static void initDb() throws ManagerException {
		File file = getDbDir();
		if (file != null) {
			for (File f : file.listFiles()) {
				if (!f.getName().endsWith(".xml")) {
					continue;
				}
				parseDatabaseXML(f);
			}
		}
	}

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
			TaskProPropertiesConfig taskConfig = new TaskProPropertiesConfig();
			taskConfig
					.setId(root.element("id") != null && StringUtils.isNotBlank(root.element("id").getText()) ? Integer
							.parseInt(root.element("id").getText()) : null);
			taskConfig.setTaskType(root.element("taskType") != null
					&& StringUtils.isNotBlank(root.element("taskType").getText()) ? Integer.parseInt(root.element(
					"taskType").getText()) : null);
			taskConfig.setGroupName(root.element("groupName") != null
					&& StringUtils.isNotBlank(root.element("groupName").getText()) ? root.element("groupName")
					.getText() : null);
			taskConfig.setTriggerName(root.element("triggerName") != null
					&& StringUtils.isNotBlank(root.element("triggerName").getText()) ? root.element("triggerName")
					.getText() : null);
			taskConfig.setSourceDbId(root.element("sourceDbId") != null
					&& StringUtils.isNotBlank(root.element("sourceDbId").getText()) ? Integer.parseInt(root.element(
					"sourceDbId").getText()) : null);
			taskConfig.setSourceDbEntity(cacheDbEntities.get(taskConfig.getSourceDbId()) != null ? cacheDbEntities
					.get(taskConfig.getSourceDbId()) : null);
		 
			taskConfig.setPrcedurename(root.element("proceduname") != null
					&& StringUtils.isNotBlank(root.element("proceduname").getText()) ? root.element("proceduname")
					.getText() : "");
			taskConfig.setSyncFreqSeconds(root.element("syncFreqSeconds") != null
					&& StringUtils.isNotBlank(root.element("syncFreqSeconds").getText()) ? Integer.parseInt(root
					.element("syncFreqSeconds").getText()) : null);
			taskConfig.setIsPhysicalDel(root.element("isPhysicalDel") != null
					&& StringUtils.isNotBlank(root.element("isPhysicalDel").getText()) ? Integer.parseInt(root.element(
					"isPhysicalDel").getText()) : null);
			taskConfig.setVersion(root.element("version") != null ? root.element("version").getText() : null);
			taskConfig.setIsreturn(Integer.valueOf(root.element("isreturn") != null ? root.element("isreturn").getText() : "0"));
			taskConfig.setReturntype( root.element("returntype") != null ? root.element("returntype").getText() : "int") ;
		 
			cacheproTaskEntities.put(taskConfig.getGroupName() + SPLITE_CHAR_4 + taskConfig.getTriggerName(), taskConfig);
		} catch (Exception e) {
			throw new ManagerException("加载task的xml配置出错：", e);
		}
	}

	/**
	 * 解析数据库类型的xml
	 */
	@SuppressWarnings("rawtypes")
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
	}

	private static File getDbDir() {
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

	private static File getTaskDir() {
		File file = new File(MessageConstant.WINDOW_SQOOP_PRO_TASK_XML_PATH);
		if (!file.exists()) {
			file = new File(MessageConstant.LINUX_SQOOP_PRO_TASK_XML_PATH);
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
	 	ParseProXMLFileUtil.parseTaskXML(xmlFile);
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
		TaskProPropertiesConfig config=ParseProXMLFileUtil.getTaskConfig("test_dm_pro", "test_dm_pro");
		System.out.println(config.getPrcedurename());
	}

}
