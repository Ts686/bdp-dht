package cn.wonhigh.dc.client.common;

import cn.wonhigh.dc.client.common.constans.MessageConstant;
import cn.wonhigh.dc.client.common.model.TaskDatabaseConfig;
import cn.wonhigh.dc.client.common.model.TaskPropertiesConfig;
import cn.wonhigh.dc.client.common.util.redis.JedisUtils;
import com.yougou.logistics.base.common.exception.ManagerException;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import redis.clients.jedis.Pipeline;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * 解析XML文件
 *
 * @author wang.w
 */
public class ParseXMLFileUtil {
    private static final Logger logger = Logger.getLogger(ParseXMLFileUtil.class);
    //缓存任务基本信息
    private static ConcurrentMap<String, TaskPropertiesConfig> cacheTaskEntities = new ConcurrentHashMap<String, TaskPropertiesConfig>();

    public static ConcurrentMap<String, String> histroyTables = new ConcurrentHashMap<String, String>();

    //通过id来映射任务 key：id，value：组名@调度名
    private static ConcurrentMap<Integer, String> cacheTaskEntitiesById = new ConcurrentHashMap<Integer, String>();

    public static ConcurrentMap<Integer, TaskDatabaseConfig> getCacheDbEntities() {
        return cacheDbEntities;
    }

    private static ConcurrentMap<Integer, TaskDatabaseConfig> cacheDbEntities = new ConcurrentHashMap<Integer, TaskDatabaseConfig>();

    public static final String[] SPECIAL_CHAR_STR = new String[]{"&lt;", "&gt;", "&eq;"};

    public static final String SPLITE_CHAR_1 = ",";
    public static final String SPLITE_CHAR_2 = ":";
    public static final String SPLITE_CHAR_3 = "=";
    public static final String SPLITE_CHAR_4 = "@";
    public static final String SPLITE_CHAR_5 = ";";

    /**
     * 通过组名、调度名来获取任务配置信息
     *
     * @param groupName
     * @param triggerName
     * @return
     */
    public static TaskPropertiesConfig getTaskConfig(String groupName, String triggerName) {
        return getTaskConfig(groupName + SPLITE_CHAR_4 + triggerName);
    }

    /**
     * 通过组名、调度名来获取任务配置信息
     *
     * @param key
     * @return
     */
    public static TaskPropertiesConfig getTaskConfig(String key) {
        if (StringUtils.isNotBlank(key) && key.contains(SPLITE_CHAR_4)) {
            if (cacheTaskEntities.containsKey(key)) {
                TaskPropertiesConfig taskConfig = cacheTaskEntities.get(key);
                TaskPropertiesConfig dependencyTask = taskConfig.getDependencyTaskList() != null
                        && taskConfig.getDependencyTaskList().size() > 0 ? taskConfig.getDependencyTaskList().get(0)
                        : null;
                if (dependencyTask == null && taskConfig.getDependencyTaskIds() != null
                        && taskConfig.getDependencyTaskIds().size() > 0) {
                    if (taskConfig.getDependencyTaskIds() != null) {
                        List<TaskPropertiesConfig> tList = new ArrayList<TaskPropertiesConfig>();
                        for (Integer id : taskConfig.getDependencyTaskIds()) {
                            TaskPropertiesConfig tmp = getTaskConfig(id);
                            if (tmp == null) {
                                logger.error("获取依赖的id出错  没有取到id  id:" + id);
                            } else {
                                tList.add(tmp);
                            }
                            // 当从表的时间戳字段为空时，使用主表的时间戳字段
                        }
                        taskConfig.setDependencyTaskList(tList);
                        cacheTaskEntities.put(key, taskConfig);
                    } else {
                        cacheTaskEntities.put(key, taskConfig);
                    }
                }
                return taskConfig;
            }
        }
        logger.error("key is :" + key + "  while return null");
        return null;
    }

    /**
     * 通过组名、调度名来获取任务配置信息  避免产生过度递归情况
     *
     * @param key
     * @return
     */
    public static TaskPropertiesConfig getTaskConfigByKey(String key) {
        if (StringUtils.isNotBlank(key) && key.contains(SPLITE_CHAR_4)) {
            if (cacheTaskEntities.containsKey(key)) {
                TaskPropertiesConfig taskConfig = cacheTaskEntities.get(key);
                return taskConfig;
            }
        }
        return null;
    }

    /**
     * 初始化某个任务的依赖列表
     */

    /**
     * 通过id来获取任务配置信息
     *
     * @param id
     * @return
     */
    public static TaskPropertiesConfig getTaskConfig(Integer id) {
        if (cacheTaskEntitiesById.containsKey(id)) {
            return getTaskConfigByKey(cacheTaskEntitiesById.get(id));
        }
        return null;
    }

    /**
     * 获取导入的任务
     */
    public static List<TaskPropertiesConfig> getCacheTaskList() {
        List<TaskPropertiesConfig> taskList = new ArrayList<TaskPropertiesConfig>();
        if (cacheTaskEntities != null && cacheTaskEntities.size() > 0) {
            for (Entry<String, TaskPropertiesConfig> entry : cacheTaskEntities.entrySet()) {
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
     * 临时生成的xml处理
     * 初始化解析任务的xml
     */
    public static void initTempTargetXmlFile(String... filePath) throws ManagerException {
        cacheTaskEntities.clear();
        if (filePath != null) {
            // 解析
            for (String f : filePath) {
                if (StringUtils.isBlank(f)) {
                    continue;
                }
                parseTaskXML(null, f);
            }

            // 复制一份以id为主的任务
            syncTaskCache();

            // 配置关联关系+时间戳字段
            for (Entry<String, TaskPropertiesConfig> entity : cacheTaskEntities.entrySet()) {
                TaskPropertiesConfig task = entity.getValue();
                if (task.getDependencyTaskIds() != null) {
                    List<TaskPropertiesConfig> tList = new ArrayList<TaskPropertiesConfig>();
                    for (Integer id : task.getDependencyTaskIds()) {
                        TaskPropertiesConfig tmp = getTaskConfig(id);
                        if (tmp == null) {
                            logger.error("获取依赖的id出错  没有取到id  id:" + id);
                        } else {
                            // 当从表的时间戳字段为空时，使用主表的时间戳字段
                            //							if (tmp.getSyncTimeColumn() == null  ) {
                            //								tmp.setSyncTimeColumn(task.getSyncTimeColumn());
                            //							}
                            tList.add(tmp);
                        }

                    }
                    task.setDependencyTaskList(tList);
                    cacheTaskEntities.put(entity.getKey(), task);
                }
            }

            // 保证一致性
            syncTaskCache();
        }
    }

    /**
     * 初始化解析任务的xml
     */
    public static void initTask(String... filePath) throws ManagerException {
        cacheTaskEntities.clear();
        //modify by zhang.rq 2016-06-15  修改路径可配置 start
        String[] sorceDbConfDirPathArray = null;
        String[] latestXmlDirPathArray = null;

        //a.路径：数据源配置信息文件所在路径
        if (filePath != null && filePath.length >= 1) {
            sorceDbConfDirPathArray = new String[1];
            sorceDbConfDirPathArray[0] = filePath[0];
        }

        initDb(sorceDbConfDirPathArray);

        //b.路径：最新所有的表配置xml文件所在路径
        if (filePath != null && filePath.length >= 2) {
            latestXmlDirPathArray = new String[1];
            latestXmlDirPathArray[0] = filePath[1];
            if (StringUtils.isBlank(filePath[0]))
                return;//不传入最新的xml文件
        }

        File file = getTaskDir(latestXmlDirPathArray);
        //modify by zhang.rq 2016-06-15  修改路径可配置 end

        if (file != null) {
            // 解析
            for (File f : file.listFiles()) {
                if (!f.getName().endsWith(".xml")) {
                    continue;
                }
                parseTaskXML(f);
            }

            // 复制一份以id为主的任务
            syncTaskCache();

            // 配置关联关系+时间戳字段
            for (Entry<String, TaskPropertiesConfig> entity : cacheTaskEntities.entrySet()) {
                TaskPropertiesConfig task = entity.getValue();
                if (task.getDependencyTaskIds() != null && task.getDependencyTaskIds().size() > 0) {
                    List<TaskPropertiesConfig> tList = new ArrayList<TaskPropertiesConfig>();
                    for (Integer id : task.getDependencyTaskIds()) {
                        TaskPropertiesConfig tmp = getTaskConfig(id);
                        if (tmp == null) {
                            logger.error("获取依赖的id出错  没有取到id  id:" + id);
                        } else {
                            // 当从表的时间戳字段为空时，使用主表的时间戳字段
                            //							if (tmp.getSyncTimeColumn() == null  ) {
                            //								tmp.setSyncTimeColumn(task.getSyncTimeColumn());
                            //							}
                            tList.add(tmp);
                        }

                    }
                    task.setDependencyTaskList(tList);
                    cacheTaskEntities.put(entity.getKey(), task);
                }
            }

            // 保证一致性
            syncTaskCache();
        }
    }

    private static void syncTaskCache() {
        for (Entry<String, TaskPropertiesConfig> entity : cacheTaskEntities.entrySet()) {
            cacheTaskEntitiesById.put(entity.getValue().getId(), entity.getValue().getGroupName() + SPLITE_CHAR_4
                    + entity.getValue().getTriggerName());
        }
    }

    /**
     * 初始化解析数据库的xml
     */
    private static void initDb(String... sorceDbConfPath) throws ManagerException {
        File file = getDbDir(sorceDbConfPath);
        if (file != null) {
            //判断，是否不是目录
            if (file.isDirectory()) {
                for (File f : file.listFiles()) {
                    if (!f.getName().endsWith(".xml")) {
                        continue;
                    }
                    parseDatabaseXML(f);
                }
            } else if (file.getName().endsWith(".xml")) {
                parseDatabaseXML(file);
            }
        }
    }

    /**
     * 获取历史数据转移map的keys
     */
    public static Set<String> getHistoryTableKeys() {
        return histroyTables.keySet();
    }

    /**
     * 获取历史数据转移的 主键值
     */
    public static String getHistoryTableHisPrimaryKeys(String key) {
        return histroyTables.get(key);
    }

    /**
     * 更新历史数据转移的 主键值
     */
    public static boolean setHistoryTableHisPrimaryKeys(String key, String value) {
        if (histroyTables.containsKey(key)) {
            String oldValue = histroyTables.get(key);
            return histroyTables.replace(key, oldValue, value);
        }
        return false;
    }

    /**
     * 解析任务类型的xml
     * file to Document
     * String to Document
     */
    @SuppressWarnings("rawtypes")
    public static void parseTaskXML(InputStream in, String file) throws ManagerException {

        TaskPropertiesConfig taskConfig = new TaskPropertiesConfig();
        SAXReader reader = new SAXReader();
        try {

            Document document = reader.read(in);
            // 获取根节点
            Element root = document.getRootElement();
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
            taskConfig.setSourceParentTableId(root.element("sourceParentTableId") != null
                    && StringUtils.isNotBlank(root.element("sourceParentTableId").getText()) ? root.element(
                    "sourceParentTableId").getText() : "");
            taskConfig.setSourceTable(root.element("sourceTable") != null
                    && StringUtils.isNotBlank(root.element("sourceTable").getText()) ? root.element("sourceTable")
                    .getText().toLowerCase() : "");
            // 获取历史备份表
            taskConfig.sethisSourceTable(root.element("hisSourceTable") != null
                    && StringUtils.isNotBlank(root.element("hisSourceTable").getText()) ? root
                    .element("hisSourceTable").getText().toLowerCase() : "");
            // 是否修复数据
            taskConfig.setRepairData(root.element("repairData") != null
                    && StringUtils.isNotBlank(root.element("repairData").getText()) ? Integer.parseInt(root.element(
                    "repairData").getText()) : -1);

            taskConfig.setSelectPreMonth(root.element("selectPreMonth") != null
                    && StringUtils.isNotBlank(root.element("selectPreMonth").getText()) ? Integer.parseInt(root
                    .element("selectPreMonth").getText()) : null);

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
            // 设置关联字段
            String relationColumn = root.element("relationColumns").getText();
            taskConfig.setRelationColumns(relationColumn != null ? Arrays.asList(relationColumn.toLowerCase().split(
                    SPLITE_CHAR_1)) : null);
            taskConfig.setPrimaryKeys(root.element("primaryKeys") != null
                    && StringUtils.isNotBlank(root.element("primaryKeys").getText()) ? Arrays.asList(root
                    .element("primaryKeys").getText().toLowerCase().split(SPLITE_CHAR_1)) : null);
            taskConfig.setSelectColumns(root.element("selectColumns") != null
                    && StringUtils.isNotBlank(root.element("selectColumns").getText()) ? Arrays.asList(root
                    .element("selectColumns").getText().split(SPLITE_CHAR_1)) : null);
            taskConfig.setHisSelectColumns(root.element("hisSelectColumns") != null
                    && StringUtils.isNotBlank(root.element("hisSelectColumns").getText()) ? Arrays.asList(root
                    .element("hisSelectColumns").getText().split(SPLITE_CHAR_1)) : Arrays.asList(""));
            //.element("selectColumns").getText().toLowerCase().split(SPLITE_CHAR_1)) : null);
            taskConfig.setSpecialColumnTypeList(root.element("specialColumnTypeList") != null
                    && StringUtils.isNotBlank(root.element("specialColumnTypeList").getText()) ? Arrays.asList(root
                    .element("specialColumnTypeList").getText().toLowerCase().split(SPLITE_CHAR_1)) : null);
            taskConfig.setSyncTimeColumn(root.element("syncTimeColumn") != null
                    && StringUtils.isNotBlank(root.element("syncTimeColumn").getText()) ? Arrays.asList(root
                    .element("syncTimeColumn").getText().toLowerCase().split(SPLITE_CHAR_5)) : null);
            taskConfig.setTargetDbId(root.element("targetDbId") != null
                    && StringUtils.isNotBlank(root.element("targetDbId").getText()) ? Integer.parseInt(root.element(
                    "targetDbId").getText()) : null);
            taskConfig.setTargetDbEntity(cacheDbEntities.get(taskConfig.getTargetDbId()) != null ? cacheDbEntities
                    .get(taskConfig.getTargetDbId()) : null);
            taskConfig.setTargetTable(root.element("targetTable") != null
                    && StringUtils.isNotBlank(root.element("targetTable").getText()) ? root.element("targetTable")
                    .getText().toLowerCase() : null);
            taskConfig.setTargetColumns(root.element("targetColumns") != null
                    && StringUtils.isNotBlank(root.element("targetColumns").getText()) ? Arrays.asList(root
                    .element("targetColumns").getText().toLowerCase().split(SPLITE_CHAR_1)) : null);
            taskConfig.setSyncFreqSeconds(root.element("syncFreqSeconds") != null
                    && StringUtils.isNotBlank(root.element("syncFreqSeconds").getText()) ? Integer.parseInt(root
                    .element("syncFreqSeconds").getText()) : null);
            taskConfig.setUseSqlFlag(root.element("useSqlFlag") != null
                    && StringUtils.isNotBlank(root.element("useSqlFlag").getText()) ? Integer.parseInt(root.element(
                    "useSqlFlag").getText()) : null);
            taskConfig.setIsSlaveTable(root.element("isSlaveTable") != null
                    && StringUtils.isNotBlank(root.element("isSlaveTable").getText()) ? Integer.parseInt(root.element(
                    "isSlaveTable").getText()) : null);
            taskConfig.setIsOverwrite(root.element("isOverwrite") != null
                    && StringUtils.isNotBlank(root.element("isOverwrite").getText()) ? Integer.parseInt(root.element(
                    "isOverwrite").getText()) : null);
            taskConfig.setIsPhysicalDel(root.element("isPhysicalDel") != null
                    && StringUtils.isNotBlank(root.element("isPhysicalDel").getText()) ? Integer.parseInt(root.element(
                    "isPhysicalDel").getText()) : null);
            // 获取导出保存时间长度 从导出xml
            taskConfig.setExportDateSpan(root.element("exportDateSpan") != null
                    && StringUtils.isNotBlank(root.element("exportDateSpan").getText()) ? Integer.parseInt(root
                    .element("exportDateSpan").getText()) : null);

            // 获取导入Map 从导出xml 中的数量
            taskConfig.setImportMapSize(root.element("importMapSize") != null
                    && StringUtils.isNotBlank(root.element("importMapSize").getText()) ? root.element("importMapSize")
                    .getText() : null);

            // 获取导出Map 从导出xml 中的数量
            taskConfig.setExportMapSize(root.element("exportMapSize") != null
                    && StringUtils.isNotBlank(root.element("exportMapSize").getText()) ? root.element("exportMapSize")
                    .getText() : null);

            // 获取导出方式
            taskConfig.setOpenExpDirect(root.element("isOpenExpDirect") != null
                    && StringUtils.isNotBlank(root.element("isOpenExpDirect").getText()) ? root.element(
                    "isOpenExpDirect").getText() : null);

            if (root.element("filterConditions") == null) {
            } else {
                taskConfig.setFilterConditions(root.element("filterConditions") != null
                        && StringUtils.isNotBlank(root.element("filterConditions").getText()) ? Arrays.asList(root
                        .element("filterConditions").getText().split(SPLITE_CHAR_5)) : null);
            }

            //sqoop hadoop配置参数 (导入导出)
            Element sqoopParam = root.element("sqoopParam");
            if (sqoopParam != null) {
                Map<String, String> sqoopMap = new HashMap<String, String>();
                for (Iterator it = sqoopParam.elementIterator(); it.hasNext(); ) {
                    Element element = (Element) it.next();
                    if (element == null)
                        continue;
                    sqoopMap.put(element.getName(), element.getTextTrim());
                }
                if (sqoopMap.size() > 0)
                    taskConfig.setSqoopParam(sqoopMap);
            }

            //hive hadoop配置参数(清洗)
            Element hiveParam = root.element("hiveParam");
            if (hiveParam != null) {
                Map<String, String> hiveMap = new HashMap<String, String>();
                for (Iterator it = hiveParam.elementIterator(); it.hasNext(); ) {
                    Element element = (Element) it.next();
                    if (element == null)
                        continue;
                    hiveMap.put(element.getName(), element.getTextTrim());
                }
                if (hiveMap.size() > 0)
                    taskConfig.setHiveParam(hiveMap);
            }

            if (root.element("fullExportTimeSpan") == null) {

            } else {
                taskConfig.setFullExportTimeSpan(root.element("fullExportTimeSpan") != null ? root.element(
                        "fullExportTimeSpan").getText() : null);
            }
            taskConfig.setSyncType(root.element("syncType") != null ? root.element("syncType").getText() : "0");
            taskConfig.setVersion(root.element("version") != null ? root.element("version").getText() : null);
            SortedMap<Integer, String> contentMap = new ConcurrentSkipListMap<Integer, String>();
            Element element = root.element("subTaskList");
            if (element != null) {
                // 多个任务封装
                for (Iterator iterator = element.elementIterator("stepContent"); iterator.hasNext(); ) {
                    Element elementEl = (Element) iterator.next();
                    contentMap.put(Integer.parseInt(elementEl.attributeValue("id")),
                            specialCharReplace(elementEl.getText()));
                }
                taskConfig.setTaskContent(contentMap);
            }
            cacheTaskEntities.put(taskConfig.getGroupName() + SPLITE_CHAR_4 + taskConfig.getTriggerName(), taskConfig);
//            System.out.println(cacheTaskEntities.size());
        } catch (Exception e) {
            logger.error(String.format("解析失败...%s", taskConfig.getGroupName() + taskConfig.getTriggerName()) + taskConfig.getId() + "--->" + file, e);
            throw new ManagerException("加载task的xml配置出错：", e);
        }
    }

    /**
     * 解析任务类型的xml
     * file to Document
     * String to Document
     */
    @SuppressWarnings("rawtypes")
    public static void parseTaskXML(InputStream in) throws ManagerException {

        TaskPropertiesConfig taskConfig = new TaskPropertiesConfig();
        SAXReader reader = new SAXReader();
        try {

            Document document = reader.read(in);
            // 获取根节点
            Element root = document.getRootElement();
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
            taskConfig.setSourceParentTableId(root.element("sourceParentTableId") != null
                    && StringUtils.isNotBlank(root.element("sourceParentTableId").getText()) ? root.element(
                    "sourceParentTableId").getText() : "");
            taskConfig.setSourceTable(root.element("sourceTable") != null
                    && StringUtils.isNotBlank(root.element("sourceTable").getText()) ? root.element("sourceTable")
                    .getText().toLowerCase() : "");
            // 获取历史备份表
            taskConfig.sethisSourceTable(root.element("hisSourceTable") != null
                    && StringUtils.isNotBlank(root.element("hisSourceTable").getText()) ? root
                    .element("hisSourceTable").getText().toLowerCase() : "");
            // 是否修复数据
            taskConfig.setRepairData(root.element("repairData") != null
                    && StringUtils.isNotBlank(root.element("repairData").getText()) ? Integer.parseInt(root.element(
                    "repairData").getText()) : -1);

            taskConfig.setSelectPreMonth(root.element("selectPreMonth") != null
                    && StringUtils.isNotBlank(root.element("selectPreMonth").getText()) ? Integer.parseInt(root
                    .element("selectPreMonth").getText()) : null);

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
            // 设置关联字段
            String relationColumn = root.element("relationColumns").getText();
            taskConfig.setRelationColumns(relationColumn != null ? Arrays.asList(relationColumn.toLowerCase().split(
                    SPLITE_CHAR_1)) : null);
            taskConfig.setPrimaryKeys(root.element("primaryKeys") != null
                    && StringUtils.isNotBlank(root.element("primaryKeys").getText()) ? Arrays.asList(root
                    .element("primaryKeys").getText().toLowerCase().split(SPLITE_CHAR_1)) : null);
            taskConfig.setSelectColumns(root.element("selectColumns") != null
                    && StringUtils.isNotBlank(root.element("selectColumns").getText()) ? Arrays.asList(root
                    .element("selectColumns").getText().split(SPLITE_CHAR_1)) : null);
            taskConfig.setHisSelectColumns(root.element("hisSelectColumns") != null
                    && StringUtils.isNotBlank(root.element("hisSelectColumns").getText()) ? Arrays.asList(root
                    .element("hisSelectColumns").getText().split(SPLITE_CHAR_1)) : Arrays.asList(""));
            //.element("selectColumns").getText().toLowerCase().split(SPLITE_CHAR_1)) : null);
            taskConfig.setSpecialColumnTypeList(root.element("specialColumnTypeList") != null
                    && StringUtils.isNotBlank(root.element("specialColumnTypeList").getText()) ? Arrays.asList(root
                    .element("specialColumnTypeList").getText().toLowerCase().split(SPLITE_CHAR_1)) : null);
            taskConfig.setSyncTimeColumn(root.element("syncTimeColumn") != null
                    && StringUtils.isNotBlank(root.element("syncTimeColumn").getText()) ? Arrays.asList(root
                    .element("syncTimeColumn").getText().toLowerCase().split(SPLITE_CHAR_5)) : null);
            taskConfig.setTargetDbId(root.element("targetDbId") != null
                    && StringUtils.isNotBlank(root.element("targetDbId").getText()) ? Integer.parseInt(root.element(
                    "targetDbId").getText()) : null);
            taskConfig.setTargetDbEntity(cacheDbEntities.get(taskConfig.getTargetDbId()) != null ? cacheDbEntities
                    .get(taskConfig.getTargetDbId()) : null);
            taskConfig.setTargetTable(root.element("targetTable") != null
                    && StringUtils.isNotBlank(root.element("targetTable").getText()) ? root.element("targetTable")
                    .getText().toLowerCase() : null);
            taskConfig.setTargetColumns(root.element("targetColumns") != null
                    && StringUtils.isNotBlank(root.element("targetColumns").getText()) ? Arrays.asList(root
                    .element("targetColumns").getText().toLowerCase().split(SPLITE_CHAR_1)) : null);
            taskConfig.setSyncFreqSeconds(root.element("syncFreqSeconds") != null
                    && StringUtils.isNotBlank(root.element("syncFreqSeconds").getText()) ? Integer.parseInt(root
                    .element("syncFreqSeconds").getText()) : null);
            taskConfig.setUseSqlFlag(root.element("useSqlFlag") != null
                    && StringUtils.isNotBlank(root.element("useSqlFlag").getText()) ? Integer.parseInt(root.element(
                    "useSqlFlag").getText()) : null);
            taskConfig.setIsSlaveTable(root.element("isSlaveTable") != null
                    && StringUtils.isNotBlank(root.element("isSlaveTable").getText()) ? Integer.parseInt(root.element(
                    "isSlaveTable").getText()) : null);
            taskConfig.setIsOverwrite(root.element("isOverwrite") != null
                    && StringUtils.isNotBlank(root.element("isOverwrite").getText()) ? Integer.parseInt(root.element(
                    "isOverwrite").getText()) : null);
            taskConfig.setIsPhysicalDel(root.element("isPhysicalDel") != null
                    && StringUtils.isNotBlank(root.element("isPhysicalDel").getText()) ? Integer.parseInt(root.element(
                    "isPhysicalDel").getText()) : null);
            // 获取导出保存时间长度 从导出xml
            taskConfig.setExportDateSpan(root.element("exportDateSpan") != null
                    && StringUtils.isNotBlank(root.element("exportDateSpan").getText()) ? Integer.parseInt(root
                    .element("exportDateSpan").getText()) : null);

            // 获取导入Map 从导出xml 中的数量
            taskConfig.setImportMapSize(root.element("importMapSize") != null
                    && StringUtils.isNotBlank(root.element("importMapSize").getText()) ? root.element("importMapSize")
                    .getText() : null);

            // 获取导出Map 从导出xml 中的数量
            taskConfig.setExportMapSize(root.element("exportMapSize") != null
                    && StringUtils.isNotBlank(root.element("exportMapSize").getText()) ? root.element("exportMapSize")
                    .getText() : null);

            // 获取导出方式
            taskConfig.setOpenExpDirect(root.element("isOpenExpDirect") != null
                    && StringUtils.isNotBlank(root.element("isOpenExpDirect").getText()) ? root.element(
                    "isOpenExpDirect").getText() : null);

            if (root.element("filterConditions") == null) {
            } else {
                taskConfig.setFilterConditions(root.element("filterConditions") != null
                        && StringUtils.isNotBlank(root.element("filterConditions").getText()) ? Arrays.asList(root
                        .element("filterConditions").getText().split(SPLITE_CHAR_5)) : null);
            }

            //sqoop hadoop配置参数 (导入导出)
            Element sqoopParam = root.element("sqoopParam");
            if (sqoopParam != null) {
                Map<String, String> sqoopMap = new HashMap<String, String>();
                for (Iterator it = sqoopParam.elementIterator(); it.hasNext(); ) {
                    Element element = (Element) it.next();
                    if (element == null)
                        continue;
                    sqoopMap.put(element.getName(), element.getTextTrim());
                }
                if (sqoopMap.size() > 0)
                    taskConfig.setSqoopParam(sqoopMap);
            }

            //hive hadoop配置参数(清洗)
            Element hiveParam = root.element("hiveParam");
            if (hiveParam != null) {
                Map<String, String> hiveMap = new HashMap<String, String>();
                for (Iterator it = hiveParam.elementIterator(); it.hasNext(); ) {
                    Element element = (Element) it.next();
                    if (element == null)
                        continue;
                    hiveMap.put(element.getName(), element.getTextTrim());
                }
                if (hiveMap.size() > 0)
                    taskConfig.setHiveParam(hiveMap);
            }

            if (root.element("fullExportTimeSpan") == null) {

            } else {
                taskConfig.setFullExportTimeSpan(root.element("fullExportTimeSpan") != null ? root.element(
                        "fullExportTimeSpan").getText() : null);
            }
            taskConfig.setSyncType(root.element("syncType") != null ? root.element("syncType").getText() : "0");
            taskConfig.setVersion(root.element("version") != null ? root.element("version").getText() : null);
            SortedMap<Integer, String> contentMap = new ConcurrentSkipListMap<Integer, String>();
            Element element = root.element("subTaskList");
            if (element != null) {
                // 多个任务封装
                for (Iterator iterator = element.elementIterator("stepContent"); iterator.hasNext(); ) {
                    Element elementEl = (Element) iterator.next();
                    contentMap.put(Integer.parseInt(elementEl.attributeValue("id")),
                            specialCharReplace(elementEl.getText()));
                }
                taskConfig.setTaskContent(contentMap);
            }
            cacheTaskEntities.put(taskConfig.getGroupName() + SPLITE_CHAR_4 + taskConfig.getTriggerName(), taskConfig);
//            System.out.println(cacheTaskEntities.size());
        } catch (Exception e) {
            logger.error(String.format("解析失败...%s", taskConfig.getGroupName() + taskConfig.getTriggerName()) + taskConfig.getId(), e);
            throw new ManagerException("加载task的xml配置出错：", e);
        }
    }

    /**
     * 获取cacheTaskEntities中的 key set<list></>字段中
     */
    public static Set<String> getCacheTaskEntitiesKeys() {
        return cacheTaskEntities.keySet();
    }

    /**
     * 更新 历史数据迁移表的主键到 taskConfig 的hisPrimaryKeys 字段中
     * file to Document
     * String to Document
     */
    public static boolean setTaskConfigParam(String key, Integer value) {
        TaskPropertiesConfig taskConfig = getTaskConfigByKey(key);
        if (taskConfig == null) {
            logger.error("将key:" + key + " value:" + value + "更新到TaskPropertiesConfig中失败taskConfig is null，"
                    + "请确认key是否在cacheTaskEntities中");
            return false;
        }

        taskConfig.setHisPrimaryKeys(value);
        logger.debug("成功将key:" + key + " value:" + value + "更新到 TaskPropertiesConfig中");
        return true;
    }

    /**
     * 解析任务类型的xml----------只读取id
     * file to Document
     * String to Document
     */
    protected static String parseTaskXMLReadId(File xmlFile, String... fileContent) throws ManagerException {
        SAXReader reader = new SAXReader();
        String str;
        try {
            // 读入文档流
            Document document = null;

            //读取String内容转换为文件流
            document = reader.read(xmlFile);

            // 获取根节点
            Element root = document.getRootElement();
            str = root.element(fileContent[0]) != null
                    && StringUtils.isNotBlank(root.element(fileContent[0]).getText()) ? String.valueOf(root.element(
                    fileContent[0]).getText()) : null;
        } catch (Exception e) {
            throw new ManagerException("加载task的xml配置出错：", e);
        }
        return str;
    }

    /**
     * add by zhang.rq 2016-6-13 17:19
     * DBcache修改后，这里触发taskCache同步更新dbcache依赖
     */
    private static void updateTaskXMLCacheForDbCache() {
        Iterator<String> it = cacheTaskEntities.keySet().iterator();
        while (it.hasNext()) {
            String key = it.next();
            Integer targetDbId = cacheTaskEntities.get(key).getTargetDbId();
            if (targetDbId != null) {
                TaskDatabaseConfig taskConfig = cacheDbEntities.get(targetDbId) != null ? cacheDbEntities
                        .get(targetDbId) : null;
                //cacheTaskEntities.get(key).setSourceDbEntity(taskConfig);
                cacheTaskEntities.get(key).setTargetDbEntity(taskConfig);

            }

            Integer sourceDbId = cacheTaskEntities.get(key).getSourceDbId();
            if (sourceDbId != null) {
                TaskDatabaseConfig taskConfig = cacheDbEntities.get(sourceDbId) != null ? cacheDbEntities
                        .get(sourceDbId) : null;
                cacheTaskEntities.get(key).setSourceDbEntity(taskConfig);
                //cacheTaskEntities.get(key).setTargetDbEntity(taskConfig);
            }

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
            for (Iterator iterator = root.elementIterator("database"); iterator.hasNext(); ) {
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
                dbConfig.setSchemaName(element.element("schemaName") != null ? element.element("schemaName").getText()
                        : null);
                // 加入缓存
                cacheDbEntities.put(dbConfig.getId(), dbConfig);
            }

        } catch (Exception e) {
            e.printStackTrace();
            throw new ManagerException("加载db的xml配置文件出错", e);
        }
    }

    /**
     * 解析数据库类型的xml
     * 返回Map------key:数据库名字;   value：数据库源ID
     *
     * @author zhang.rq  2016-06-17
     */
    @SuppressWarnings("rawtypes")
    static Map<String, String> parseDatabaseXMLToMap(File xmlFile) throws ManagerException {
        SAXReader reader = new SAXReader();
        Map<String, String> dbMap = new HashMap<String, String>();

        try {
            // 读入文档流
            Document document = reader.read(xmlFile);
            // 获取根节点
            Element root = document.getRootElement();
            // 遍历多个数据库配置
            for (Iterator iterator = root.elementIterator("database"); iterator.hasNext(); ) {
                Element element = (Element) iterator.next();
                // 获取属性值--数据库名字
                String dbName = element.element("dbName") != null ? element.element("dbName").getText() : null;
                // 获取属性值--数据库对应源Id
                Integer sourceId = element.attributeValue("id") != null ? Integer
                        .parseInt(element.attributeValue("id")) : null;
                dbMap.put(dbName, sourceId + "");
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new ManagerException("获取业务系统列表【数据库列表】，加载db的xml配置文件出错", e);
        }
        return dbMap;
    }

    public static void saveOrUpdate(File xmlFile) throws ManagerException {
        ParseXMLFileUtil.parseTaskXML(xmlFile);
        // 保证一致性
        syncTaskCache();

    }

    public static void saveOrUpdateDbXml(File xmlFile) throws ManagerException {
        ParseXMLFileUtil.parseDatabaseXML(xmlFile);
    }

    /**
     * add by zhang.rq 2016-6-13 17:19
     * DBcache修改后，这里触发taskCache同步更新dbcache依赖
     */
    public static void saveOrUpdateTaskCache() {
        ParseXMLFileUtil.updateTaskXMLCacheForDbCache();
        // 保证一致性
        syncTaskCache();
    }

    /**
     * modify by zhang.rq 2016-06-15  修改路径可配置
     * windPath[0]  路径：数据库配置文件路径
     */
    private static File getDbDir(String... windPath) {
        String sorceDbConfPath = windPath != null && windPath.length > 0 && windPath[0] != null ? windPath[0]
                : MessageConstant.WINDOW_DB_XML_PATH;

        File file = new File(sorceDbConfPath);
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

    /**
     * modify by zhang.rq 2016-06-15  修改路径可配置
     * windPath[0]  路径：最新的表的xml所在的路径
     */
    private static File getTaskDir(String... windPath) {

        String windowSqoopTaskXmlPath = windPath != null && windPath.length > 0 && windPath[0] != null ? windPath[0]
                : MessageConstant.WINDOW_SQOOP_TASK_XML_PATH;

        File file = new File(windowSqoopTaskXmlPath);
        if (!file.exists()) {
            file = new File(MessageConstant.LINUX_SQOOP_TASK_XML_PATH);
            if (file.exists()) {
                return file;
            } else {
                return null;
            }
        } else {
            return file;
        }
    }

    /**
     * 获取  所有的任务列表
     */
    public static ConcurrentMap<String, TaskPropertiesConfig> getTaskList() {
        return cacheTaskEntities;
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


    /**
     * 解析数据库类型的xml
     */
    @SuppressWarnings("rawtypes")
    private static void parseDatabaseXML(InputStream in) throws ManagerException {
        SAXReader reader = new SAXReader();

        try {
            // 读入文档流
            Document document = reader.read(in);
            // 获取根节点
            Element root = document.getRootElement();
            // 遍历多个数据库配置
            for (Iterator iterator = root.elementIterator("database"); iterator.hasNext(); ) {
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
                dbConfig.setSchemaName(element.element("schemaName") != null ? element.element("schemaName").getText()
                        : null);
                // 加入缓存
                cacheDbEntities.put(dbConfig.getId(), dbConfig);
            }

        } catch (Exception e) {
            e.printStackTrace();
            throw new ManagerException("加载db的xml配置文件出错", e);
        }
    }


    /**
     * 解析任务类型的xml
     * file to Document
     * String to Document
     */
    @SuppressWarnings("rawtypes")
    private static void parseTaskXML(File xmlFile, String... fileContent) throws ManagerException {
        if (xmlFile == null) {
            if (StringUtils.isBlank(fileContent[0]))
                return;
        }

        SAXReader reader = new SAXReader();
        try {
            // 读入文档流
            Document document = null;

            //读取String内容转换为文件流
            if (fileContent != null && fileContent.length > 0) {
                document = DocumentHelper.parseText(fileContent[0]);
            } else {//读取文件转换为文件流
                document = reader.read(xmlFile);
            }

            // 获取根节点
            Element root = document.getRootElement();
            TaskPropertiesConfig taskConfig = new TaskPropertiesConfig();
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
            taskConfig.setSourceParentTableId(root.element("sourceParentTableId") != null
                    && StringUtils.isNotBlank(root.element("sourceParentTableId").getText()) ? root.element(
                    "sourceParentTableId").getText() : "");
            taskConfig.setSourceTable(root.element("sourceTable") != null
                    && StringUtils.isNotBlank(root.element("sourceTable").getText()) ? root.element("sourceTable")
                    .getText().toLowerCase() : "");
            // 获取历史备份表
            taskConfig.sethisSourceTable(root.element("hisSourceTable") != null
                    && StringUtils.isNotBlank(root.element("hisSourceTable").getText()) ? root
                    .element("hisSourceTable").getText().toLowerCase() : "");
            // 是否修复数据
            taskConfig.setRepairData(root.element("repairData") != null
                    && StringUtils.isNotBlank(root.element("repairData").getText()) ? Integer.parseInt(root.element(
                    "repairData").getText()) : -1);

            taskConfig.setSelectPreMonth(root.element("selectPreMonth") != null
                    && StringUtils.isNotBlank(root.element("selectPreMonth").getText()) ? Integer.parseInt(root
                    .element("selectPreMonth").getText()) : null);

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
            // 设置关联字段
            String relationColumn = root.element("relationColumns").getText();
            taskConfig.setRelationColumns(relationColumn != null ? Arrays.asList(relationColumn.toLowerCase().split(
                    SPLITE_CHAR_1)) : null);
            taskConfig.setPrimaryKeys(root.element("primaryKeys") != null
                    && StringUtils.isNotBlank(root.element("primaryKeys").getText()) ? Arrays.asList(root
                    .element("primaryKeys").getText().toLowerCase().split(SPLITE_CHAR_1)) : null);
            taskConfig.setSelectColumns(root.element("selectColumns") != null
                    && StringUtils.isNotBlank(root.element("selectColumns").getText()) ? Arrays.asList(root
                    .element("selectColumns").getText().split(SPLITE_CHAR_1)) : null);
            taskConfig.setHisSelectColumns(root.element("hisSelectColumns") != null
                    && StringUtils.isNotBlank(root.element("hisSelectColumns").getText()) ? Arrays.asList(root
                    .element("hisSelectColumns").getText().split(SPLITE_CHAR_1)) : Arrays.asList(""));
            //.element("selectColumns").getText().toLowerCase().split(SPLITE_CHAR_1)) : null);
            taskConfig.setSpecialColumnTypeList(root.element("specialColumnTypeList") != null
                    && StringUtils.isNotBlank(root.element("specialColumnTypeList").getText()) ? Arrays.asList(root
                    .element("specialColumnTypeList").getText().toLowerCase().split(SPLITE_CHAR_1)) : null);
            taskConfig.setSyncTimeColumn(root.element("syncTimeColumn") != null
                    && StringUtils.isNotBlank(root.element("syncTimeColumn").getText()) ? Arrays.asList(root
                    .element("syncTimeColumn").getText().toLowerCase().split(SPLITE_CHAR_5)) : null);
            taskConfig.setTargetDbId(root.element("targetDbId") != null
                    && StringUtils.isNotBlank(root.element("targetDbId").getText()) ? Integer.parseInt(root.element(
                    "targetDbId").getText()) : null);
            taskConfig.setTargetDbEntity(cacheDbEntities.get(taskConfig.getTargetDbId()) != null ? cacheDbEntities
                    .get(taskConfig.getTargetDbId()) : null);
            taskConfig.setTargetTable(root.element("targetTable") != null
                    && StringUtils.isNotBlank(root.element("targetTable").getText()) ? root.element("targetTable")
                    .getText().toLowerCase() : null);
            taskConfig.setTargetColumns(root.element("targetColumns") != null
                    && StringUtils.isNotBlank(root.element("targetColumns").getText()) ? Arrays.asList(root
                    .element("targetColumns").getText().toLowerCase().split(SPLITE_CHAR_1)) : null);
            taskConfig.setSyncFreqSeconds(root.element("syncFreqSeconds") != null
                    && StringUtils.isNotBlank(root.element("syncFreqSeconds").getText()) ? Integer.parseInt(root
                    .element("syncFreqSeconds").getText()) : null);
            taskConfig.setUseSqlFlag(root.element("useSqlFlag") != null
                    && StringUtils.isNotBlank(root.element("useSqlFlag").getText()) ? Integer.parseInt(root.element(
                    "useSqlFlag").getText()) : null);
            taskConfig.setIsSlaveTable(root.element("isSlaveTable") != null
                    && StringUtils.isNotBlank(root.element("isSlaveTable").getText()) ? Integer.parseInt(root.element(
                    "isSlaveTable").getText()) : null);
            taskConfig.setIsOverwrite(root.element("isOverwrite") != null
                    && StringUtils.isNotBlank(root.element("isOverwrite").getText()) ? Integer.parseInt(root.element(
                    "isOverwrite").getText()) : null);
            taskConfig.setIsPhysicalDel(root.element("isPhysicalDel") != null
                    && StringUtils.isNotBlank(root.element("isPhysicalDel").getText()) ? Integer.parseInt(root.element(
                    "isPhysicalDel").getText()) : null);
            // 获取导出保存时间长度 从导出xml
            taskConfig.setExportDateSpan(root.element("exportDateSpan") != null
                    && StringUtils.isNotBlank(root.element("exportDateSpan").getText()) ? Integer.parseInt(root
                    .element("exportDateSpan").getText()) : null);

            // 获取导入Map 从导出xml 中的数量
            taskConfig.setImportMapSize(root.element("importMapSize") != null
                    && StringUtils.isNotBlank(root.element("importMapSize").getText()) ? root.element("importMapSize")
                    .getText() : null);

            // 获取导出Map 从导出xml 中的数量
            taskConfig.setExportMapSize(root.element("exportMapSize") != null
                    && StringUtils.isNotBlank(root.element("exportMapSize").getText()) ? root.element("exportMapSize")
                    .getText() : null);

            // 获取导出方式
            taskConfig.setOpenExpDirect(root.element("isOpenExpDirect") != null
                    && StringUtils.isNotBlank(root.element("isOpenExpDirect").getText()) ? root.element(
                    "isOpenExpDirect").getText() : null);

            if (root.element("filterConditions") == null) {
            } else {
                taskConfig.setFilterConditions(root.element("filterConditions") != null
                        && StringUtils.isNotBlank(root.element("filterConditions").getText()) ? Arrays.asList(root
                        .element("filterConditions").getText().split(SPLITE_CHAR_5)) : null);
            }

            //sqoop hadoop配置参数 (导入导出)
            Element sqoopParam = root.element("sqoopParam");
            if (sqoopParam != null) {
                Map<String, String> sqoopMap = new HashMap<String, String>();
                for (Iterator it = sqoopParam.elementIterator(); it.hasNext(); ) {
                    Element element = (Element) it.next();
                    if (element == null)
                        continue;
                    sqoopMap.put(element.getName(), element.getTextTrim());
                }
                if (sqoopMap.size() > 0)
                    taskConfig.setSqoopParam(sqoopMap);
            }

            //hive hadoop配置参数(清洗)
            Element hiveParam = root.element("hiveParam");
            if (hiveParam != null) {
                Map<String, String> hiveMap = new HashMap<String, String>();
                for (Iterator it = hiveParam.elementIterator(); it.hasNext(); ) {
                    Element element = (Element) it.next();
                    if (element == null)
                        continue;
                    hiveMap.put(element.getName(), element.getTextTrim());
                }
                if (hiveMap.size() > 0)
                    taskConfig.setHiveParam(hiveMap);
            }

            if (root.element("fullExportTimeSpan") == null) {

            } else {
                taskConfig.setFullExportTimeSpan(root.element("fullExportTimeSpan") != null ? root.element(
                        "fullExportTimeSpan").getText() : null);
            }
            taskConfig.setSyncType(root.element("syncType") != null ? root.element("syncType").getText() : "0");
            taskConfig.setVersion(root.element("version") != null ? root.element("version").getText() : null);
            SortedMap<Integer, String> contentMap = new ConcurrentSkipListMap<Integer, String>();
            Element element = root.element("subTaskList");
            if (element != null) {
                // 多个任务封装
                for (Iterator iterator = element.elementIterator("stepContent"); iterator.hasNext(); ) {
                    Element elementEl = (Element) iterator.next();
                    contentMap.put(Integer.parseInt(elementEl.attributeValue("id")),
                            specialCharReplace(elementEl.getText()));
                }
                taskConfig.setTaskContent(contentMap);
            }
            cacheTaskEntities.put(taskConfig.getGroupName() + SPLITE_CHAR_4 + taskConfig.getTriggerName(), taskConfig);

        } catch (Exception e) {
            throw new ManagerException("加载task的xml配置出错：", e);
        }
    }

    /**
     * 初始化解析任务的xml
     */
    public static void initTaskByRedis() throws Exception {
        logger.info("加载redis......");
        //cacheTaskEntities.clear();
        Set<String> dbConfigKeys = JedisUtils.keys("BDP_MDM_DB_CONFIG*");
        Set<String> taskConfigKeys = JedisUtils.keys("BDP_MDM_TASK_XML*");
        for (String configKey : dbConfigKeys) {
            String configVal = JedisUtils.get(configKey);
            parseDatabaseXML(new ByteArrayInputStream(configVal.getBytes()));
        }
        Pipeline pipeline = JedisUtils.getResource().pipelined();
        int i = 0;
//        String configVal = JedisUtils.get(configKey);
        for (String configKey : taskConfigKeys) {
            pipeline.get(configKey);
            i++;
            if (i % 500 == 0) {
                List<Object> datas = pipeline.syncAndReturnAll();
                handlePipeline(datas);
                logger.info("cache redis " + i);
            }
        }
        List<Object> datas = pipeline.syncAndReturnAll();
        handlePipeline(datas);
        pipeline.close();
        syncTaskCache();
        // 配置关联关系+时间戳字段
        for (Entry<String, TaskPropertiesConfig> entity : cacheTaskEntities.entrySet()) {
            TaskPropertiesConfig task = entity.getValue();
            if (task.getDependencyTaskIds() != null && task.getDependencyTaskIds().size() > 0) {
                List<TaskPropertiesConfig> tList = new ArrayList<TaskPropertiesConfig>();
                for (Integer id : task.getDependencyTaskIds()) {
                    TaskPropertiesConfig tmp = getTaskConfig(id);
                    if (tmp == null) {
                        logger.error("获取依赖的id出错  没有取到id  id:" + id);
                    } else {
                        tList.add(tmp);
                    }
                }
                task.setDependencyTaskList(tList);
                cacheTaskEntities.put(entity.getKey(), task);
            }
        }
        // 保证一致性
        syncTaskCache();
    }

    private static void handlePipeline(List<Object> datas) {
        for (Object o : datas) {
//            logger.info("任务" + o.toString());
            try {
                parseTaskXML(new ByteArrayInputStream(o.toString().getBytes()), o.toString());
            } catch (ManagerException e) {
                e.printStackTrace();
                continue;
            }
        }
    }


    /**
     * 初始化解析任务的xml
     */
    private static void initTaskByRedis2() throws Exception {
        //cacheTaskEntities.clear();
        Set<String> taskConfigKeys = JedisUtils.keys("BDP_MDM_TASK_XML*clnd.xml");
        Pipeline pipeline = JedisUtils.getResource().pipelined();
        int i = 0;
//        String configVal = JedisUtils.get(configKey);
        for (String configKey : taskConfigKeys) {
            pipeline.get(configKey);
            i++;
            if (i % 500 == 0) {
                List<Object> datas = pipeline.syncAndReturnAll();
                handlePipeline(datas);
                logger.info("cache redis " + i);
            }
        }
        List<Object> datas = pipeline.syncAndReturnAll();
        handlePipeline(datas);
        pipeline.close();
        syncTaskCache();

    }

    public static void main(String[] args) throws Exception {
        initTaskByRedis2();
        for (TaskPropertiesConfig config : cacheTaskEntities.values()) {
            if (config.getSyncType().equals("2") && config.getIsOverwrite().equals(0)) {
                System.out.println(config.getGroupName() + " " + config.getTriggerName() + " " + config.getSyncType());
            }
        }
    }
}
