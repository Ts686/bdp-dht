package cn.wonhigh.dc.client.common.util;

import java.io.File;
import java.io.FileWriter;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.XMLWriter;

import cn.wonhigh.dc.client.common.constans.MessageConstant;
import cn.wonhigh.dc.client.common.model.TaskPropertiesConfig;

/**
 * 创建xml文件
 * 
 * @author wang.w
 * 
 */
public class CreateTaskConfigXMLUtil implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 469183807603996194L;

	public static final String SPLITE_CHAR_1 = ",";
	public static final String SPLITE_CHAR_2 = ":";
	public static final String SPLITE_CHAR_3 = "=";
	public static final String SPLITE_CHAR_4 = "@";

	public static void createFile(TaskPropertiesConfig taskConfig, Map<String, String> paramsMap) throws Exception {
		// 创建Document
		Document document = DocumentHelper.createDocument();
		// 创建根root
		Element root = document.addElement("root");
		root.addComment(paramsMap.get("comments") != null ? paramsMap.get("comments") : "This is a config of "
				+ taskConfig.getGroupName() + "." + taskConfig.getSourceTable());
		Element id = root.addElement("id");
		id.setText(taskConfig.getId() + "");
		Element taskType = root.addElement("taskType");
		taskType.setText(taskConfig.getTaskType() + "");
		Element groupName = root.addElement("groupName");
		groupName.setText(taskConfig.getGroupName());
		Element triggerName = root.addElement("triggerName");
		triggerName.setText(taskConfig.getTriggerName());
		Element sourceDbId = root.addElement("sourceDbId");
		sourceDbId.setText(taskConfig.getSourceDbId() + "");
		Element sourceParentTableId = root.addElement("sourceParentTableId");
		sourceParentTableId.setText(taskConfig.getSourceParentTableId() != null ? taskConfig.getSourceParentTableId()
				: "");
		Element sourceTable = root.addElement("sourceTable");
		sourceTable.setText(taskConfig.getSourceTable() != null ? taskConfig.getSourceTable() : "");
		Element dependencyTaskIds = root.addElement("dependencyTaskIds");
		dependencyTaskIds.setText(taskConfig.getDependencyTaskIds() != null ? StringUtils.list2String1(
				taskConfig.getDependencyTaskIds(), SPLITE_CHAR_1) : "");
		Element relationColumns = root.addElement("relationColumns");
		relationColumns.setText(taskConfig.getRelationColumns() != null ? StringUtils.list2String(
				taskConfig.getRelationColumns(), SPLITE_CHAR_1) : "");
		// 默认主键为id
		Element primaryKeys = root.addElement("primaryKeys");
		primaryKeys.setText(taskConfig.getPrimaryKeys() != null ? StringUtils.list2String(taskConfig.getPrimaryKeys(),
				SPLITE_CHAR_1) : "id");
		Element selectColumns = root.addElement("selectColumns");
		selectColumns.setText(taskConfig.getSelectColumns() != null ? StringUtils.list2String(
				taskConfig.getSelectColumns(), SPLITE_CHAR_1) : "");

		Element specialColumnTypeList = root.addElement("specialColumnTypeList");
		specialColumnTypeList.setText(taskConfig.getSpecialColumnTypeList() != null ? StringUtils.list2String(
				taskConfig.getSpecialColumnTypeList(), SPLITE_CHAR_1) : "");
		Element syncTimeColumn = root.addElement("syncTimeColumn");
		syncTimeColumn.setText(taskConfig.getSyncTimeColumn() != null ? StringUtils.list2String(
				taskConfig.getSyncTimeColumn(), SPLITE_CHAR_1) : "");
		Element targetDbId = root.addElement("targetDbId");
		targetDbId.setText(taskConfig.getTargetDbId() != null ? taskConfig.getTargetDbId() + "" : "");
		Element targetTable = root.addElement("targetTable");
		targetTable.setText(taskConfig.getTargetTable() != null ? taskConfig.getTargetTable() : "");
		Element targetColumns = root.addElement("targetColumns");
		targetColumns.setText(taskConfig.getTargetColumns() != null ? StringUtils.list2String(
				taskConfig.getTargetColumns(), SPLITE_CHAR_1) : "");
		Element syncFreqSeconds = root.addElement("syncFreqSeconds");
		syncFreqSeconds.setText(taskConfig.getSyncFreqSeconds() != null ? taskConfig.getSyncFreqSeconds() + "" : "");
		Element useSqlFlag = root.addElement("useSqlFlag");
		useSqlFlag.setText(taskConfig.getUseSqlFlag() != null ? taskConfig.getUseSqlFlag() + "" : "0");
		Element isSlaveTable = root.addElement("isSlaveTable");
		isSlaveTable.setText(taskConfig.getIsSlaveTable() != null ? taskConfig.getIsSlaveTable() + "" : "0");
		Element isOverwrite = root.addElement("isOverwrite");
		isOverwrite.setText(taskConfig.getIsOverwrite() != null ? taskConfig.getIsOverwrite() + "" : "0");

		Element isPhysicalDel = root.addElement("isPhysicalDel");
		isPhysicalDel.setText(taskConfig.getIsPhysicalDel() != null ? taskConfig.getIsPhysicalDel() + "" : "0");
		Element version = root.addElement("version");
		version.setText(taskConfig.getVersion() != null ? taskConfig.getVersion() : "");
		Element subTaskList = root.addElement("subTaskList");
		int index = 0;
		if (taskConfig.getTaskContent() != null) {
			for (Entry<Integer, String> entry : taskConfig.getTaskContent().entrySet()) {
				Element stepContent = subTaskList.addElement("stepContent");
				stepContent.addAttribute("id", index + "");
				stepContent.setText(entry.getValue() != null ? entry.getValue() : "");
				index++;
			}
		}
		// 输出全部原始数据，在编译器中显示
		OutputFormat format = OutputFormat.createPrettyPrint();
		format.setEncoding("utf-8");// 根据需要设置编码
		XMLWriter writer = new XMLWriter(System.out, format);
		document.normalize();
		writer.write(document);
		writer.close();

		// 输出全部原始数据，并用它生成新的我们需要的XML文件
		File file = new File(MessageConstant.LINUX_SQOOP_TASK_XML_PATH);
		if (!file.exists()) {
			file = new File(MessageConstant.WINDOW_SQOOP_TASK_XML_PATH);
		}
		XMLWriter writer2 = new XMLWriter(new FileWriter(new File(file.getAbsolutePath() + File.separatorChar
				+ taskConfig.getGroupName() + "-" + taskConfig.getTriggerName() + ".xml")), format);
		// 输出到文件
		writer2.write(document);
		writer2.close();
	}

	public static void main(String[] args) throws Exception {
		Connection conn = DriverManager.getConnection("jdbc:mysql://172.17.210.180:3306/dc_scheduler_client",
				"dc_scheduler_cli", "dc_scheduler_cli");
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery("select t.* from dc_client_task_metastore t");
		while (rs.next()) {
			TaskPropertiesConfig taskConfig = new TaskPropertiesConfig();
			taskConfig.setId(rs.getInt("id"));
			taskConfig.setTaskType(rs.getInt("task_type"));
			taskConfig.setGroupName(rs.getString("group_name"));
			taskConfig.setTriggerName(rs.getString("trigger_name"));
			taskConfig.setSourceDbId(rs.getInt("source_db_id"));
			taskConfig.setSourceParentTableId(rs.getString("source_parent_table") == null
					|| rs.getString("source_parent_table").equals("0") ? null : rs.getString("source_parent_table"));
			taskConfig.setSourceTable(rs.getString("source_table"));
			String sourceSubTables = rs.getString("dependency_task_ids");
			if (sourceSubTables != null) {
				List<Integer> idList = new ArrayList<Integer>();
				for (String id : sourceSubTables.split(SPLITE_CHAR_1)) {
					idList.add(Integer.parseInt(id));
				}
				taskConfig.setDependencyTaskIds(idList.size() > 0 ? idList : null);
			}
			taskConfig.setRelationColumns(rs.getString("relation_columns") != null ? Arrays.asList(rs.getString(
					"relation_columns").split(SPLITE_CHAR_1)) : null);
			taskConfig.setPrimaryKeys(rs.getString("primary_keys") != null ? Arrays.asList(rs.getString("primary_keys")
					.split(SPLITE_CHAR_1)) : null);
			taskConfig.setSelectColumns(rs.getString("select_columns") != null ? Arrays.asList(rs.getString(
					"select_columns").split(SPLITE_CHAR_1)) : null);
			// 历史表的字段列表，当真实表和历史表不一致时需要配置此字段
			taskConfig.setHisSelectColumns(rs.getString("hisSelect_columns") != null ? Arrays.asList(rs.getString(
					"hisSelect_columns").split(SPLITE_CHAR_1)) : null);

			taskConfig.setSpecialColumnTypeList(rs.getString("special_columns_type") != null ? Arrays.asList(rs
					.getString("special_columns_type").split(SPLITE_CHAR_1)) : null);

			taskConfig.setSyncTimeColumn(rs.getString("sync_time_columns") != null ? Arrays.asList(rs.getString(
					"sync_time_columns").split(SPLITE_CHAR_1)) : null);
			taskConfig.setTargetDbId(rs.getInt("target_db_id"));
			taskConfig.setTargetTable(rs.getString("target_table"));
			taskConfig.setTargetColumns(rs.getString("target_columns") != null ? Arrays.asList(rs.getString(
					"target_columns").split(SPLITE_CHAR_1)) : null);
			taskConfig.setSyncFreqSeconds(rs.getInt("sync_freq_seconds"));
			taskConfig.setUseSqlFlag(rs.getInt("use_sql_flag"));
		    taskConfig.setIsSlaveTable(rs.getInt("is_slave_table"));
			taskConfig.setIsOverwrite(rs.getInt("is_overwrite"));
			taskConfig.setIsPhysicalDel(rs.getInt("is_physical_del"));
			taskConfig.setVersion(rs.getString("version"));
			createFile(taskConfig, new HashMap<String, String>());
		}
		stmt.close();
		conn.close();
	}
}
