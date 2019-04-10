package cn.wonhigh.dc.client.common.util;

import java.io.File;
import java.io.FileWriter;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.XMLWriter;
import cn.wonhigh.dc.client.common.constans.MessageConstant;
import cn.wonhigh.dc.client.common.model.TaskProPropertiesConfig;

/**
 * 创建存储过程xml文件
 * 
 * @author jiang.pl
 * 
 */
public class CreateProTaskConfigXMLUtil implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 469183807603996194L;

	public static final String SPLITE_CHAR_1 = ",";
	public static final String SPLITE_CHAR_2 = ":";
	public static final String SPLITE_CHAR_3 = "=";
	public static final String SPLITE_CHAR_4 = "@";

	public static void createFile(TaskProPropertiesConfig taskConfig, Map<String, String> paramsMap) throws Exception {
		// 创建Document
		Document document = DocumentHelper.createDocument();
		// 创建根root
		Element root = document.addElement("root");
		root.addComment(paramsMap.get("comments") != null ? paramsMap.get("comments") : "This is a config of "
				+ taskConfig.getGroupName() + "." + taskConfig.getPrcedurename());
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
		 
		Element proceduname = root.addElement("proceduname");
		proceduname.setText(taskConfig.getPrcedurename() != null ? taskConfig.getPrcedurename() : "");
		 
		Element syncFreqSeconds = root.addElement("syncFreqSeconds");
		syncFreqSeconds.setText(taskConfig.getSyncFreqSeconds() != null ? taskConfig.getSyncFreqSeconds() + "" : "");
		
		Element isreturn = root.addElement("isreturn");
		isreturn.setText(taskConfig.getIsreturn() != null ? taskConfig.getIsreturn() + "" : "0");
		
		Element returntype = root.addElement("returntype");
		returntype.setText(taskConfig.getReturntype() != null ? taskConfig.getReturntype() + "" : "int"); 
		
		Element isPhysicalDel = root.addElement("isPhysicalDel");
		isPhysicalDel.setText(taskConfig.getIsPhysicalDel() != null ? taskConfig.getIsPhysicalDel() + "" : "0");
		
		Element version = root.addElement("version");
		version.setText(taskConfig.getVersion() != null ? taskConfig.getVersion() : "");
		// 输出全部原始数据，在编译器中显示
		OutputFormat format = OutputFormat.createPrettyPrint();
		format.setEncoding("utf-8");// 根据需要设置编码
		XMLWriter writer = new XMLWriter(System.out, format);
		document.normalize();
		writer.write(document);
		writer.close();

		// 输出全部原始数据，并用它生成新的我们需要的XML文件
		File file = new File(MessageConstant.LINUX_SQOOP_PRO_TASK_XML_PATH);
		if (!file.exists()) {
			file = new File(MessageConstant.WINDOW_SQOOP_PRO_TASK_XML_PATH);
		}
		XMLWriter writer2 = new XMLWriter(new FileWriter(new File(file.getAbsolutePath() + File.separatorChar
				+ taskConfig.getGroupName() + "-" + taskConfig.getTriggerName() + "-pro"+".xml")), format);
		// 输出到文件
		writer2.write(document);
		writer2.close();
	}

	public static void main(String[] args) throws Exception {
		Connection conn = DriverManager.getConnection("jdbc:mysql://172.17.210.180:3306/dc_scheduler_client",
				"dc_scheduler_cli", "dc_scheduler_cli");
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery("select t.* from dc_client_pro_task_metastore t");
		while (rs.next()) {
			TaskProPropertiesConfig taskConfig = new TaskProPropertiesConfig();
			taskConfig.setId(rs.getInt("id"));
			taskConfig.setTaskType(rs.getInt("task_type"));
			taskConfig.setGroupName(rs.getString("group_name"));
			taskConfig.setTriggerName(rs.getString("trigger_name"));
			taskConfig.setSourceDbId(rs.getInt("source_db_id"));
			taskConfig.setPrcedurename(rs.getString("prcedurename"));
			taskConfig.setSyncFreqSeconds(rs.getInt("syncFreqSeconds"));
			taskConfig.setIsreturn(rs.getInt("isreturn"));
			taskConfig.setIsPhysicalDel(rs.getInt("is_physical_del"));
			taskConfig.setVersion(rs.getString("version"));
			createFile(taskConfig, new HashMap<String, String>());
		}
		stmt.close();
		conn.close();
	}
}
