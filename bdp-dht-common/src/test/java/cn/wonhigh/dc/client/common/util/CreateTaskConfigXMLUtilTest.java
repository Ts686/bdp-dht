package cn.wonhigh.dc.client.common.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.junit.Test;

import cn.wonhigh.dc.client.common.model.TaskPropertiesConfig;
import junit.framework.TestCase;

/**
 * 创建人物信息的xml单元测试
 * 
 * @author wang.w
 * @date 2015-3-23 上午10:18:13
 * @version 0.5.0 
 * @copyright wonhigh.cn 
 */
public class CreateTaskConfigXMLUtilTest extends TestCase {

	public static final String SPLITE_CHAR_1 = ",";
	public static final String SPLITE_CHAR_2 = ":";
	public static final String SPLITE_CHAR_3 = "=";
	public static final String SPLITE_CHAR_4 = "@";

	@Test
	public void testCreateTaskFile() {
		try {
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
				taskConfig
						.setSourceParentTableId(rs.getString("source_parent_table") == null
								|| rs.getString("source_parent_table").equals("0") ? null : rs
								.getString("source_parent_table"));
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
				taskConfig.setPrimaryKeys(rs.getString("primary_keys") != null ? Arrays.asList(rs.getString(
						"primary_keys").split(SPLITE_CHAR_1)) : null);
				taskConfig.setSelectColumns(rs.getString("select_columns") != null ? Arrays.asList(rs.getString(
						"select_columns").split(SPLITE_CHAR_1)) : null);
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
				// taskConfig.setIsSlaveTable(rs.getInt("is_slave_table"));
				taskConfig.setIsOverwrite(rs.getInt("is_overwrite"));
				taskConfig.setIsPhysicalDel(rs.getInt("is_physical_del"));
				taskConfig.setVersion(rs.getString("version"));
				CreateTaskConfigXMLUtil.createFile(taskConfig, new HashMap<String, String>());
			}
			stmt.close();
			conn.close();
		} catch (NumberFormatException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
