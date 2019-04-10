package cn.wonhigh.dc.client.service;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import cn.wonhigh.dc.client.common.model.ClientTaskControl;
import cn.wonhigh.dc.client.common.model.ClientTaskStatusLog;
import cn.wonhigh.dc.client.common.util.DateUtils;
import cn.wonhigh.dc.client.common.util.ShellUtils;
import cn.wonhigh.dc.client.common.vo.ShellVo;
import cn.wonhigh.dc.client.service.ClientTaskControlService;
import cn.wonhigh.dc.client.service.ClientTaskStatusLogService;

public class ClientTaskControlServiceTest extends BaseServiceTest {

	@Autowired
	private ClientTaskControlService clientTaskControlService;
	@Autowired
	private ClientTaskStatusLogService clientTaskStatusLogService;

	@Test
	public void testGetClientTaskControl() {
		Map<String, Object> params = new HashMap<String, Object>();
		params.put("triggerName", "sale_assistant");
		params.put("groupName", "dc_retail_pos");
		ClientTaskControl clientTaskControl = clientTaskControlService.getClientTaskControl(params);
		System.out.println(clientTaskControl.getSourceDbUrl() + clientTaskControl.getSchedulerName());
	}

	@Test
	public void testGetClientTaskControlById() {
		Map<String, Object> params = new HashMap<String, Object>();
		params.put("id", 1);
		ClientTaskControl clientTaskControl = clientTaskControlService.getClientTaskControlById(params);
		System.out.println(clientTaskControl.getSourceDbUrl() + clientTaskControl.getSchedulerName());
	}

	@Test
	public void testUpdateClientTaskControl() {
		Map<String, Object> params = new HashMap<String, Object>();
		params.put("triggerName", "sale_assistant");
		params.put("groupName", "dc_retail_pos");
		ClientTaskControl clientTaskControl = clientTaskControlService.getClientTaskControl(params);
		clientTaskControl.setTaskBeginTime(new Date());
		clientTaskControlService.updateClientTaskControlByPrimaryKey(clientTaskControl);
	}

	public void testShellInvoker(ClientTaskControl clientTaskControl) {

		//1.加载数据
		/*
				Map<String, Object> params =  new HashMap<String, Object>();
				String triggerName="brand";
				String groupName="dc_retail_pos";
				params.put("triggerName", triggerName);
				params.put("groupName", groupName);
				ClientTaskControl clientTaskControl=clientTaskControlService.getClientTaskControl(params);
				*/
		//2.拼接shell脚本
		String sourceDbUrl = clientTaskControl.getSourceDbUrl();
		String sourceDbUser = clientTaskControl.getSourceDbUser();
		String sourceDbPass = clientTaskControl.getSourceDbPass();
		String sourceSqlStr_old = clientTaskControl.getSourceSqlStr();
		//String task_begin_time=DateUtils.formatDatetime(clientTaskControl.getTaskBeginTime(), "yyyy-MM-dd HH:mm:ss");
		String sYNC_BEGIN_TIME_Str;
		if (clientTaskControl.getTaskBeginTime() == null) {
			sYNC_BEGIN_TIME_Str = "1970-01-01 00:00:00";
		} else {
			sYNC_BEGIN_TIME_Str = DateUtils.formatDatetime(clientTaskControl.getTaskBeginTime(), "yyyy-MM-dd HH:mm:ss");
		}
		Date sYNC_ENDTIME = new Date();
		String sYNC_ENDTIME_Str = DateUtils.formatDatetime(sYNC_ENDTIME, "yyyy-MM-dd HH:mm:ss");
		String sourceSqlStr_new1 = sourceSqlStr_old.replaceAll("\\$SYNC_BEGIN_TIME", sYNC_BEGIN_TIME_Str);
		String sourceSqlStr_new2 = sourceSqlStr_new1.replaceAll("\\$SYNC_END_TIME", sYNC_ENDTIME_Str);
		String sourceSqlStr_new3 = sourceSqlStr_new2.replaceAll("where", "where \\$CONDITIONS and ");
		String sourceSqlStr_new = sourceSqlStr_new3.replaceAll("'", "\"");
		//String cmd="/usr/local/sqoop-1.4.5/bin/sqoop import --connect "+sourceDbUrl+" --username "+sourceDbUser+" --password "+sourceDbPass+" --query "+"'"+sourceSqlStr_new+"'"+" --split-by t.id "+"-m 1"+" --fields-terminated-by '\\t' --lines-terminated-by '\\n' --hive-import  --hive-table "+ groupName+"."+triggerName+" --null-string 'NULL' --null-non-string 'NULL'  --target-dir /user/hive/warehouse/"+groupName+"/"+triggerName;
		//String cmd="/usr/local/sqoop-1.4.5/bin/sqoop import --connect "+sourceDbUrl+" --username "+sourceDbUser+" --password "+sourceDbPass+" --table "+ clientTaskControl.getSchedulerName() +"-m 5"+" --fields-terminated-by '\\t' --lines-terminated-by '\\n' --hive-overwrite --hive-import  --hive-table "+ clientTaskControl.getGroupName()+"."+clientTaskControl.getSchedulerName()+" --null-string 'NULL' --null-non-string 'NULL'  --target-dir /user/hive/warehouse/"+clientTaskControl.getGroupName()+"/"+clientTaskControl.getSchedulerName();
		String cmd = "/usr/local/sqoop-1.4.5/bin/sqoop import --connect "
				+ sourceDbUrl
				+ " --username "
				+ sourceDbUser
				+ " --password "
				+ sourceDbPass
				+ " --table "
				+ clientTaskControl.getSchedulerName()
				+ " -m 5"
				+ " --fields-terminated-by '\\t' --lines-terminated-by '\\n' --hive-overwrite --hive-import  --hive-table "
				+ clientTaskControl.getSchedulerName()
				+ " --null-string 'NULL' --null-non-string 'NULL'  --target-dir /user/hive/warehouse/"
				+ clientTaskControl.getSchedulerName();
		//sqoop import --connect jdbc:mysql://172.17.210.180:3306/retail_pms --username retail_pms --password retail_pms -table item_sku --hive-overwrite --hive-import --hive-table item_sku -m 5
		//String cmd="/usr/local/sqoop-1.4.5/bin/sqoop import --connect jdbc:mysql://172.17.210.180:3306/retail_mdm --username retail_mdm --password retail_mdm --query 'select * from brand b where $CONDITIONS  limit 100000'  --split-by b.name --fields-terminated-by '\t' --lines-terminated-by '\n' --hive-import --append --create-hive-table --hive-table brand --null-string 'NULL' --null-non-string 'NULL'  --target-dir /user/hive/warehouse/brand";
		//3.执行shell
		String host = "172.17.210.120";
		String user = "root";
		String password = "172.17.210.120_Nnc0i4&1)P72";
		ShellVo shellVo = null;
		try {
			shellVo = ShellUtils.runSSH(host, user, password, cmd);
			System.out.println("标准输出：\n" + shellVo.getStdout());
			System.out.println("错误输出：\n" + shellVo.getStderr());
			System.out.println("返回值：" + shellVo.getStatus());
		} catch (IOException e) {
			e.printStackTrace();
			clientTaskControl.setTaskStatus((short) 9);
		} finally {
			if (shellVo.getStatus() == 0) {
				clientTaskControl.setTaskStatus((short) 0);
				System.out
						.println("------------------------------------------------------------------success-----------------------------------------");
			} else {
				clientTaskControl.setTaskStatus((short) 9);
			}
			clientTaskControl.setTaskBeginTime(sYNC_ENDTIME);
			clientTaskControlService.updateClientTaskControlByPrimaryKey(clientTaskControl);
		}

	}

	@Test
	public void testTaskLocal() {
		//for(int i=289;i<507;i++){
		Map<String, Object> params = new HashMap<String, Object>();
		params.put("id", 38);
		ClientTaskControl clientTaskControl = clientTaskControlService.getClientTaskControlById(params);
		this.testShellInvoker(clientTaskControl);
		//}
	}

	@Test
	public void testClientTaskControl() {
		//for(int i=289;i<507;i++){
		Map<String, Object> params = new HashMap<String, Object>();
		params.put("id", 65);
		ClientTaskControl clientTaskControl = clientTaskControlService.getClientTaskControlById(params);
		System.out.println(clientTaskControl.getSourceMainTable());
		//}
	}

	@Test
	public void testClientTaskLog() {
		ClientTaskStatusLog clientTaskStatusLog = new ClientTaskStatusLog();
		clientTaskStatusLog.setCreateTime(new Date());
		clientTaskStatusLogService.addClientTaskStatusLog(clientTaskStatusLog);
	}

	@Test
	public void testSelectClientTaskLog() {
		Map<String, Object> params = new HashMap<String, Object>();
		params.put("groupName", "g");
		params.put("schedulerName", "s");
		ClientTaskStatusLog clientTaskStatusLog = clientTaskStatusLogService.selectByLatestSystime(params).get(0);
		System.out.println(clientTaskStatusLog.getSyncEndTime());
	}
}
