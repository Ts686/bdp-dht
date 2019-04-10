package cn.wonhigh.dc.client.service;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import cn.wonhigh.dc.client.common.model.ClientTaskStatusLog;
import cn.wonhigh.dc.client.service.ClientTaskStatusLogService;

public class ClientTaskStatusLogTest extends BaseServiceTest{
	
	@Autowired
	private ClientTaskStatusLogService clientTaskStatusLogService;
	
	@Test
	public void testSelectByLatestSystime(){
		Map<String, Object> params = new HashMap<String, Object>();
		params.put("groupName", "dc_retail_pms_hive");
		params.put("schedulerName", "meetorder_staff");
		ClientTaskStatusLog taskEntity = clientTaskStatusLogService
				.selectByLatestSystime(params).get(0);
		System.out.println(taskEntity);
	}
	
	@Test
	public void testSelectLastestTwoDaysFin(){
		Map<String, Object> params = new HashMap<String, Object>();
		params.put("handleId", "2c9152635253537601525d1393a96e1a");
		int count = clientTaskStatusLogService
				.selectLastestTwoDaysFin(params);
		System.out.println("---------------------------"+count);
	}
	
	@Test
	public void testAddClientTaskStatusLog(){
		ClientTaskStatusLog clientTaskStatusLog = new ClientTaskStatusLog();
		clientTaskStatusLog.setTaskStatus("初始化中");
		clientTaskStatusLog.setTaskStatusDesc("初始化中");
		clientTaskStatusLog.setGroupName("dc_retail_pms_hive");
		clientTaskStatusLog.setSchedulerName("meetorder_staff");
		clientTaskStatusLog.setCreateTime(new Date());
		clientTaskStatusLog.setTaskId("1");
		clientTaskStatusLogService
				.addClientTaskStatusLog(clientTaskStatusLog);
	}
}
