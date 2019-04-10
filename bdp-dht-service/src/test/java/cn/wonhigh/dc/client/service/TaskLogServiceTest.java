package cn.wonhigh.dc.client.service;

import java.util.Date;
import java.util.List;

import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import cn.wonhigh.dc.client.common.model.TaskLog;
import cn.wonhigh.dc.client.service.TaskLogService;

import com.yougou.logistics.base.common.enums.JobBizStatusEnum;

public class TaskLogServiceTest extends BaseServiceTest {

	@Autowired
	private TaskLogService taskLogService;

	@Test
	public void testFindTaskLog() throws Exception {
		List<TaskLog> list = taskLogService.findTaskLog("teacher", "7002",
				new Date());
		ObjectMapper mapper = new ObjectMapper();
		for (TaskLog log : list) {
			System.out.println(mapper.writeValueAsString(log));
		}
	}

	@Test
	public void testGetTaskStatus() {
		JobBizStatusEnum bizStatus = taskLogService.getTaskStatus("",
				"ShellInvoker", "7002");
		System.out.println(bizStatus);
	}
}
