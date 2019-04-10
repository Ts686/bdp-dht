package cn.wonhigh.dc.client.dal.mapper;

import java.util.Date;
import java.util.List;

import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import com.yougou.logistics.base.common.enums.JobBizStatusEnum;

import cn.wonhigh.dc.client.common.model.ClientTaskStatusLog;
import cn.wonhigh.dc.client.common.model.TaskLog;
import cn.wonhigh.dc.client.dal.BaseDalTest;
import cn.wonhigh.dc.client.dal.mapper.ClientTaskStatusLogMapper;
import cn.wonhigh.dc.client.dal.mapper.TaskLogMapper;

public class TaskLogMapperTest extends BaseDalTest {

	@Autowired
	private TaskLogMapper taskLogMapper;

	@Autowired
	private ClientTaskStatusLogMapper clientTaskStatusLogMapper;

	@Test
	public void selectSome() {
		List<TaskLog> list = taskLogMapper.selectByCreateTime("teacher",
				"7002", new Date());
		ObjectMapper mapper = new ObjectMapper();
		for (TaskLog taskLog : list) {
			try {
				System.out.println(mapper.writeValueAsString(taskLog));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	@Test
	public void getTaskStatus() {
		Integer task_status = taskLogMapper
				.getTaskStatus("", "teacher", "7002");
		System.out.println(task_status == null ? JobBizStatusEnum.INITIALIZING
				: task_status);
	}

	@Test
	public void addClientTaskStatusLog() {
		ClientTaskStatusLog clientTaskstatusLog = new ClientTaskStatusLog();
		clientTaskstatusLog.setCreateTime(new Date());
		clientTaskstatusLog.setGroupName("mps");
		clientTaskstatusLog.setSchedulerName("brand");
		clientTaskstatusLog.setTaskStatus("中断");
		clientTaskstatusLog.setTaskStatusDesc("异常中断。。。");
		clientTaskStatusLogMapper
				.insertClientTaskStatusLog(clientTaskstatusLog);
	}

}
