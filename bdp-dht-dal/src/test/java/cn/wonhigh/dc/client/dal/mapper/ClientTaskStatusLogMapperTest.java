package cn.wonhigh.dc.client.dal.mapper;

import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import com.yougou.logistics.base.common.enums.JobBizStatusEnum;

import cn.wonhigh.dc.client.common.model.ClientTaskStatusLog;
import cn.wonhigh.dc.client.dal.BaseDalTest;
import cn.wonhigh.dc.client.dal.mapper.ClientTaskStatusLogMapper;

public class ClientTaskStatusLogMapperTest extends BaseDalTest{
	
	@Autowired
	private ClientTaskStatusLogMapper clientTaskStatusLogMapper;
	
	@Test
	public void testInsert(){
		ClientTaskStatusLog clientTaskStatusLog = new ClientTaskStatusLog();
		clientTaskStatusLog.setTaskId("aaaaaaaaa");
		clientTaskStatusLog.setGroupName("dcd");
		clientTaskStatusLogMapper.insertClientTaskStatusLog(clientTaskStatusLog);
		System.out.println(clientTaskStatusLog.getId());
	}
	
	@Test
	public void testSelectByLatestSystime(){
		Map<String, Object> params = new HashMap<String, Object>();
		params.put("groupName", "dc_retail_mdm");
		params.put("schedulerName", "brand");
		params.put("taskStatus", JobBizStatusEnum.FINISHED.name());
//		params.put("depenSchedulerName",null);
//		params.put("depenGroupName",null);
		params.put("depenSchedulerName","bill_asn_cln");
		params.put("depenGroupName","dc_retail_gms");
		Date date = new Date(System.currentTimeMillis()-1000*24*60*60*2);
		params.put("startSelectDate", date);
		List<ClientTaskStatusLog> clientTaskStatusLogList = clientTaskStatusLogMapper.selectByLatestSystime(params);
		System.out.println(clientTaskStatusLogList.size());
	}
}
