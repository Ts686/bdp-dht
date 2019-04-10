package cn.wonhigh.dc.client.dal.mapper;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import cn.wonhigh.dc.client.common.model.PhysicDelRecord;
import cn.wonhigh.dc.client.dal.BaseDalTest;

public class PhysicDelRecordMapperTest extends BaseDalTest{
	
	@Autowired
	private PhysicDelRecordMapper physicDelRecordMapper;
	
	@Test
	public void testUpdate(){
		PhysicDelRecord record = new PhysicDelRecord();
		record.setId(Long.valueOf("1"));
		record.setUpdateTime(new Date());
		physicDelRecordMapper.updateByPrimaryKey(record);
		System.out.println(record.getId());
	}
	
	@Test
	public void testSelect(){
		Map<String, Object> params = new HashMap<String, Object>();
		params.put("sysName", "retail_gms");
		params.put("tableName", "bill_asn");
		PhysicDelRecord record = new PhysicDelRecord();
		List<PhysicDelRecord> physicDelRecordList = physicDelRecordMapper.selectByParams(record, params);
		System.out.println(physicDelRecordList.size());
	}
}
