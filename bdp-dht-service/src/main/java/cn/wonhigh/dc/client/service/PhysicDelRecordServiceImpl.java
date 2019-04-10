package cn.wonhigh.dc.client.service;

import java.util.List;
import java.util.Map;

import javax.annotation.Resource;

import org.springframework.stereotype.Service;

import cn.wonhigh.dc.client.dal.mapper.PhysicDelRecordMapper;

import com.yougou.logistics.base.common.exception.ServiceException;
import com.yougou.logistics.base.dal.database.BaseCrudMapper;
import com.yougou.logistics.base.service.BaseCrudServiceImpl;

/**
 * 
 * 
 * @author user
 * @date 2016-4-25 下午1:42:03
 * @version 0.5.0 
 * @copyright wonhigh.cn 
 */
@Service
public class PhysicDelRecordServiceImpl extends BaseCrudServiceImpl implements PhysicDelRecordService {

	@Resource
	private PhysicDelRecordMapper physicDelRecordMapper;
	
	@Override
	public BaseCrudMapper init() {
		return physicDelRecordMapper;
	}
	
	@Override
	public <PhysicDelRecord> List<PhysicDelRecord> findByBiz(PhysicDelRecord record, Map<String, Object> paramMap) throws ServiceException {
		return physicDelRecordMapper.selectByParams(record, paramMap);
	}

	@Override
	public <PhysicDelRecord> int modifyById(PhysicDelRecord record) throws ServiceException {
		return physicDelRecordMapper.updateByPrimaryKey(record);
	}

}
