package cn.wonhigh.dc.client.service;


import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import cn.wonhigh.dc.client.common.model.ClientTaskStatusLog;
import cn.wonhigh.dc.client.dal.mapper.ClientTaskStatusLogMapper;


@Service("clientTaskStatusLogService")
public class ClientTaskStatusLogServiceImpl implements ClientTaskStatusLogService {

	@Autowired
	private ClientTaskStatusLogMapper clientTaskStatusLogMapper;

	@Transactional(propagation=Propagation.REQUIRED,rollbackFor=Exception.class)
	@Override
	public void addClientTaskStatusLog(ClientTaskStatusLog clientTaskStatusLog) {
	
		clientTaskStatusLogMapper.insertClientTaskStatusLog(clientTaskStatusLog);
	}

	@Override
	public List<ClientTaskStatusLog> selectByLatestSystime(Map<String, Object> params) {
		return clientTaskStatusLogMapper.selectByLatestSystime(params);
		
	}

	@Override
	public ClientTaskStatusLog findLastestStatus(Map<String, Object> params) {
		
		return clientTaskStatusLogMapper.selectLastestStatus(params);
	}

	@Override
	public List<ClientTaskStatusLog> findLastestStatusList(
			Map<String, Object> params) {
		
		return clientTaskStatusLogMapper.selectLastestStatusList(params);
	}

	@Override
	public List<ClientTaskStatusLog> selectByLastEndTime(Map<String, Object> params) {
		return clientTaskStatusLogMapper.selectByLastEndTime(params);
	}

	@Override
	public int selectLastestTwoDaysFin(Map<String, Object> params) {
		return clientTaskStatusLogMapper.selectLastestTwoDaysFin(params);
	}




}
