package cn.wonhigh.dc.client.service;

import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import cn.wonhigh.dc.client.common.model.ClientTaskControl;
import cn.wonhigh.dc.client.dal.mapper.ClientTaskControlMapper;

@Service("clientTaskControlService")
public class ClientTaskControlServiceImpl implements ClientTaskControlService {

	@Autowired
	private ClientTaskControlMapper clientTaskControlMapper;

	@Override
	public ClientTaskControl getClientTaskControl(Map<String, Object> params) {
		
		return clientTaskControlMapper.selectByParams(params);
	}
	
	@Transactional(propagation=Propagation.REQUIRED,rollbackFor=Exception.class)
	@Override
	public void updateClientTaskControlByPrimaryKey(
			ClientTaskControl clientTaskControl) {
		
		clientTaskControlMapper.updateByPrimaryKey(clientTaskControl);
	}

	@Override
	public ClientTaskControl getClientTaskControlById(Map<String, Object> params) {
		return clientTaskControlMapper.selectById(params);
	}

}
