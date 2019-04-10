package cn.wonhigh.dc.client.service;

import java.util.Map;

import cn.wonhigh.dc.client.common.model.ClientTaskControl;
public interface ClientTaskControlService {

	public ClientTaskControl getClientTaskControl(Map<String, Object> params);

	public void updateClientTaskControlByPrimaryKey(
			ClientTaskControl clientTaskControl);

	public ClientTaskControl getClientTaskControlById(Map<String, Object> params);

}
