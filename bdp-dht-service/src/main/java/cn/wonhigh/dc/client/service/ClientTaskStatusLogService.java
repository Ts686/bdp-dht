package cn.wonhigh.dc.client.service;

import java.util.List;
import java.util.Map;

import org.apache.ibatis.annotations.Param;

import cn.wonhigh.dc.client.common.model.ClientTaskStatusLog;


public interface ClientTaskStatusLogService{

	void addClientTaskStatusLog(ClientTaskStatusLog clientTaskStatusLog);

	List<ClientTaskStatusLog> selectByLatestSystime(Map<String, Object> params);
	
	/**
	 * 
	 * @param params
	 * @return
	 */
	public ClientTaskStatusLog findLastestStatus(Map<String, Object> params);
	
	/**
	 * 
	 * @param params
	 * @return
	 */
	public List<ClientTaskStatusLog> findLastestStatusList(Map<String, Object> params);
	
	public List<ClientTaskStatusLog> selectByLastEndTime(@Param("params")Map<String, Object> params);
	
	public int selectLastestTwoDaysFin(@Param("params")Map<String, Object> params);

}
