package cn.wonhigh.dc.client.dal.mapper;


import java.util.List;
import java.util.Map;

import org.apache.ibatis.annotations.Param;

import cn.wonhigh.dc.client.common.model.ClientTaskStatusLog;

import com.yougou.logistics.base.dal.database.BaseCrudMapper;

/**
 * 
 * TODO: 任务运行状态日志mapper接口
 * 
 * @author wang.w
 * @date 2015-3-20 上午11:47:28
 * @version 0.5.0 
 * @copyright yougou.com
 */
public interface ClientTaskStatusLogMapper extends BaseCrudMapper{
	
	/**
	 * 插入运行状态日志
	 * @param clientTaskStatusLog
	 */
	public void insertClientTaskStatusLog(ClientTaskStatusLog clientTaskStatusLog);

	/**
	 * 功能：
	 * 1、查找到最新的一条状态日志
	 * 2、查询找到两条不同的最新的状态日志
	 * @param params
	 * @return
	 */
	public List<ClientTaskStatusLog> selectByLatestSystime(@Param("params")Map<String, Object> params);
	
	/**
	 * 根据taskId查询任务的最新状态
	 * @param params
	 * @return
	 */
	public ClientTaskStatusLog selectLastestStatus(@Param("params")Map<String, Object> params);
	
	/**
	 * 查询所有任务的最新状态
	 * 3天内的
	 * @param params
	 * @return
	 */
	public List<ClientTaskStatusLog> selectLastestStatusList(@Param("params")Map<String, Object> params);
	/**
	 * 查询删除修复子任务的修复成功个数
	 * 2天内指定id的
	 * @param params
	 * @return
	 */
	public int selectLastestTwoDaysFin(@Param("params")Map<String, Object> params);
	
	public List<ClientTaskStatusLog> selectByLastEndTime(@Param("params")Map<String, Object> params);
	
}
