package cn.wonhigh.dc.client.service;

import java.util.List;

/**
 * 监控db变化的service
 * 
 * @author wang.w
 * @date 2015-3-20 下午12:13:13
 * @version 0.5.0 
 * @copyright wonhigh.cn
 */
public interface MonitorDbService {

	public List<String> checkAllTablesStructChange(String[] tailStrs, String mappingSplitBy);
}
