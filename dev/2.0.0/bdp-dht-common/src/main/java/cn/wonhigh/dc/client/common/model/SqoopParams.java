package cn.wonhigh.dc.client.common.model;

import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * TODO: 增加描述
 * 
 * @author wangl
 * @date 2015年3月17日 下午7:00:06
 * @version 0.9.1 
 * @copyright wonhigh.com 
 */
public class SqoopParams {
	String command;
    Map<String, String> paras;
    Map<String, String> properties;

	public Map<String, String> getProperties() {
		return properties;
	}

	public void setProperties(Map<String, String> properties) {
		this.properties = properties;
	}

	List<String> options;
    Date syncBeginTime;
    Date syncEndTime;
	public Date getSyncBeginTime() {
		return syncBeginTime;
	}
	public void setSyncBeginTime(Date syncBeginTime) {
		this.syncBeginTime = syncBeginTime;
	}
	public Date getSyncEndTime() {
		return syncEndTime;
	}
	public void setSyncEndTime(Date syncEndTime) {
		this.syncEndTime = syncEndTime;
	}
	public String getCommand() {
		return command;
	}
	public void setCommand(String command) {
		this.command = command;
	}
	public Map<String, String> getParas() {
		return paras;
	}
	public void setParas(Map<String, String> paras) {
		this.paras = paras;
	}
	public List<String> getOptions() {
		return options;
	}
	public void setOptions(List<String> options) {
		this.options = options;
	}
}
