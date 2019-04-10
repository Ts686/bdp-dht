package cn.wonhigh.dc.client.common.model;

import java.io.Serializable;
import java.util.Date;

/**
 * TODO: 增加描述
 * 
 * @author wangl
 * @date 2016年1月19日 上午10:35:34
 * @version 0.9.9 
 * @copyright wonhigh.cn 
 */
public class DelDataHandler implements Serializable{
  Date notProcessMinTime;
  Date notProcessMaxTime;
  String tbName;
  String delDataTbName;
  public String getDelDataTbName() {
	return delDataTbName;
}
public void setDelDataTbName(String delDataTbName) {
	this.delDataTbName = delDataTbName;
}
TaskPropertiesConfig taskConfig;
public String getTbName() {
	return tbName;
}
public void setTbName(String tbName) {
	this.tbName = tbName;
}
public Date getNotProcessMinTime() {
	return notProcessMinTime;
}
public void setNotProcessMinTime(Date notProcessMinTime) {
	this.notProcessMinTime = notProcessMinTime;
}
public Date getNotProcessMaxTime() {
	return notProcessMaxTime;
}
public void setNotProcessMaxTime(Date notProcessMaxTime) {
	this.notProcessMaxTime = notProcessMaxTime;
}
public TaskPropertiesConfig getTaskConfig() {
	return taskConfig;
}
public void setTaskConfig(TaskPropertiesConfig taskConfig) {
	this.taskConfig = taskConfig;
}
  
}
