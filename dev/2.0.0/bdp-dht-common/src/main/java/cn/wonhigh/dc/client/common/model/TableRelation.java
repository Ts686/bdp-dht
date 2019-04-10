package cn.wonhigh.dc.client.common.model;

/**
 * TODO: 增加描述
 * 
 * @author wangl
 * @date 2015年12月23日 下午1:17:49
 * @version 0.10.0 
 * @copyright wonhigh.cn 
 */
public class TableRelation {
	private String parentTableName;
	private String slaveTableName;
	private String parentRelationColmName;
	private String slaveRelationColmName;
	public String getParentTableName() {
		return parentTableName;
	}
	public void setParentTableName(String parentTableName) {
		this.parentTableName = parentTableName;
	}
	public String getSlaveTableName() {
		return slaveTableName;
	}
	public void setSlaveTableName(String slaveTableName) {
		this.slaveTableName = slaveTableName;
	}
	public String getParentRelationColmName() {
		return parentRelationColmName;
	}
	public void setParentRelationColmName(String parentRelationColmName) {
		this.parentRelationColmName = parentRelationColmName;
	}
	public String getSlaveRelationColmName() {
		return slaveRelationColmName;
	}
	public void setSlaveRelationColmName(String slaveRelationColmName) {
		this.slaveRelationColmName = slaveRelationColmName;
	}
}
