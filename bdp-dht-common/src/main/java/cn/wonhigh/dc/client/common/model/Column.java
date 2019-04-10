package cn.wonhigh.dc.client.common.model;

/**
 * TODO: 增加描述
 * 
 * @author wangl
 * @date 2015年11月4日 下午4:48:08
 * @version 0.5.0 
 * @copyright wonhigh.cn 
 */
public class Column {

	private String columnName;
	private String columnType;
	private String columnComment;
	private String isNullable;
	public String getIsNullable() {
		return isNullable;
	}
	public void setIsNullable(String isNullable) {
		this.isNullable = isNullable;
	}
	public String getColumnName() {
		return columnName;
	}
	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}
	public String getColumnType() {
		return columnType;
	}
	public void setColumnType(String columnType) {
		this.columnType = columnType;
	}
	public String getColumnComment() {
		return columnComment;
	}
	public void setColumnComment(String columnComment) {
		this.columnComment = columnComment;
	}
}
