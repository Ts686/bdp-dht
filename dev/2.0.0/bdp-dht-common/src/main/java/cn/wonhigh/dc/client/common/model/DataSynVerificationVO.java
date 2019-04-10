package cn.wonhigh.dc.client.common.model;

/**
 * 同步数据质量核查VO
 * @author zhang.rq
 * @since 2016-07-11
 */
public class DataSynVerificationVO {
	/**
	 * 组名
	 */
	private String groupName;
	
	/**
	 * 任务名
	 */
	private String taskName;
	
	/**
	 * 同步日
	 */
	private String synDate;
	
	/**
	 * 业务表数量
	 */
	private int sourceTableCount;
	
	/**
	 * hive表数量
	 */
	private int hiveTableCount;
	
	/**
	 * 数据差异量
	 */
	private int diffCount;
	
	/**
	 * 阀值
	 */
	private double standardDeviation;
	
	/**
	 * 是否同步差异   1:存在差异   0：不存在差异
	 */
	private int isSyn;

	public String getGroupName() {
		return groupName;
	}

	public void setGroupName(String groupName) {
		this.groupName = groupName;
	}

	public String getTaskName() {
		return taskName;
	}

	public void setTaskName(String taskName) {
		this.taskName = taskName;
	}

	public String getSynDate() {
		return synDate;
	}

	public void setSynDate(String synDate) {
		this.synDate = synDate;
	}

	public int getSourceTableCount() {
		return sourceTableCount;
	}

	public void setSourceTableCount(int sourceTableCount) {
		this.sourceTableCount = sourceTableCount;
	}

	public int getHiveTableCount() {
		return hiveTableCount;
	}

	public void setHiveTableCount(int hiveTableCount) {
		this.hiveTableCount = hiveTableCount;
	}

	public int getDiffCount() {
		return diffCount;
	}

	public void setDiffCount(int diffCount) {
		this.diffCount = diffCount;
	}

	public double getStandardDeviation() {
		return standardDeviation;
	}

	public void setStandardDeviation(double standardDeviation) {
		this.standardDeviation = standardDeviation;
	}

	public int getIsSyn() {
		return isSyn;
	}

	public void setIsSyn(int isSyn) {
		this.isSyn = isSyn;
	}
	
}
