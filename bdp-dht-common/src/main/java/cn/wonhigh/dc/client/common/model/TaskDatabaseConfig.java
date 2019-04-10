package cn.wonhigh.dc.client.common.model;

import java.io.Serializable;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.yougou.logistics.base.common.utils.EncryptionUtils;

import cn.wonhigh.dc.client.common.constans.CommonEnumCollection.DbTypeCollecEnum;
import cn.wonhigh.dc.client.common.constans.MessageConstant;
import cn.wonhigh.dc.client.common.util.PropertyFile;

/**
 * 数据库配置信息
 *
 * @author wang.w
 *
 */
public class TaskDatabaseConfig implements Serializable {

	private static final Logger logger = Logger.getLogger(TaskDatabaseConfig.class);

	/**
	 *
	 */
	private static final long serialVersionUID = 5380426021530624832L;

	/** 主键id */
	private Integer id;

	/** 数据库类型 0.mysql 1.postgresql 2.hive 3.oracle 4.sqlserver 5.sybase */
	private Integer dbType;

	/** 数据库所在的ip地址 */
	private String ipAddr;

	/** 数据库端口 */
	private String port;

	/** 数据库用户名 */
	private String userName;

	/** 数据库密码 */
	private String password;

	/** 数据名称 */
	private String dbName;

	/** 字符集 */
	private String charset;

	/** 版本号 */
	private String version;

	/**
	 * 模式名
	 */
	private String schemaName;

	public Integer getId() {
		return id;
	}

	public void setId(Integer id) {
		this.id = id;
	}

	public Integer getDbType() {
		return dbType;
	}

	public void setDbType(Integer dbType) {
		this.dbType = dbType;
	}

	public String getIpAddr() {
		return ipAddr;
	}

	public void setIpAddr(String ipAddr) {
		this.ipAddr = ipAddr;
	}

	public String getPort() {
		return port;
	}

	public void setPort(String port) {
		this.port = port;
	}

	public String getUserName() {
		return userName;
	}

	public void setUserName(String userName) {
		this.userName = userName;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		// 密码开关打开 on
		// logger.warn(String.format("密码开关【%s】",MessageConstant.PWD_ON_OFF_VALUE));

		if (MessageConstant.PWD_ON_OFF_VALUE) {
			try {
				this.password = EncryptionUtils.blowfishDecode(MessageConstant.PWD_ENCRYP_VALUE, password);
			} catch (Exception e) {
				logger.error("解密失败。。。");
			}
		} else {
			// 密码开关关闭 off
			this.password = password;
		}
	}

	public String getDbName() {
		return dbName;
	}

	public void setDbName(String dbName) {
		this.dbName = dbName;
	}

	public String getCharset() {
		return charset;
	}

	public void setCharset(String charset) {
		this.charset = charset;
	}

	public String getVersion() {
		return version;
	}

	public void setVersion(String version) {
		this.version = version;
	}

	public String getSchemaName() {
		return schemaName;
	}

	public void setSchemaName(String schemaName) {
		this.schemaName = schemaName;
	}

	@Override
	public String toString() {
		return "TaskDatabaseConfig [id=" + id + ", dbType=" + dbType + ", ipAddr=" + ipAddr + ", port=" + port
				+ ", userName=" + userName + ", password=" + password + ", dbName=" + dbName + ", charset=" + charset
				+ ", version=" + version + "]";
	}

	public String getConnectionUrl() {
		String jdbcUrl = null;
		if (dbType == DbTypeCollecEnum.MYSQL.getValue()) {
			jdbcUrl = "jdbc:mysql://" + ipAddr + ":" + port + "/" + dbName + "?tinyInt1isBit=false";
		} else if (dbType == DbTypeCollecEnum.POSTGRESQL.getValue()) {
			jdbcUrl = "jdbc:postgresql://" + ipAddr + ":" + port + "/" + dbName;
		} else if (dbType == DbTypeCollecEnum.HIVE.getValue()) {
			if (StringUtils.isNotBlank(PropertyFile.getValue(MessageConstant.DC_HIVE_PRINCIPAL, null))) {
				jdbcUrl = "jdbc:hive2://" + ipAddr + ":" + port + "/" + dbName + ";principal="
						+ PropertyFile.getValue(MessageConstant.DC_HIVE_PRINCIPAL, "hive/dn012043@WONHIGH.CN");
			} else {
				jdbcUrl = "jdbc:hive2://" + ipAddr + ":" + port + "/" + dbName;
			}
		} else if (dbType == DbTypeCollecEnum.SQLSERVER.getValue()) {
			jdbcUrl = "jdbc:microsoft:sqlserver://" + ipAddr + ":" + port + ";DatabaseName=" + dbName;
		} else if (dbType == DbTypeCollecEnum.SYBASE.getValue()) {
			jdbcUrl = "jdbc:sybase:Tds:" + ipAddr + ":" + port + "/" + dbName;
		} else if (dbType == DbTypeCollecEnum.ORACLE.getValue()) {
			jdbcUrl = "jdbc:oracle:thin:@" + ipAddr + ":" + port + "/" + dbName;
		} else if (dbType == DbTypeCollecEnum.SPARK_HIVE.getValue()) {
			if (StringUtils.isNotBlank(PropertyFile.getValue(MessageConstant.SPARK_HIVE_PRINCIPAL, null))) {
				jdbcUrl = "jdbc:hive2://" + ipAddr + ":" + port + "/" + dbName + ";principal="
						+ PropertyFile.getValue(MessageConstant.SPARK_HIVE_PRINCIPAL, "hive/dn012043@WONHIGH.CN");
			} else {
				jdbcUrl = "jdbc:hive2://" + ipAddr + ":" + port + "/" + dbName;
			}
			// 新增db2 jdbc url
		} else if (dbType == DbTypeCollecEnum.DB2.getValue()) {
			jdbcUrl = "jdbc:db2://" + ipAddr + ":" + port + "/" + dbName;
		}
		return jdbcUrl;
	}

	public String getDriverClassName() {
		String driverClassName = null;
		if (dbType == DbTypeCollecEnum.MYSQL.getValue()) {
			driverClassName = "com.mysql.jdbc.Driver";
		} else if (dbType == DbTypeCollecEnum.POSTGRESQL.getValue()) {
			driverClassName = "org.postgresql.Driver";
		} else if (dbType == DbTypeCollecEnum.HIVE.getValue()) {
			driverClassName = "org.apache.hive.jdbc.HiveDriver";
		} else if (dbType == DbTypeCollecEnum.SQLSERVER.getValue()) {
			driverClassName = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
		} else if (dbType == DbTypeCollecEnum.SYBASE.getValue()) {
			driverClassName = "com.sybase.jdbc4.jdbc.SybDriver";
		} else if (dbType == DbTypeCollecEnum.ORACLE.getValue()) {
			driverClassName = "oracle.jdbc.driver.OracleDriver";
		} else if (dbType == DbTypeCollecEnum.DB2.getValue()) {
			driverClassName = "com.ibm.db2.jcc.DB2Driver";
		}
		return driverClassName;
	}
}
