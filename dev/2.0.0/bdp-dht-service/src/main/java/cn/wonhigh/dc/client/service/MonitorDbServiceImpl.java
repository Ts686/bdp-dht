package cn.wonhigh.dc.client.service;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
import org.springframework.stereotype.Service;

import cn.wonhigh.dc.client.common.constans.CommonEnumCollection.DbTypeCollecEnum;
import cn.wonhigh.dc.client.common.constans.CommonEnumCollection.DbTypeEnum;
import cn.wonhigh.dc.client.common.model.TaskDatabaseConfig;
import cn.wonhigh.dc.client.common.util.HiveUtils;
import cn.wonhigh.dc.client.common.util.ParseXMLFileUtil;

/**
 * 监控db变化的service实现
 * 
 * @author wang.w
 * @date 2015-3-20 下午12:14:35
 * @version 0.9.1 
 * @copyright yougou.com 
 */
@Service
public class MonitorDbServiceImpl implements MonitorDbService {

	private static final Logger logger = Logger.getLogger(MonitorDbServiceImpl.class);

	/**
	 * 检查所有源库的表结构是否有变更操作
	 */
	@Override
	public List<String> checkAllTablesStructChange(String[] tailStrs, String mappingSplitBy) {
		List<String> resultList = new ArrayList<String>();
		for (String config : tailStrs) {
			String[] dbs = config.split(mappingSplitBy);
			if (compareTableStruct(getTableStruct(Integer.parseInt(dbs[0])), getTableStruct(Integer.parseInt(dbs[1])))) {
				logger.info(String.format("数据库[%]表结构比对完成一致", ParseXMLFileUtil.getDbById(Integer.parseInt(dbs[0]))
						.getUserName()));
			} else {
				logger.error(String.format("数据库[%]表结构比对出现不一致的情况", ParseXMLFileUtil.getDbById(Integer.parseInt(dbs[0]))
						.getUserName()));
				resultList.add(ParseXMLFileUtil.getDbById(Integer.parseInt(dbs[0])).getUserName());
			}
		}
		return resultList;
	}

	/**
	 * 
	 * @param dbId
	 * @return
	 */
	private Map<String, Map<String, DbTypeEnum>> getTableStruct(int dbId) {
		TaskDatabaseConfig dbConfig = ParseXMLFileUtil.getDbById(dbId);

		if (dbConfig.getDbType() == DbTypeCollecEnum.MYSQL.getValue()) {
			//mysql库
			return getMysqlTableStruct(dbConfig, DbTypeCollecEnum.MYSQL);
		} else if (dbConfig.getDbType() == DbTypeCollecEnum.HIVE.getValue()) {
			//hive库
			return getHiveTableStruct(dbConfig, DbTypeCollecEnum.HIVE);
		}

		return null;
	}

	/**
	 * 获取mysql中的表结构
	 * @param dbConfig
	 * @param dbTypeEnum
	 * @return
	 */
	private Map<String, Map<String, DbTypeEnum>> getMysqlTableStruct(TaskDatabaseConfig dbConfig,
			DbTypeCollecEnum dbTypeEnum) {
		try {
			Class.forName(dbTypeEnum.getDriverName());
			Connection connection = DriverManager.getConnection(dbConfig.getConnectionUrl(), dbConfig.getUserName(),
					dbConfig.getPassword());
			//TODO mycat不提供information_schema库

		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * 获取hive中的表结构
	 * @param dbConfig
	 * @param dbTypeEnum
	 * @return
	 */
	private Map<String, Map<String, DbTypeEnum>> getHiveTableStruct(TaskDatabaseConfig dbConfig,
			DbTypeCollecEnum dbTypeEnum) {
		try {
			Connection connection = HiveUtils.getConn(dbConfig.getConnectionUrl(), dbConfig.getUserName(),
					dbConfig.getPassword(),30);
			//TODO 不能每次都hive查询一个表的信息

		} catch (SQLException e) {

		}
		return null;
	}

	/**
	 * 
	 * @param rdbmsTableMap
	 * @param hiveTableMap
	 * @return
	 */
	private boolean compareTableStruct(Map<String, Map<String, DbTypeEnum>> rdbmsTableMap,
			Map<String, Map<String, DbTypeEnum>> hiveTableMap) {
		if (rdbmsTableMap == null || rdbmsTableMap.size() <= 0 || hiveTableMap == null || hiveTableMap.size() <= 0) {
			return false;
		}
		//遍历表--都以源数据库为准
		for (Entry tableEntry : rdbmsTableMap.entrySet()) {
			Map<String, DbTypeEnum> hiveColumnMap = null;
			Map<String, DbTypeEnum> rdbmsColumnMap = (Map<String, DbTypeEnum>) tableEntry.getValue();

			//在hive中找到对应的表
			for (Entry tableEntry1 : hiveTableMap.entrySet()) {
				//对比表名相同
				if (tableEntry1.getKey().equals(tableEntry.getKey())) {
					hiveColumnMap = (Map<String, DbTypeEnum>) tableEntry1.getValue();
					break;
				}
			}

			//只对比hive中存在的表
			if (hiveColumnMap == null) {
				continue;
			}

			//1、对比字段个数是否一致
			if (rdbmsColumnMap.size() != hiveColumnMap.size()) {
				return false;
			}

			//遍历字段列
			for (Entry columnEntry : rdbmsColumnMap.entrySet()) {
				//2、对比字段类型是否一致
				//TODO
			}
		}
		return true;
	}

}
