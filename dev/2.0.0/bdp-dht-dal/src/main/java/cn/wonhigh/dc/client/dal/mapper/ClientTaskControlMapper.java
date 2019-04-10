package cn.wonhigh.dc.client.dal.mapper;

import java.util.Map;

import org.apache.ibatis.annotations.Param;

import cn.wonhigh.dc.client.common.model.ClientTaskControl;

import com.yougou.logistics.base.dal.database.BaseCrudMapper;

public interface ClientTaskControlMapper extends BaseCrudMapper{
	
	public ClientTaskControl selectByParams(@Param("params")Map<String,Object> params);

	public void updateByPrimaryKey(
			ClientTaskControl clientTaskControl);

	public ClientTaskControl selectById(@Param("params")Map<String,Object> params);
}
