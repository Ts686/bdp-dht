package cn.wonhigh.dc.client.dal.mapper;

import java.util.List;

import cn.wonhigh.dc.client.common.model.TabDupPrimaryConfig;

import com.yougou.logistics.base.dal.database.BaseCrudMapper;

public interface TabDupPrimaryConfigMapper extends BaseCrudMapper {

	public List<TabDupPrimaryConfig> getTabDupPrimaryConfigs();


}
