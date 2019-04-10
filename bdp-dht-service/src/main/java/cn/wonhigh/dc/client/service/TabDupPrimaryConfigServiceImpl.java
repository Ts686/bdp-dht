package cn.wonhigh.dc.client.service;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import cn.wonhigh.dc.client.common.model.TabDupPrimaryConfig;
import cn.wonhigh.dc.client.dal.mapper.TabDupPrimaryConfigMapper;

@Service("tabDupPrimaryConfigService")
public class TabDupPrimaryConfigServiceImpl implements TabDupPrimaryConfigService {

	@Autowired
	private TabDupPrimaryConfigMapper tabDupPrimaryConfigMapper;

	@Override
	public List<TabDupPrimaryConfig> getTabDupPrimaryConfigs() {
		return tabDupPrimaryConfigMapper.getTabDupPrimaryConfigs();
	}


}
