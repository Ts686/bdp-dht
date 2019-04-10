package cn.wonhigh.dc.client.manager.cache;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import cn.wonhigh.dc.client.manager.BaseManagerTest;

/**
 * 缓存单元测试
 * 
 * @author wang.w
 * @date 2015-3-23 上午10:50:58
 * @version 0.5.0 
 * @copyright wonhigh.cn
 */
public class RinseStatusAndLogCacheTest extends BaseManagerTest{

	@Autowired
	private RinseStatusAndLogCache RinseStatusAndLogCache;
	
	@Test
	public void testInitCache(){
		try {
			//在使用前，需要将下面类中对应的方法的重写标识去掉
			RinseStatusAndLogCache.afterPropertiesSet();
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("success");
	}
}
