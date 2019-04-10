package cn.wonhigh.dc.client.common.util;

import org.junit.Test;

import com.yougou.logistics.base.common.exception.ManagerException;

import junit.framework.TestCase;

/**
 * 加载xml任务配置信息
 * 
 * @author wang.w
 * @date 2015-3-23 上午10:27:41
 * @version 0.5.0 
 * @copyright wonhigh.cn
 */
public class ParseXMLFileUtilTest extends TestCase {

	@Test
	public void testLoadTask() {
		try {
			ParseXMLFileUtil.initTask();
		} catch (ManagerException e) {
			e.printStackTrace();
		}
	}
}
