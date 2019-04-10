package cn.wonhigh.dc.client.manager.task;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import cn.wonhigh.dc.client.manager.BaseManagerTest;

/**
 * 心跳单元测试
 * 
 * @author wang.w
 * @date 2015-3-23 上午10:46:09
 * @version 0.5.0 
 * @copyright wonhigh.cn
 */
public class UploadStatusTaskTest extends BaseManagerTest {
	
	@Autowired
	private UploadStatusTask uploadStatusTask;
	
	@Test
	public void testHeartBeat() {
		uploadStatusTask.heartBeat();
	}
}
