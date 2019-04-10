package cn.wonhigh.dc.client.manager.jms;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import com.yougou.logistics.base.common.enums.JobBizStatusEnum;

import cn.wonhigh.dc.client.manager.BaseManagerTest;
import cn.wonhigh.dc.client.service.jms.JmsClusterMgr;

/**
 * 测试更新状态，并发送给mq
 * 
 * @author wang.w
 * @date 2015-3-23 上午10:38:59
 * @version 0.5.0 
 * @copyright wonhigh.cn
 */
public class SendMsg2AMQTest extends BaseManagerTest{
	
	@Autowired
	private JmsClusterMgr jmsClusterMgr;
	
	@Test
	public void testUpdateStatusAndSend(){
		String jobId = "123";
		JobBizStatusEnum jobBizStatusEnum = JobBizStatusEnum.FINISHED;
		SendMsg2AMQ.updateStatusAndSend(jobId, jobBizStatusEnum, jmsClusterMgr);
	}
}
