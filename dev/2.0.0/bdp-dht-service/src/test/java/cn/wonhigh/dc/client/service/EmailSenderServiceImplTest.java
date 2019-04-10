package cn.wonhigh.dc.client.service;

import static org.junit.Assert.*;

import javax.mail.MessagingException;
import javax.mail.internet.AddressException;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * 邮件发送测试类
 * 
 * @author wang.w
 * @date 2015-6-8 下午6:58:53
 * @version 0.5.0 
 * @copyright wonhigh.cn 
 */
public class EmailSenderServiceImplTest extends BaseServiceTest{

	@Autowired
	private EmailSenderService emailSenderService;
	
	public void setEmailSenderService(EmailSenderService emailSenderService) {
		this.emailSenderService = emailSenderService;
	}

	@Test
	public void testSendStringStringObject() {
		try {
			emailSenderService.send("wang.w@wonhigh.cn","jiang.pl@wonhigh.cn", "test", "test");
		} catch (AddressException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (MessagingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Test
	public void testSendListOfStringStringObject() {
		fail("Not yet implemented");
	}

}
