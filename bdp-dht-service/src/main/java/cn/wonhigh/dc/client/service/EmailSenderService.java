package cn.wonhigh.dc.client.service;

import java.util.List;

import javax.mail.MessagingException;
import javax.mail.internet.AddressException;

/**
 * 邮件服务类
 * 
 * @author wang.w
 * @date 2015-6-8 下午5:32:34
 * @version 0.5.0 
 * @copyright wonhigh.cn 
 */
public interface EmailSenderService {

	/**
	 * 发送邮件
	 * 
	 * @param recipient
	 *                收件人邮箱地址
	 * @param ccName 抄送人
	 * @param subject
	 *                邮件主题
	 * @param content
	 *                邮件内容
	 * @throws AddressException
	 * @throws MessagingException
	 */
	public void send(String recipient, String ccName, String subject, Object content) throws AddressException,
			MessagingException;

	/**
	 * 群发邮件
	 * 
	 * @param recipients
	 *                收件人们
	 * @param ccNameList 抄送人列表
	 * @param subject
	 *                主题
	 * @param content
	 *                内容
	 * @throws AddressException
	 * @throws MessagingException
	 */
	public void send(List<String> recipients, List<String> ccNameList, String subject, Object content)
			throws AddressException, MessagingException;
}
