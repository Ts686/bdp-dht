package cn.wonhigh.dc.client.service;

import java.io.UnsupportedEncodingException;
import java.util.List;

import javax.mail.MessagingException;
import javax.mail.internet.AddressException;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.JavaMailSenderImpl;
import org.springframework.mail.javamail.MimeMessageHelper;
import org.springframework.stereotype.Service;

/**
 * 邮件发送服务类
 * 
 * @author wang.w
 * @date 2015-6-8 下午5:34:10
 * @version 0.5.0 
 * @copyright wonhigh.cn 
 */
@Service
public class EmailSenderServiceImpl implements EmailSenderService {

	@Autowired(required = true)
	private JavaMailSender javaMailSender;

	@Override
	public void send(String recipient, String ccName, String subject, Object content) throws AddressException,
			MessagingException {

		JavaMailSenderImpl javaMailSenderImpl = (JavaMailSenderImpl) javaMailSender;
		MimeMessage mime = javaMailSender.createMimeMessage();
		MimeMessageHelper helper = new MimeMessageHelper(mime, false, "UTF-8");
		// 发件人，不填会报501错误 
		helper.setFrom(javaMailSenderImpl.getUsername());
		helper.setTo(new InternetAddress(recipient));
		helper.setCc(new InternetAddress(ccName));
		helper.setSubject(subject);
		helper.setText(content.toString());
		javaMailSender.send(mime);
	}

	@Override
	public void send(List<String> recipients, List<String> ccNameList, String subject, Object content)
			throws AddressException, MessagingException {
		JavaMailSenderImpl javaMailSenderImpl = (JavaMailSenderImpl) javaMailSender;
		MimeMessage mime = javaMailSender.createMimeMessage();
		MimeMessageHelper helper = new MimeMessageHelper(mime, false, "gbk");
		// 发件人，不填会报501错误 
		helper.setFrom(javaMailSenderImpl.getUsername());

		//主接收人
		InternetAddress[] mainArray = new InternetAddress[recipients.size()];
		int i = 0;
		for (String name : recipients) {
			mainArray[i++] = new InternetAddress(name);
		}
		if (mainArray.length <= 0) {
			return;
		}
		helper.setTo(mainArray);

		if (ccNameList != null && ccNameList.size() > 0) {
			i = 0;
			InternetAddress[] copyArray = new InternetAddress[ccNameList.size()];
			for (String name : ccNameList) {
				copyArray[i++] = new InternetAddress(name);
			}
			//抄送
			helper.setCc(copyArray);
		}
		helper.setSubject(subject);
		helper.setText(content.toString(),true); 
		javaMailSender.send(mime);
	}

}
