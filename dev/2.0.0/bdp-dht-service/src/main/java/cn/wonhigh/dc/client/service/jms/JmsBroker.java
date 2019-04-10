package cn.wonhigh.dc.client.service.jms;

import java.util.HashMap;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.MessageListener;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.jms.connection.CachingConnectionFactory;
import org.springframework.jms.core.JmsTemplate;

/**
 * broker entity
 * @author wang.w
 *
 */
public class JmsBroker implements InitializingBean {

	private CachingConnectionFactory connectionFactory;

	private JmsTemplate jmsTemplate;

	/** 是否正常工作 */
	private boolean normalWork;

	// for listener
	private Map<String, Connection> connMap;
	
	private Map<String, MessageListener> messageListenerMap;
	
	private Map<String, String> durableTopicClientIdMap;
	
	public JmsBroker() {
		this.connMap = new HashMap<String, Connection>(0);
		this.messageListenerMap = new HashMap<String, MessageListener>(0);
		this.durableTopicClientIdMap = new HashMap<String, String>(0);
	}

	public CachingConnectionFactory getConnectionFactory() {
		return connectionFactory;
	}

	public void setConnectionFactory(CachingConnectionFactory connectionFactory) {
		this.connectionFactory = connectionFactory;
	}

	public JmsTemplate getJmsTemplate() {
		return jmsTemplate;
	}

	public void setJmsTemplate(JmsTemplate jmsTemplate) {
		this.jmsTemplate = jmsTemplate;
	}

	public boolean isNormalWork() {
		return normalWork;
	}

	public void setNormalWork(boolean normalWork) {
		this.normalWork = normalWork;
	}

	public Map<String, Connection> getConnMap() {
		return connMap;
	}

	public void setConnMap(Map<String, Connection> connMap) {
		this.connMap = connMap;
	}

	public Map<String, MessageListener> getMessageListenerMap() {
		return messageListenerMap;
	}

	public void setMessageListenerMap(
			Map<String, MessageListener> messageListenerMap) {
		this.messageListenerMap = messageListenerMap;
	}

	public Map<String, String> getDurableTopicClientIdMap() {
		return durableTopicClientIdMap;
	}

	public void setDurableTopicClientIdMap(
			Map<String, String> durableTopicClientIdMap) {
		this.durableTopicClientIdMap = durableTopicClientIdMap;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		try {
			connectionFactory = (CachingConnectionFactory) this.getJmsTemplate().getConnectionFactory();
		} catch (Exception e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
	}

}
