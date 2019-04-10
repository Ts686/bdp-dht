package cn.wonhigh.dc.client.service.jms;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.annotation.PostConstruct;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.QueueBrowser;
import javax.jms.Session;
import javax.jms.TemporaryQueue;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.log4j.Logger;
import org.springframework.jms.JmsException;
import org.springframework.jms.core.BrowserCallback;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.core.ProducerCallback;

/**
 * manager for cluster
 * 
 * @author wang.w
 * 
 */
public class JmsClusterMgr {

	private static final Logger logger = Logger.getLogger(JmsClusterMgr.class);

	private int brokerNum;

	private List<JmsBroker> jmsBrokerList;

	/**
	 * 当前不支持集群环境. 0.9.2
	 */
	private boolean isCluster = false;

	@PostConstruct
	public void init() {
		brokerNum = jmsBrokerList.size();
	}

	/**
	 * 随机获取集群中一个broker,返回null表示集群中无可用broker
	 * 
	 * @return
	 */
	public JmsBroker getJmsBroker() {
		JmsBroker jmsBroker = null;
		int random = (int) (Math.random() * 10);
		jmsBroker = jmsBrokerList.get(random % jmsBrokerList.size());
		if (jmsBroker.isNormalWork()) {
			return jmsBroker;
		}
		// 随机选中直失败后，按顺序选择第一个可用的broker
		for (int i = 0; i < jmsBrokerList.size(); i++) {
			jmsBroker = jmsBrokerList.get(i);
			if (jmsBroker.isNormalWork()) {
				return jmsBroker;
			}
		}
		// 整个集群崩溃，返回null
		return null;
	}

	/**
	 * 发送topic消息
	 * 
	 * @param destinationName
	 *            目标队列名
	 * @param content
	 *            内容
	 * @param msgPropertyMap
	 */
	public void sendTopicMsg(final String destinationName,
			final String content, final Map<String, Object> msgPropertyMap) {
		JmsBroker jmsBroker = null;
		for (int i = 0; i < brokerNum; i++) {
			try {
				jmsBroker = getJmsBroker();
				if (jmsBroker == null) {
					throw new RuntimeException("无法连接集群中任意的broker,无法发送消息..");
				}
				ActiveMQTopic topic = new ActiveMQTopic(destinationName);
				jmsBroker.getJmsTemplate().execute(topic,
						new ProducerCallback<Object>() {

							@Override
							public Object doInJms(Session session,
									MessageProducer producer)
									throws JMSException {
								Message message = session
										.createTextMessage(content);
								if (msgPropertyMap != null
										&& !msgPropertyMap.isEmpty()) {
									// 如果有，处理消息属性
									Iterator<Entry<String, Object>> iter = msgPropertyMap
											.entrySet().iterator();
									while (iter.hasNext()) {
										Map.Entry<String, Object> entry = (Map.Entry<String, Object>) iter
												.next();
										if (entry.getValue() instanceof java.lang.String) {
											message.setStringProperty(
													entry.getKey(),
													(String) entry.getValue());
										} else if (entry.getValue() instanceof java.lang.Long) {
											message.setLongProperty(
													entry.getKey(),
													(Long) entry.getValue());
										} else if (entry.getValue() instanceof java.lang.Boolean) {
											message.setBooleanProperty(
													entry.getKey(),
													(Boolean) entry.getValue());
										} else if (entry.getValue() instanceof java.lang.Double) {
											message.setDoubleProperty(
													entry.getKey(),
													(Double) entry.getValue());
										} else if (entry.getValue() instanceof java.lang.Integer) {
											message.setIntProperty(
													entry.getKey(),
													(Integer) entry.getValue());
										}
									}
								}
								// 设置非持久特性
								producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
								producer.send(message);

								System.out.println("发送topic:" + msgPropertyMap);

								return session;
							}
						});
				break;
			} catch (JmsException e) {
				e.printStackTrace();
				if (isCluster) {
					// 如果是集群的环境，当发生jms异常则将此broker置为无效状态
					synchronized (jmsBroker) {
						jmsBroker.setNormalWork(false);
					}
					// 扔出线程,不断检测损坏的节点,如果修复,即时修改状态,便于后续加入集群中
					new SenderResumeThread(jmsBroker).start();
				}
				throw e;
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * 发送消息至指定队列
	 * 
	 * @param message
	 *            消息
	 * @param destination
	 *            目的地
	 * @param persitent
	 *            true:持久化,false:非持久化
	 * @param msgPropertyMap
	 *            消息头设置
	 * @param expiredTime
	 *            消息过期时间
	 * @param isReply
	 *            是否创建临时队列
	 * @return
	 */
	private Object sendMessage(final Object content,
			final Destination destination, final boolean persitent,
			final Map<String, Object> msgPropertyMap, final long expiredTime,
			final boolean isReply) {
		JmsBroker jmsBroker = null;
		Object target = null;
		// 找到集群中的第一个有效的broker进行发送即可
		for (int i = 0; i < brokerNum; i++) {
			try {
				jmsBroker = getJmsBroker();
				if (jmsBroker == null) {
					throw new RuntimeException("无法连接集群中任意的broker,无法发送消息..");
				}
				JmsTemplate jmsTemplate = jmsBroker.getJmsTemplate();
				jmsTemplate.setSessionTransacted(false);
				target = jmsTemplate.execute(destination,
						new ProducerCallback<Object>() {
							@Override
							public Object doInJms(Session session,
									MessageProducer producer)
									throws JMSException {
								TemporaryQueue replyQueue = null;
								Message message = null;
								if (content instanceof String) {
									message = session
											.createTextMessage((String) content);
								} else if (content instanceof byte[]
										|| Serializable.class
												.isAssignableFrom(content
														.getClass())) {
									message = session
											.createObjectMessage((Serializable) content);
								} else {
									throw new IllegalArgumentException(
											"不支持此类型的消息.");
								}
								if (isReply) {
									replyQueue = session.createTemporaryQueue();
									message.setJMSReplyTo(replyQueue);
								}

								producer.setDisableMessageID(false);
								producer.setDisableMessageTimestamp(false);
								producer.setPriority(Message.DEFAULT_PRIORITY);
								producer.setTimeToLive(expiredTime);
								producer.setDeliveryMode(persitent ? DeliveryMode.PERSISTENT
										: DeliveryMode.NON_PERSISTENT);

								if (msgPropertyMap != null
										&& !msgPropertyMap.isEmpty()) {
									// 如果有，处理消息属性
									Iterator<Entry<String, Object>> iter = msgPropertyMap
											.entrySet().iterator();
									while (iter.hasNext()) {
										Map.Entry<String, Object> entry = (Map.Entry<String, Object>) iter
												.next();
										if (entry.getValue() instanceof java.lang.String) {
											message.setStringProperty(
													entry.getKey(),
													(String) entry.getValue());
										} else if (entry.getValue() instanceof java.lang.Long) {
											message.setLongProperty(
													entry.getKey(),
													(Long) entry.getValue());
										} else if (entry.getValue() instanceof java.lang.Boolean) {
											message.setBooleanProperty(
													entry.getKey(),
													(Boolean) entry.getValue());
										} else if (entry.getValue() instanceof java.lang.Double) {
											message.setDoubleProperty(
													entry.getKey(),
													(Double) entry.getValue());
										} else if (entry.getValue() instanceof java.lang.Integer) {
											message.setIntProperty(
													entry.getKey(),
													(Integer) entry.getValue());
										}
									}
								}
								producer.send(message);
								if (replyQueue != null)
									return replyQueue;
								return session;
							}
						});
				break;
			} catch (JmsException e) {
				if (isCluster) {
					synchronized (jmsBroker) {
						jmsBroker.setNormalWork(false);
					}
					// 扔出线程,不断检测损坏的节点,如果修复,即时修改状态,便于后续加入集群中
					new SenderResumeThread(jmsBroker).start();
				}
				throw e;
			}
		}
		return target;
	}

	public Session sendQueueMsg(String queueName, Object msg) {
		return this.sendQueueMsg(queueName, msg, null);
	}

	public Session sendQueueMsg(String queueName, Object msg,
			Map<String, Object> msgPropertyMap) {
		return (Session) this.sendQueueMsg(queueName, msg, msgPropertyMap,
				false);
	}

	/**
	 * 发送p2p非持久化消息
	 * 
	 * @param queueName
	 * @param msg
	 * @param msgPropertyMap
	 * @param replyto
	 *            true:Temporary false:Session
	 * @return
	 */
	public Object sendQueueMsg(String queueName, Object msg,
			Map<String, Object> msgPropertyMap, boolean replyto) {
		return this.sendMessage(msg, new ActiveMQQueue(queueName), false,
				msgPropertyMap, Message.DEFAULT_TIME_TO_LIVE, replyto);
	}

	public Session sendPstQueueMsg(String queueName, Object msg) {
		return this.sendPstQueueMsg(queueName, msg, null);
	}

	/**
	 * 
	 * @param queueName
	 *            队列名
	 * @param msg
	 *            消息体
	 * @param msgPropertyMap
	 *            消息头
	 * @return
	 */
	public Session sendPstQueueMsg(String queueName, Object msg,
			Map<String, Object> msgPropertyMap) {
		return (Session) this.sendPstQueueMsg(queueName, msg, msgPropertyMap,
				false);
	}

	/**
	 * 发送p2p持久化消息
	 * 
	 * @param queueName
	 * @param msg
	 * @param msgPropertyMap
	 * @param replyto
	 *            是否创建临时队列
	 * @return
	 */
	public Object sendPstQueueMsg(String queueName, Object msg,
			Map<String, Object> msgPropertyMap, boolean replyto) {
		return this.sendMessage(msg, new ActiveMQQueue(queueName), true,
				msgPropertyMap, Message.DEFAULT_TIME_TO_LIVE, replyto);
	}

	/**
	 * 针对某个Desnation注册JMS集群的消息监听
	 * 
	 * @param destName
	 * @param messageListener
	 * @return 注册成功的集群节点个数
	 */
	private int registClusterListener(String clientId, boolean queue,
			String destNameWithParam, String messageSelector,
			MessageListener messageListener) {
		int ret = 0;
		JmsBroker jmsBroker = null;
		Connection conn = null;

		for (int i = 0; i < brokerNum; i++) {
			try {
				jmsBroker = jmsBrokerList.get(i);
				ActiveMQConnectionFactory acf = (ActiveMQConnectionFactory) jmsBroker
						.getConnectionFactory().getTargetConnectionFactory();
				conn = getNewConn(acf);

				jmsBroker.getConnMap().put(destNameWithParam, conn);
				jmsBroker.getMessageListenerMap().put(destNameWithParam,
						messageListener);

				if (clientId != null) {
					conn.setClientID(clientId);
					if (!queue) {
						jmsBroker.getDurableTopicClientIdMap().put(
								destNameWithParam, clientId);
					}
				}

				Session se = conn.createSession(false,
						Session.CLIENT_ACKNOWLEDGE);
				MessageConsumer messageConsumer = null;
				if (clientId != null && !queue) {
					if (messageSelector == null) {
						messageConsumer = se.createDurableSubscriber(
								new ActiveMQTopic(destNameWithParam), clientId);
					} else {
						messageConsumer = se.createDurableSubscriber(
								new ActiveMQTopic(destNameWithParam), clientId,
								messageSelector, true);
					}
				} else {
					if (messageSelector == null) {
						messageConsumer = se
								.createConsumer(queue ? new ActiveMQQueue(
										destNameWithParam) : new ActiveMQTopic(
										destNameWithParam));
					} else {
						messageConsumer = se.createConsumer(
								queue ? new ActiveMQQueue(destNameWithParam)
										: new ActiveMQTopic(destNameWithParam),
								messageSelector);
					}
				}
				messageConsumer.setMessageListener(messageListener);

				conn.setExceptionListener(new ConnExceptionListener(jmsBroker,
						queue, destNameWithParam));
				conn.start();
				ret++;
				logger.info("对JMS<" + acf.getBrokerURL() + ">注册Destination<"
						+ destNameWithParam + ">的监听器完毕"
						+ messageListener.toString());
			} catch (Exception e) {
				e.printStackTrace();
				new ReceiverResumeThread(jmsBroker, queue, destNameWithParam)
						.start();
			}
		}

		logger.info("对JMS集群中所有Broker注册Destination " + destNameWithParam
				+ " 的监听器完毕，该Destination成功注册了 " + ret + " 个监听器");
		return ret;
	}

	/**
	 * 注册集群中指定p2p的监听，系统初始化调用
	 * 
	 * @param queueNameWithParam
	 *            ，可带参数的队列名称，通常就是一个单纯的队列名
	 * @param messageListener
	 * @return
	 */
	@Deprecated
	public int registClusterQueueListener(String queueNameWithParam,
			MessageListener messageListener) {
		return this.registClusterQueueListener(queueNameWithParam, "",
				messageListener);
	}

	@Deprecated
	public int registClusterQueueListener(String queueNameWithParam,
			String messageSelector, MessageListener messageListener) {
		return this.registClusterQueueListener(null, queueNameWithParam,
				messageSelector, messageListener);
	}

	@Deprecated
	public int registClusterQueueListener(String clientId,
			String queueNameWithParam, String messageSelector,
			MessageListener messageListener) {
		return this.registClusterListener(null, true, queueNameWithParam,
				messageSelector, messageListener);
	}

	/**
	 * 注册集群中指定PubSub的非持久监听，系统初始化调用
	 * 
	 * @param topicNameWithParam
	 *            ，可带参数的队列名称，通常就是一个单纯的队列名
	 * @param messageListener
	 * @return
	 */
	public int registClusterTopicListener(String topicNameWithParam,
			MessageListener messageListener) {
		return this.registClusterListener(null, false, topicNameWithParam,
				null, messageListener);
	}

	/**
	 * 注册集群中指定PubSub的非持久监听，带消息选择器，系统初始化调用
	 * 
	 * @param topicNameWithParam
	 *            ，可带参数的队列名称，通常就是一个单纯的队列名
	 * @param messageSelector
	 *            消息选择器表达式，如果不需要请调用上面的方法
	 * @param messageListener
	 * @return
	 */
	public int registClusterTopicListener(String topicNameWithParam,
			String messageSelector, MessageListener messageListener) {
		return this.registClusterListener(null, false, topicNameWithParam,
				messageSelector, messageListener);
	}

	/**
	 * 注册集群中指定PubSub的持久性监听，系统初始化调用
	 * 
	 * @param uniqueClientId
	 *            ，消费端标示，必须在整个集群唯一
	 * @param topicNameWithParam
	 *            ，可带参数的队列名称，通常就是一个单纯的队列名
	 * @param messageListener
	 * @return
	 */
	public int registClusterDurableTopicListener(String uniqueClientId,
			String topicNameWithParam, MessageListener messageListener) {
		return this.registClusterListener(uniqueClientId, false,
				topicNameWithParam, null, messageListener);
	}

	/**
	 * 注册集群中指定PubSub的持久性监听，带消息选择器 , 系统初始化调用
	 * 
	 * @param uniqueClientId
	 *            ，消费端标示，必须在整个集群唯一
	 * @param topicNameWithParam
	 *            ，可带参数的队列名称，通常就是一个单纯的队列名
	 * @param messageSelector
	 *            消息选择器表达式，如果不需要请调用上面的方法
	 * @param messageListener
	 * @return
	 */
	public int registClusterDurableTopicListener(String uniqueClientId,
			String topicNameWithParam, String messageSelector,
			MessageListener messageListener) {
		return this.registClusterListener(uniqueClientId, false,
				topicNameWithParam, messageSelector, messageListener);
	}

	/**
	 * 销毁所有的JMS监听，系统关闭时调用
	 */
	public void unregistClusterListener() {
		Connection conn = null;
		Map<String, Connection> connMap = null;
		Iterator<String> destNameWithParamIter = null;
		String destNameWithParam = null;
		for (JmsBroker jmsBroker : jmsBrokerList) {
			connMap = jmsBroker.getConnMap();
			destNameWithParamIter = connMap.keySet().iterator();
			while (destNameWithParamIter.hasNext()) {
				try {
					destNameWithParam = (String) destNameWithParamIter.next();
					conn = connMap.get(destNameWithParam);
					conn.stop();
					conn.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			jmsBroker.getConnMap().clear();
			jmsBroker.getDurableTopicClientIdMap().clear();
			jmsBroker.getMessageListenerMap().clear();
		}
	}

	/**
	 * 销毁指定目标（queue或者topic）的JMS监听
	 */
	public void unregistClusterListener(String destName) {
		Connection conn = null;
		Map<String, Connection> connMap = null;
		Iterator<String> destNameWithParamIter = null;
		String destNameWithParam = null;
		for (JmsBroker jmsBroker : jmsBrokerList) {
			connMap = jmsBroker.getConnMap();
			destNameWithParamIter = connMap.keySet().iterator();
			while (destNameWithParamIter.hasNext()) {
				try {
					destNameWithParam = (String) destNameWithParamIter.next();
					if (destNameWithParam.equals(destName)) {
						conn = connMap.get(destNameWithParam);
						conn.stop();
						conn.close();
						break;
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}

	private Connection getNewConn(ActiveMQConnectionFactory afc)
			throws JMSException {
		Connection conn = null;
		ActiveMQConnectionFactory connfc = new ActiveMQConnectionFactory();
		connfc.setBrokerURL(afc.getBrokerURL());
		connfc.setPassword(afc.getPassword());
		connfc.setUserName(afc.getUserName());
		connfc.setUseAsyncSend(true);
		connfc.setAlwaysSessionAsync(false);
		connfc.setOptimizeAcknowledge(true);
		connfc.setProducerWindowSize(1024000);
		conn = connfc.createConnection();
		return conn;
	}

	public List<JmsBroker> getJmsBrokerList() {
		return jmsBrokerList;
	}

	public void setJmsBrokerList(List<JmsBroker> jmsBrokerList) {
		this.jmsBrokerList = jmsBrokerList;
	}

	// 监听异常处理
	class ConnExceptionListener implements ExceptionListener {

		private JmsBroker jmsBroker;
		private boolean queue;
		private String destNameWithParam;

		public ConnExceptionListener(JmsBroker jmsBroker, boolean queue,
				String destNameWithParam) {
			this.jmsBroker = jmsBroker;
			this.queue = queue;
			this.destNameWithParam = destNameWithParam;
		}

		public void onException(JMSException exception) {
			try {
				jmsBroker.getConnMap().get(destNameWithParam).close();
			} catch (Exception e) {
			}
			// 启动监听修复线程
			new ReceiverResumeThread(jmsBroker, queue, destNameWithParam)
					.start();
			ActiveMQConnectionFactory acf = (ActiveMQConnectionFactory) jmsBroker
					.getConnectionFactory().getTargetConnectionFactory();
		}
	}

	// 监听端修复线程
	class ReceiverResumeThread extends Thread {

		private JmsBroker jmsBroker;
		private boolean queue;
		private String destNameWithParam;

		public ReceiverResumeThread(JmsBroker jmsBroker, boolean queue,
				String destNameWithParam) {
			this.jmsBroker = jmsBroker;
			this.queue = queue;
			this.destNameWithParam = destNameWithParam;
		}

		@Override
		public void run() {
			Connection conn = null;
			String clientId = jmsBroker.getDurableTopicClientIdMap().get(
					destNameWithParam);
			while (true) {
				try {
					conn = getNewConn((ActiveMQConnectionFactory) jmsBroker
							.getConnectionFactory()
							.getTargetConnectionFactory());
					if (clientId != null) {
						conn.setClientID(clientId);
					}
					jmsBroker.getConnMap().put(destNameWithParam, conn);

					Session se = conn.createSession(false,
							Session.CLIENT_ACKNOWLEDGE);
					MessageConsumer messageConsumer = null;
					if (clientId != null) {
						messageConsumer = se.createDurableSubscriber(
								new ActiveMQTopic(destNameWithParam), clientId);
					} else {
						messageConsumer = se
								.createConsumer(queue ? new ActiveMQQueue(
										destNameWithParam) : new ActiveMQTopic(
										destNameWithParam));
					}

					messageConsumer.setMessageListener(jmsBroker
							.getMessageListenerMap().get(destNameWithParam));

					conn.setExceptionListener(new ConnExceptionListener(
							jmsBroker, queue, destNameWithParam));
					conn.start();

					ActiveMQConnectionFactory acf = (ActiveMQConnectionFactory) jmsBroker
							.getConnectionFactory()
							.getTargetConnectionFactory();
					break;
				} catch (Exception e) {
					synchronized (jmsBroker) {
						try {
							jmsBroker.wait(1000L * 10);
						} catch (Exception e2) {
						}
					}
				}

			}
		}

	}

	// 发送端修复线程
	class SenderResumeThread extends Thread {

		private JmsBroker jmsBroker;

		public SenderResumeThread(JmsBroker jmsBroker) {
			this.jmsBroker = jmsBroker;
		}

		@SuppressWarnings("unchecked")
		@Override
		public void run() {
			while (true) {
				try {
					jmsBroker.getJmsTemplate().browse(new BrowserCallback() {
						public Object doInJms(Session arg0, QueueBrowser arg1)
								throws JMSException {
							arg1.getQueue();
							return null;
						}
					});
					synchronized (jmsBroker) {
						jmsBroker.setNormalWork(true);
					}
					ActiveMQConnectionFactory acf = (ActiveMQConnectionFactory) jmsBroker
							.getConnectionFactory()
							.getTargetConnectionFactory();
					logger.info("已修复JMS Broker " + acf.getBrokerURL()
							+ " , 已经加入集群");
					break;
				} catch (Exception e) {
					synchronized (jmsBroker) {
						try {
							jmsBroker.wait(1000L * 10);
						} catch (Exception e2) {
						}
					}
				}

			}
		}

	}
}
