package cn.wonhigh.dc.client.manager.bootstrap;

import cn.wonhigh.dc.client.common.constans.CommonEnumCollection;
import cn.wonhigh.dc.client.common.constans.MessageConstant;
import cn.wonhigh.dc.client.common.model.TaskPropertiesConfig;
import cn.wonhigh.dc.client.common.monitor.ConfigFileMonitor;
import cn.wonhigh.dc.client.common.monitor.ConfigSQLFileMonitor;
import cn.wonhigh.dc.client.common.util.DateUtils;
import cn.wonhigh.dc.client.common.util.HiveUtils;
import cn.wonhigh.dc.client.common.util.ParseXMLFileUtil;
import cn.wonhigh.dc.client.common.util.PropertyFile;
import cn.wonhigh.dc.client.manager.yarnmsg.ApplicationsInfoThread;
import cn.wonhigh.dc.client.service.jms.JmsClusterMgr;
import com.yougou.logistics.base.common.exception.ManagerException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.io.IOException;
import java.util.*;

public class StartMain {

    private static final Logger logger = LoggerFactory.getLogger(StartMain.class);

    private static boolean initFlag = false;

    public static void init() {
        logger.info("客户端开始启动...");
        ClassPathXmlApplicationContext ctx = new ClassPathXmlApplicationContext();
        try {
            Properties properties = PropertyFile.getProps("");
            PropertyConfigurator.configureAndWatch(properties.getProperty(MessageConstant.LOG4J_PROPERTIES_DIR));

            //对hive和sqoop密码进行解密
            MessageConstant.PWD_ENCRYP_VALUE = (String) properties.get(MessageConstant.PWD_ENCRYPTION_KEY);

            //存储是否打开加密开关
            Object onOff = properties.get(MessageConstant.PWD_ON_OFF_KEY);
            MessageConstant.PWD_ON_OFF_VALUE = (onOff != null && "true".equalsIgnoreCase(onOff + "")) ? true : false;
            //logger.info("=====密匙："+ MessageConstant.PWD_ENCRYP_VALUE);
            //重复主键，设置查询表数据的时间
            Object primaryConfig = properties.get(MessageConstant.TAB_DUP_PRIMARY_CONFIG_KEY);
            MessageConstant.TAB_DUP_PRIMARY_CONFIG_VALUE = (primaryConfig != null && !"".equals(primaryConfig + ""))
                    ? Integer.parseInt(primaryConfig + "") : 365;

            logger.info("加载本地连接配置信息及初始化log4j.");
            logger.info("加载本地应用配置文件:spring-client-manager.xml");
            String[] configureLocation = new String[]{"classpath:/META-INF/spring-client-manager.xml"};
            ctx = new ClassPathXmlApplicationContext(configureLocation);
            ctx.start();
            //初始化任务基本信息
//            ParseXMLFileUtil.initTask();
            logger.info("---------------------更新redis中任务及数据库信息---------------------");
            logger.info(String.format("【当前任务个数%s个,数据库信息%s个】", ParseXMLFileUtil.getCacheTaskEntitiesKeys().size(), ParseXMLFileUtil.getCacheDbEntities().size()));
            ParseXMLFileUtil.initTaskByRedis(properties);
            logger.info("---------------------更新完成---------------------");
            logger.info(String.format("【redis加载完成:任务个数%s个,数据库信息%s个】", ParseXMLFileUtil.getCacheTaskEntitiesKeys().size(), ParseXMLFileUtil.getCacheDbEntities().size()));
//            ParseProXMLFileUtil.initTask();

//            ParseSQLXMLFileUtil.initTask();
            //ParseXMLFileUtil.initHistoryTable();
            String cdcTable_list = PropertyFile.getValue(MessageConstant.TRANSCATION_HISTORY_LOG, "");

            if (HiveUtils.getCdcTableList().length > 0) {
                String str = HiveUtils.getCdcTableList()[0];
                String groupName = str.replace("_transaction_history_log_src", "");
                String triggerName = CommonEnumCollection.HiveDefinePartNameEnum.CDC_TABLE_SUBFIX.getValue();
                logger.info("在启动过程中加载： groupName：【 " + groupName + "】 targetName：【" + triggerName + "】");
                TaskPropertiesConfig taskConfig = ParseXMLFileUtil.getTaskConfig(groupName, triggerName);
                if (taskConfig == null) {
                    String message = "获取dc-client.properties中的cdc.table.list 列表的第一项【groupName：%s】【triggerName：%s】hive的jdbc信息失败，" +
                            "请检测是否配置该项，多项间用逗号分割";
                    logger.error(String.format("%s：【groupName：%s】【triggerName：%s】", message, groupName, triggerName));
                } else {
                    Date currentDate = new Date();
                    Calendar calendar = Calendar.getInstance();
                    if (null == currentDate || null == calendar) {
                        String message = "获取导出时间实例失败";
                        throw new ManagerException(message);
                    }
                    calendar.setTime(currentDate);
                    Date endTime = calendar.getTime();
                    calendar.add(Calendar.YEAR, -1);
                    Date startTimer = calendar.getTime();
//                    HiveUtils.updateTransactionHisLog(taskConfig, null, startTimer, endTime, null, 6000, true);
                    String message = String.format("依据dc-client.properties中的cdc.table.list 列表中的第一项【groupName：%s】【triggerName：%s】" +
                            "获取hive的jdbc连接信息", groupName, triggerName, groupName + "-" + triggerName);
                    logger.info(message);
                }
            }

            //是否开启配置文件监控
            String isopen = properties.getProperty(MessageConstant.IS_OPEN_CONFIG_MONITOR);
            boolean openmonitor = true;
            //配置文件不为false时  及表示 默认开启监控
            if (isopen == null) {
                openmonitor = true;
            } else if (isopen.equals("false")) {
                openmonitor = false;
            }
            if (openmonitor) {
                //启动配置文件监控  目录为  /db  /task/sqoop  /task/sqooppro
                ConfigFileMonitor.startMonitor();
            }

            if (openmonitor) {
                //启动配置文件监控
                ConfigSQLFileMonitor.startMonitor();
            }


            JmsClusterMgr jmsClusterMgr = (JmsClusterMgr) ctx.getBean("jmsClusterMgr");

            initFlag = true;
            logger.info("客户端启动成功.");
            logger.warn(String.format("Current bdp-dht version: %s",
                    Package.getPackage("cn.wonhigh.dc.client.manager.bootstrap").getImplementationVersion()));

            //bdp-dht启动完成后进行异步获取yarn上的app状态进行库同步
            String host = properties.getProperty(MessageConstant.YARN_HOST);
            String port = properties.getProperty(MessageConstant.YARN_PORT);
            String url = "http://" + host + ":" + port + "/ws/v1/cluster/apps";
            Map<String, Object> paramsMap = new HashMap<>();
//            paramsMap.put("states","ACCEPTED,RUNNING");
//            paramsMap.put("states","ACCEPTED,RUNNING,KILLED,FINISHED");
            //查询最近两小时的数据
            paramsMap.put("startedTimeBegin", DateUtils.getHeadDate(new Date(), -2).getTime());

            //属性文件中传入一个重启标志,用户控制同步yarn状态时只拉起一次宕机的hive任务
            properties.setProperty("isRestart", "true");
            new Thread(new ApplicationsInfoThread(ctx, url, paramsMap, jmsClusterMgr, properties)).start();
            logger.info("开启异步同步yarn状态成功");


        } catch (Exception e) {
            logger.error("客户端启动失败,", e);
            if (ctx != null) {
                ctx.stop();
                ctx.destroy();
            }
            System.exit(1);
        }
    }

    /**
     * 验证kerbos
     */
    private static void validateKerbos() {
        Configuration conf = new Configuration();
        conf.set("hadoop.security.authentication", "Kerberos");
        UserGroupInformation.setConfiguration(conf);
        try {
            UserGroupInformation.loginUserFromKeytab("user/jet@WONHIGH.CN",
                    "C:/temp/krb5cache/jet.keytab");
        } catch (IOException e) {
            logger.error("权限认证失败...", e);
            e.printStackTrace();
        }
    }


    public static void main(String[] args) {
        if (!initFlag) {
            init();
        }
//        while (true) {
//            try {
//                Thread.sleep(1000 * 5);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//        }
    }


}
