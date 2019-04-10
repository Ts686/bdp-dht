package cn.wonhigh.dc.client.manager;


import cn.wonhigh.dc.client.common.util.DateUtils;
import cn.wonhigh.dc.client.common.util.RemoteShellExeUtil;
import cn.wonhigh.dc.client.manager.jms.SendMsg2AMQ;
import cn.wonhigh.dc.client.manager.yarnmsg.TaskThreadPool;
import cn.wonhigh.dc.client.service.jms.JmsClusterMgr;
import com.yougou.logistics.base.common.enums.JobBizStatusEnum;
import com.yougou.logistics.base.common.interfaces.RemoteJobServiceExtWithParams;
import com.yougou.logistics.base.common.vo.scheduler.RemoteJobInvokeParamsDto;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jmx.export.annotation.ManagedResource;
import org.springframework.stereotype.Service;

import java.util.Date;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * jmx执行shell服务用法：
 *         JMXServiceURL url = new JMXServiceURL
 *                 ("service:jmx:rmi:///jndi/rmi://localhost:9999/jmxrmi");
 *         JMXConnector jmxc = JMXConnectorFactory.connect(url, null);
 *         MBeanServerConnection mbsc = jmxc.getMBeanServerConnection();
 *         //ObjectName的名称与前面注册时候的保持一致
 *         ObjectName mbeanName = new ObjectName("dc:client=ExecuteShellTaskImpl");
 *         HelloMBean proxy = MBeanServerInvocationHandler.
 *                 newProxyInstance(mbsc, mbeanName, ExecuteShellTaskImpl.class, false);
 *                 proxy.executeJobWithParams( jobId,  triggerName,  groupName,  remoteJobInvokeParamsDto);
 */
@Service
@ManagedResource(objectName = ExecuteShellTaskImpl.MBEAN_NAME, description = "shell执行统一服务")
public class ExecuteShellTaskImpl implements RemoteJobServiceExtWithParams {


    private static final Logger logger = Logger.getLogger(ExecuteShellTaskImpl.class);

    public static final String MBEAN_NAME = "dc:client=ExecuteShellTaskImpl";

    @Autowired
    private JmsClusterMgr jmsClusterMgr;

    private  ThreadPoolExecutor taskThreadPoolInstance;

    @Override
    public void initializeJob(String s, String s1, String s2) {

    }

    /**
     * 执行shell脚本方法
     * @param jobId
     * @param triggerName
     * @param groupName
     * @param remoteJobInvokeParamsDto：对象中必须有shellName,startTime,endTime,exeUserName,host参数，port非必传，默认为22
     *                                如果有业务参数则必须有businessParamNames参数，此参数作用是规定业务参数的顺序
     *                                格式为：businessParamNames=p1,p2,p3,此时p1在shell命令中一定为第一业务参数，依次类推
     *
     */
    @Override
    public void executeJobWithParams(String jobId, String triggerName, String groupName, RemoteJobInvokeParamsDto remoteJobInvokeParamsDto) {

        taskThreadPoolInstance = TaskThreadPool.getTaskThreadPoolInstance();

        String msg = "";
        if (StringUtils.isBlank(jobId)) {
            msg = "调用接口出错，jobId不能为空，请检查。";
            logger.error(msg);
            //发送MQ消息
            SendMsg2AMQ.updateStatusAndSendMsg(jobId,JobBizStatusEnum.STOPED,jmsClusterMgr,msg);
            return;
        }

        taskThreadPoolInstance.execute(new ExeShellThread( jmsClusterMgr, jobId,
                triggerName,  groupName,  remoteJobInvokeParamsDto));


    }

    //内部类异步执行shell
    class ExeShellThread implements Runnable{

        private JmsClusterMgr jmsClusterMgr;
        private String jobId;
        private String triggerName;
        private String groupName;
        private RemoteJobInvokeParamsDto remoteJobInvokeParamsDto;

        ExeShellThread(JmsClusterMgr jmsClusterMgr,String jobId, String triggerName,
                       String groupName, RemoteJobInvokeParamsDto remoteJobInvokeParamsDto){
            this.jmsClusterMgr = jmsClusterMgr;
            this.jobId = jobId;
            this.triggerName = triggerName;
            this.groupName = groupName;
            this.remoteJobInvokeParamsDto = remoteJobInvokeParamsDto;

        }

        @Override
        public void run() {

            String msg = "";
            String shellName = remoteJobInvokeParamsDto.getParam("shellName");
            String exeUserName = remoteJobInvokeParamsDto.getParam("exeUserName");
            String host = remoteJobInvokeParamsDto.getParam("host");
            String startTime = remoteJobInvokeParamsDto.getParam("startTime");
            String endTime = remoteJobInvokeParamsDto.getParam("endTime");
            int port =22;

            if(StringUtils.isNotBlank(remoteJobInvokeParamsDto.getParam("port"))){
                port = Integer.parseInt(remoteJobInvokeParamsDto.getParam("port"));
            }

            logger.info("入参：shellName=【" + shellName + "】,startTime=【" + startTime + "】，endTime=【" + endTime + "】,exeUserName"+"【"+exeUserName+"】，" +
                    "host=【"+host+"】,port=【"+port+"】");
            if (StringUtils.isBlank(shellName)) {
                //发送MQ消息
                msg ="调用接口出错，脚本名称不能为空，请检查。";
                logger.error(msg);
                //发送MQ消息
                SendMsg2AMQ.updateStatusAndSendMsg(jobId,JobBizStatusEnum.STOPED,jmsClusterMgr,msg);
                return;
            }
            if (StringUtils.isBlank(exeUserName)) {
                //发送MQ消息
                msg ="调用接口出错，执行脚本用户为空，请检查。";
                logger.error(msg);
                //发送MQ消息
                SendMsg2AMQ.updateStatusAndSendMsg(jobId,JobBizStatusEnum.STOPED,jmsClusterMgr,msg);
                return;
            }
            if (StringUtils.isBlank(host)) {
                //发送MQ消息
                msg ="调用接口出错，脚本所在主机ip为空，请检查。";
                logger.error(msg);
                //发送MQ消息
                SendMsg2AMQ.updateStatusAndSendMsg(jobId,JobBizStatusEnum.STOPED,jmsClusterMgr,msg);
                return;
            }

            if(StringUtils.isBlank(startTime)){
                startTime = DateUtils.formatDatetime(new Date(),DateUtils.FORMAT_DT);

            }

            if(StringUtils.isBlank(endTime)){
                endTime =  DateUtils.formatDatetime(new Date(),DateUtils.FORMAT_DT);

            }
            logger.info("执行shell脚本开始");
            SendMsg2AMQ.updateStatusAndSendMsg(jobId,JobBizStatusEnum.RUNNING,jmsClusterMgr,"正在执行脚本："+shellName);


            StringBuilder sb = new StringBuilder();
            logger.info("组装可执行shell脚本命令");

            sb.append(" sh ");
            sb.append(shellName);
            sb.append(" '");
            sb.append(startTime);
            sb.append("' '");
            sb.append(endTime);
            sb.append("' ");
            //拼接业务参数
            String businessParamNames = remoteJobInvokeParamsDto.getParam("businessParamNames");
            if(StringUtils.isNotBlank(businessParamNames)){
                String[] split = businessParamNames.split(",");
                for(int i=0;i<split.length;i++){
                    sb.append(remoteJobInvokeParamsDto.getParam(split[i]));
                    sb.append(" ");
                }
            }

            //异步执行shell脚本，防止脚本阻塞
            String shellString = sb.toString();
            logger.info("最终执行shell命令为："+shellString);
            SendMsg2AMQ.updateStatusAndSendMsg(jobId,JobBizStatusEnum.RUNNING,jmsClusterMgr,"执行shell脚本："+shellString);

            String result = RemoteShellExeUtil.exeShell(exeUserName, host, port, shellString);

            if(StringUtils.isEmpty(result)||( result.indexOf("job_success")==-1)){
                //执行脚本失败
                logger.error("执行脚本失败。");
                SendMsg2AMQ.updateStatusAndSendMsg(jobId,JobBizStatusEnum.FINISHED,jmsClusterMgr,"脚本执行失败，脚本返回信息："+result);
                return ;
            }

            SendMsg2AMQ.updateStatusAndSendMsg(jobId,JobBizStatusEnum.FINISHED,jmsClusterMgr,"脚本执行成功");
            logger.info("执行shell脚本结束");

        }
    }

    @Override
    public void pauseJob(String s, String s1, String s2) {

    }

    @Override
    public void resumeJob(String s, String s1, String s2) {

    }

    @Override
    public void stopJob(String s, String s1, String s2) {

    }

    @Override
    public void restartJob(String s, String s1, String s2) {

    }

    @Override
    public JobBizStatusEnum getJobStatus(String s, String s1, String s2) {
        return null;
    }

    @Override
    public String getLogs(String s, String s1, String s2, long l) {
        return null;
    }
}
