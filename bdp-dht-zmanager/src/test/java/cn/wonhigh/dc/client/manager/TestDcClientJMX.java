package cn.wonhigh.dc.client.manager;

import com.yougou.logistics.base.common.interfaces.RemoteJobServiceExtWithParams;
import com.yougou.logistics.base.common.vo.scheduler.RemoteJobInvokeParamsDto;

import javax.management.MBeanServerConnection;
import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

public class TestDcClientJMX {

    public static void main(String[] args) throws Exception{
        for(int i = 0;i<1;i++){
//            JMXServiceURL url = new JMXServiceURL("service:jmx:rmi://127.0.0.1/jndi/rmi://127.0.0.1:6088/eltJob");
            JMXServiceURL url = new JMXServiceURL("service:jmx:rmi://10.240.12.24/jndi/rmi://10.240.12.24:6088/eltJob");
            //生产测试
//            JMXServiceURL url = new JMXServiceURL("service:jmx:rmi://10.240.20.57/jndi/rmi://10.240.20.57:6088/eltJob");
            JMXConnector jmxc = JMXConnectorFactory.connect(url, null);

            MBeanServerConnection mbsc = jmxc.getMBeanServerConnection();
            //ObjectName的名称与前面注册时候的保持一致
            ObjectName mbeanName = new ObjectName("dc:client=ExecuteShellTaskImpl");


            RemoteJobServiceExtWithParams proxy = MBeanServerInvocationHandler.
                    newProxyInstance(mbsc, mbeanName, RemoteJobServiceExtWithParams.class, false);

//            System.out.println(i+":"+proxy);
            RemoteJobInvokeParamsDto dto = new RemoteJobInvokeParamsDto();
//            dto.addParam("shellName","/usr/local/test/hello.sh");
            dto.addParam("shellName","/usr/local/wonhigh/dc/client/test.sh");
            dto.addParam("exeUserName","bdp_app");
            //生产测试
//            dto.addParam("host","10.240.20.57 ");
            dto.addParam("host","10.240.12.24");
            dto.addParam("port","");
            dto.addParam("p1","业务参数_"+i);
            dto.addParam("p2","业务参数_"+(i+1));
            dto.addParam("businessParamNames","p1,p2");

            proxy.executeJobWithParams("test_"+i,"trigger_xxxx","test",dto);
            System.out.println("调用完毕");
        }


        }
}
