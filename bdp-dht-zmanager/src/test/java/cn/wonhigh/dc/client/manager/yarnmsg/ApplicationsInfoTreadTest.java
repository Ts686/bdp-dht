package cn.wonhigh.dc.client.manager.yarnmsg;


import cn.wonhigh.dc.client.service.ApplicationInfoService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.client.cli.ApplicationCLI;
import org.junit.Test;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.util.HashMap;
import java.util.Map;

public class ApplicationsInfoTreadTest {

//    @Autowired
//    ApplicationInfoService applicationInfoService;

    @Test
    public void testGetYarnInfo(){

//        ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

        String url = "http://10.234.6.44:8088/ws/v1/cluster/apps";

//        scheduledExecutorService.execute(new ApplicationsInfoThread(url));
        Map<String, Object> parmasMap = new HashMap<>();
        parmasMap.put("states","ACCEPTED,RUNNING,KILLED");

        ClassPathXmlApplicationContext ctx = null;
        String[] configureLocation = new String[]{"classpath:/META-INF/spring-client-manager.xml"};
        ctx = new ClassPathXmlApplicationContext(configureLocation);
        ctx.start();

        ApplicationInfoService applicationInfoService = (ApplicationInfoService)ctx.getBean("applicationInfoService");

//        new ApplicationsInfoThread(applicationInfoService,url,parmasMap).run();



    }

    @Test
    public void testKilledApp(){

        try {
            Configuration conf = new Configuration();

            conf.addResource("core-site.xml");

            String args[] = {"application","-kill","application_1546571870663_0319_xxx"};
            ApplicationCLI cli = new ApplicationCLI();
            cli.setSysOutPrintStream(System.out);
            cli.setSysErrPrintStream(System.err);
            int res = ToolRunner.run(cli, args);
            cli.stop();
            System.exit(res);

        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
