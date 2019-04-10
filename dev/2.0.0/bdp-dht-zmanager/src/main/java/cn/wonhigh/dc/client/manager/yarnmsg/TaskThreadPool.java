package cn.wonhigh.dc.client.manager.yarnmsg;

import cn.wonhigh.dc.client.common.util.PropertyFile;
import org.apache.commons.lang.StringUtils;

import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.*;

public class TaskThreadPool {

    private static ConcurrentHashMap<String,ThreadPoolExecutor> concurrentHashMap = null;

    private ThreadPoolExecutor threadPoolExecutor;

    private TaskThreadPool(int core, int max,
                           Long alive, LinkedBlockingDeque blockingDeque,
                           boolean allowCoreThreadTimeout,
                           RejectedExecutionHandler rejectedHandler) {
        threadPoolExecutor = new ThreadPoolExecutor(core, max, alive, TimeUnit.SECONDS, blockingDeque
                , rejectedHandler);
        concurrentHashMap = new ConcurrentHashMap(2);
        threadPoolExecutor.allowCoreThreadTimeOut(allowCoreThreadTimeout);
        concurrentHashMap.put("taskThreadPool",threadPoolExecutor);

    }

    public static ThreadPoolExecutor getTaskThreadPoolInstance() {

        if (Objects.nonNull(concurrentHashMap)
                && Objects.nonNull(concurrentHashMap.get("taskThreadPool"))) {

            return (ThreadPoolExecutor) concurrentHashMap.get("taskThreadPool");
        }
        //将所有参数设置为可配置化处理
        Properties properties = PropertyFile.getProps("");
        int core = 6;
        int max = 20;
        long alive = 120L;
        int capacity = 1000;

        if(StringUtils.isNotBlank(properties.getProperty("define.thread.coreThread"))){
            core = Integer.parseInt(properties.getProperty("define.thread.coreThread"));
        }
        if(StringUtils.isNotBlank(properties.getProperty("define.thread.maxThread"))){
            max = Integer.parseInt(properties.getProperty("define.thread.maxThread"));
        }
        if(StringUtils.isNotBlank(properties.getProperty("define.thread.keepAlive"))){
            alive = Long.parseLong(properties.getProperty("define.thread.keepAlive"));
        }
        if(StringUtils.isNotBlank(properties.getProperty("define.thread.queueCapacity"))){
            capacity = Integer.parseInt(properties.getProperty("define.thread.queueCapacity"));
        }

        new TaskThreadPool(core,max,alive,new LinkedBlockingDeque(capacity),true,new DcClientTaskReject());
        return (ThreadPoolExecutor) concurrentHashMap.get("taskThreadPool");

    }

}
