package cn.wonhigh.dc.client.manager.yarnmsg;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;

public class DcClientTaskReject implements RejectedExecutionHandler {

    private static final Logger logger = LoggerFactory.getLogger(DcClientTaskReject.class.getName());
    @Override
    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
        logger.info("进入自定义拒绝策略-->进行邮件或短信通知相关人员."+r.getClass());
        //todo:进行邮件或短信通知相关人员



    }
}
